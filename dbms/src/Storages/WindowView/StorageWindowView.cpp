#include <Columns/ColumnsNumber.h>
#include <DataStreams/AddingConstColumnBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsWindow.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/AddingConstColumnTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/StorageFactory.h>
#include <boost/lexical_cast.hpp>
#include <Common/typeid_cast.h>

#include <Storages/WindowView/BlocksListSource.h>
#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/WindowView/WatermarkBlockInputStream.h>
#include <Storages/WindowView/WindowViewBlockInputStream.h>
#include <Storages/WindowView/WindowViewProxyStorage.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_PARSE_TEXT;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_WAS_NOT_DROPPED;
}

namespace
{
    const auto RESCHEDULE_MS = 500;

    struct StageMergeableVisitorData
    {
        using TypeToVisit = ASTFunction;

        ASTPtr window_function;
        String window_column_name;
        String window_column_alias;
        String timestamp_column_name;
        bool is_tumble = false;
        bool is_hop = false;

        void visit(const ASTFunction & node, ASTPtr & node_ptr)
        {
            if (node.name == "TUMBLE")
            {
                if (!window_function)
                {
                    is_tumble = true;
                    window_column_name = node.getColumnName();
                    window_column_alias = node.alias;
                    window_function = node.clone();
                    timestamp_column_name = node.arguments->children[0]->getColumnName();
                }
                else if (serializeAST(node) != serializeAST(*window_function))
                    throw Exception("WINDOW VIEW only support ONE WINDOW FUNCTION", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
            }
            else if (node.name == "HOP")
            {
                if (!window_function)
                {
                    is_hop = true;
                    window_function = node.clone();
                    timestamp_column_name = node.arguments->children[0]->getColumnName();
                    auto ptr = node.clone();
                    std::static_pointer_cast<ASTFunction>(ptr)->setAlias("");
                    auto array_join = makeASTFunction("arrayJoin", ptr);
                    array_join->alias = node.alias;
                    node_ptr = array_join;
                    window_column_name = array_join->getColumnName();
                    window_column_alias = array_join->alias;
                }
                else if (serializeAST(node) != serializeAST(*window_function))
                    throw Exception("WINDOW VIEW only support ONE WINDOW FUNCTION", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
            }
        }
    };

    struct ReplaceFuncNowVisitorData
    {
        using TypeToVisit = ASTFunction;

        bool is_time_column_func_now = false;
        String window_column_name;

        void visit(ASTFunction & node, ASTPtr & node_ptr)
        {
            if (node.name == "TUMBLE")
            {
                if (const auto * t = node.arguments->children[0]->as<ASTFunction>(); t && t->name == "now")
                {
                    is_time_column_func_now = true;
                    node_ptr->children[0]->children[0] = std::make_shared<ASTIdentifier>("____timestamp");
                    window_column_name = node.getColumnName();
                }
            }
            else if (node.name == "HOP")
            {
                if (const auto * t = node.arguments->children[0]->as<ASTFunction>(); t && t->name == "now")
                    is_time_column_func_now = true;
            }
        }
    };

    class ReplaceFunctionWindowMatcher
    {
    public:
        using Visitor = InDepthNodeVisitor<ReplaceFunctionWindowMatcher, true>;

        struct Data
        {
            String window_column_name;
            String window_column_alias;
            Aliases * aliases;
        };

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (const auto * t = ast->as<ASTFunction>())
                visit(*t, ast, data);
            if (const auto * t = ast->as<ASTIdentifier>())
                visit(*t, ast, data);
        }

    private:
        static void visit(const ASTFunction & node, ASTPtr & node_ptr, Data &)
        {
            if (node.name == "tuple")
                return;
            if (node.name == "HOP")
                node_ptr = std::make_shared<ASTIdentifier>("arrayJoin(" + node.getColumnName() + ")");
            else
                node_ptr = std::make_shared<ASTIdentifier>(node.getColumnName());
        }

        static void visit(const ASTIdentifier & node, ASTPtr & node_ptr, Data & data)
        {
            if (node.getColumnName() == data.window_column_alias)
                dynamic_cast<ASTIdentifier *>(node_ptr.get())->name = data.window_column_name;
            else if (auto it = data.aliases->find(node.getColumnName()); it != data.aliases->end())
                dynamic_cast<ASTIdentifier *>(node_ptr.get())->name = it->second->getColumnName();
        }
    };

    IntervalKind strToIntervalKind(const String& interval_str)
    {
        if (interval_str == "Second")
            return IntervalKind::Second;
        else if (interval_str == "Minute")
            return IntervalKind::Minute;
        else if (interval_str == "Hour")
            return IntervalKind::Hour;
        else if (interval_str == "Day")
            return IntervalKind::Day;
        else if (interval_str == "Week")
            return IntervalKind::Week;
        else if (interval_str == "Month")
            return IntervalKind::Month;
        else if (interval_str == "Quarter")
            return IntervalKind::Quarter;
        else if (interval_str == "Year")
            return IntervalKind::Year;
        __builtin_unreachable();
    }

    UInt32 addTime(UInt32 time_sec, IntervalKind::Kind window_kind, int window_num_units, const DateLUTImpl & time_zone)
    {
        switch (window_kind)
        {
#define CASE_WINDOW_KIND(KIND) \
        case IntervalKind::KIND: \
        { \
            return AddTime<IntervalKind::KIND>::execute(time_sec, window_num_units, time_zone); \
        }
            CASE_WINDOW_KIND(Second)
            CASE_WINDOW_KIND(Minute)
            CASE_WINDOW_KIND(Hour)
            CASE_WINDOW_KIND(Day)
            CASE_WINDOW_KIND(Week)
            CASE_WINDOW_KIND(Month)
            CASE_WINDOW_KIND(Quarter)
            CASE_WINDOW_KIND(Year)
#undef CASE_WINDOW_KIND
        }
        __builtin_unreachable();
    }

    String generateInnerTableName(const String & table_name) { return ".inner." + table_name; }
}

static void extractDependentTable(ASTSelectQuery & query, String & select_database_name, String & select_table_name)
{
    auto db_and_table = getDatabaseAndTable(query, 0);
    ASTPtr subquery = extractTableExpression(query, 0);

    if (!db_and_table && !subquery)
        return;

    if (db_and_table)
    {
        select_table_name = db_and_table->table;

        if (db_and_table->database.empty())
        {
            db_and_table->database = select_database_name;
            AddDefaultDatabaseVisitor visitor(select_database_name);
            visitor.visit(query);
        }
        else
            select_database_name = db_and_table->database;
    }
    else if (auto * ast_select = subquery->as<ASTSelectWithUnionQuery>())
    {
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for WINDOW VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);

        auto & inner_query = ast_select->list_of_selects->children.at(0);

        extractDependentTable(inner_query->as<ASTSelectQuery &>(), select_database_name, select_table_name);
    }
    else
        throw Exception(
            "Logical error while creating StorageWindowView."
            " Could not retrieve table name from select query.",
            DB::ErrorCodes::LOGICAL_ERROR);
}

ASTPtr StorageWindowView::generateCleanCacheQuery(UInt32 timestamp)
{
    auto function_tuple
        = makeASTFunction("tupleElement", std::make_shared<ASTIdentifier>(window_column_name), std::make_shared<ASTLiteral>(Field{UInt8(2)}));
    auto function_equal = makeASTFunction("less", function_tuple, std::make_shared<ASTLiteral>(timestamp));

    auto alter_command = std::make_shared<ASTAlterCommand>();
    alter_command->type = ASTAlterCommand::DELETE;
    alter_command->predicate = function_equal;
    alter_command->children.push_back(alter_command->predicate);

    auto alter_command_list = std::make_shared<ASTAlterCommandList>();
    alter_command_list->add(alter_command);

    auto alter_query = std::make_shared<ASTAlterQuery>();
    alter_query->database = inner_table_id.database_name;
    alter_query->table = inner_table_id.table_name;
    alter_query->set(alter_query->command_list, alter_command_list);
    return alter_query;
}

void StorageWindowView::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    Dependencies dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (!dependencies.empty())
    {
        StorageID dependent_table_id = dependencies.front();
        throw Exception("Table has dependency " + dependent_table_id.getNameForLogs(), ErrorCodes::TABLE_WAS_NOT_DROPPED);
    }
}

static void executeDropQuery(ASTDropQuery::Kind kind, Context & global_context, const StorageID & target_table_id)
{
    if (DatabaseCatalog::instance().tryGetTable(target_table_id))
    {
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = target_table_id.database_name;
        drop_query->table = target_table_id.table_name;
        drop_query->kind = kind;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, global_context);
        drop_interpreter.execute();
    }
}

void StorageWindowView::drop(TableStructureWriteLockHolder &)
{
    auto table_id = getStorageID();
    DatabaseCatalog::instance().removeDependency(select_table_id, table_id);
    executeDropQuery(ASTDropQuery::Kind::Drop, global_context, inner_table_id);

    std::lock_guard lock(mutex);
    is_dropped = true;
    fire_condition.notify_all();
}

void StorageWindowView::truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &)
{
    executeDropQuery(ASTDropQuery::Kind::Truncate, global_context, inner_table_id);
}

bool StorageWindowView::optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    return getInnerStorage()->optimize(query, partition, final, deduplicate, context);
}

Pipes StorageWindowView::blocksToPipes(BlocksList & blocks, Block & sample_block)
{
    Pipes pipes;
    for (auto & block : blocks)
        pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(block.getColumns(), block.rows())));
    return pipes;
}

inline void StorageWindowView::cleanCache()
{
    UInt32 w_bound;
    {
        std::lock_guard lock(fire_signal_mutex);
        w_bound = max_fired_watermark;
        if (w_bound == 0)
            return;

        if (!is_proctime)
        {
            if (max_watermark == 0)
                return;
            if (allowed_lateness)
            {
                UInt32 lateness_bound = addTime(max_timestamp, lateness_kind, -1 * lateness_num_units, time_zone);
                lateness_bound = getWindowLowerBound(lateness_bound);
                if (lateness_bound < w_bound)
                    w_bound = lateness_bound;
            }
        }
    }

    auto sql = generateCleanCacheQuery(w_bound);
    InterpreterAlterQuery alt_query(sql, global_context);
    alt_query.execute();

    std::lock_guard lock(fire_signal_mutex);
    watch_streams.remove_if([](std::weak_ptr<WindowViewBlockInputStream> & ptr) { return ptr.expired(); });
}

inline void StorageWindowView::fire(UInt32 watermark)
{
    if (target_table_id.empty() && watch_streams.empty())
        return;

    BlockInputStreamPtr in_stream;
    {
        std::lock_guard lock(mutex);
        in_stream = getNewBlocksInputStreamPtr(watermark);
    }

    if (target_table_id.empty())
    {
        in_stream->readPrefix();
        while (auto block = in_stream->read())
        {
            for (auto & watch_stream : watch_streams)
            {
                if (auto watch_stream_ptr = watch_stream.lock())
                    watch_stream_ptr->addBlock(block);
            }
        }
        in_stream->readSuffix();
    }
    else
    {
        try
        {
            StoragePtr target_table = getTargetStorage();
            auto lock = target_table->lockStructureForShare(global_context.getCurrentQueryId());
            auto out_stream = target_table->write(getInnerQuery(), global_context);
            in_stream->readPrefix();
            out_stream->writePrefix();
            while (auto block = in_stream->read())
            {
                for (auto & watch_stream : watch_streams)
                {
                    if (const auto & watch_stream_ptr = watch_stream.lock())
                        watch_stream_ptr->addBlock(block);
                }
                out_stream->write(std::move(block));
            }
            in_stream->readSuffix();
            out_stream->writeSuffix();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    fire_condition.notify_all();
}

std::shared_ptr<ASTCreateQuery> StorageWindowView::generateInnerTableCreateQuery(ASTStorage * storage, const String & database_name, const String & table_name)
{
    /// We will create a query to create an internal table.
    auto inner_create_query = std::make_shared<ASTCreateQuery>();
    inner_create_query->database = database_name;
    inner_create_query->table = table_name;

    auto inner_select_query = std::static_pointer_cast<ASTSelectQuery>(getInnerQuery());

    Aliases aliases;
    QueryAliasesVisitor::Data query_aliases_data{aliases};
    QueryAliasesVisitor(query_aliases_data).visit(inner_select_query);

    auto t_sample_block
        = InterpreterSelectQuery(inner_select_query, global_context, getParentStorage(), SelectQueryOptions(QueryProcessingStage::WithMergeableState))
              .getSampleBlock();

    auto columns_list = std::make_shared<ASTExpressionList>();

    if (is_time_column_func_now && is_tumble)
    {
        auto column_window = std::make_shared<ASTColumnDeclaration>();
        column_window->name = window_column_name;
        column_window->type
            = makeASTFunction("Tuple", std::make_shared<ASTIdentifier>("DateTime"), std::make_shared<ASTIdentifier>("DateTime"));
        columns_list->children.push_back(column_window);
    }

    for (auto & column : t_sample_block.getColumnsWithTypeAndName())
    {
        ParserIdentifierWithOptionalParameters parser;
        String sql = column.type->getName();
        ASTPtr ast = parseQuery(parser, sql.data(), sql.data() + sql.size(), "data type", 0);
        auto column_dec = std::make_shared<ASTColumnDeclaration>();
        column_dec->name = column.name;
        column_dec->type = ast;
        columns_list->children.push_back(column_dec);
    }

    ReplaceFunctionWindowMatcher::Data query_data;
    query_data.window_column_name = window_column_name;
    query_data.window_column_alias = window_column_alias;
    query_data.aliases = &aliases;
    ReplaceFunctionWindowMatcher::Visitor visitor(query_data);

    ReplaceFuncNowVisitorData parser_proc_time_data;
    InDepthNodeVisitor<OneTypeMatcher<ReplaceFuncNowVisitorData>, true> time_now_visitor(parser_proc_time_data);

    auto new_storage = std::make_shared<ASTStorage>();
    if (storage == nullptr)
    {
        new_storage->set(new_storage->engine, makeASTFunction("AggregatingMergeTree"));

        for (auto & child : inner_select_query->groupBy()->children)
            if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(child.get()))
                ast_with_alias->setAlias("");

        auto order_by = std::make_shared<ASTFunction>();
        order_by->name = "tuple";
        order_by->arguments = inner_select_query->groupBy();
        order_by->children.push_back(order_by->arguments);

        ASTPtr order_by_ptr = order_by;
        if (is_time_column_func_now)
            time_now_visitor.visit(order_by_ptr);
        visitor.visit(order_by_ptr);

        for (auto & child : order_by->arguments->children)
        {
            if (child->getColumnName() == window_column_name)
            {
                ASTPtr tmp = child;
                child = order_by->arguments->children[0];
                order_by->arguments->children[0] = tmp;
                break;
            }
        }
        new_storage->set(new_storage->order_by, order_by_ptr);
        new_storage->set(new_storage->primary_key, std::make_shared<ASTIdentifier>(window_column_name));
    }
    else
    {
        if (storage->ttl_table)
            throw Exception("TTL is not supported for inner table in Window View", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
        if (!endsWith(storage->engine->name, "MergeTree"))
            throw Exception(
                "The ENGINE of WindowView must be MergeTree family of table engines including the engines with replication support",
                ErrorCodes::INCORRECT_QUERY);

        new_storage->set(new_storage->engine, storage->engine->clone());
        if (storage->partition_by)
        {
            auto partition_by = storage->partition_by->clone();
            if (is_time_column_func_now)
                time_now_visitor.visit(partition_by);
            visitor.visit(partition_by);
            new_storage->set(new_storage->partition_by, partition_by);
        }
        if (storage->primary_key)
        {
            auto primary_key = storage->primary_key->clone();
            if (is_time_column_func_now)
                time_now_visitor.visit(primary_key);
            visitor.visit(primary_key);
            new_storage->set(new_storage->primary_key, primary_key);
        }
        if (storage->order_by)
        {
            auto order_by = storage->order_by->clone();
            if (is_time_column_func_now)
                time_now_visitor.visit(order_by);
            visitor.visit(order_by);
            new_storage->set(new_storage->order_by, order_by);
        }
        if (storage->sample_by)
        {
            auto sample_by = storage->sample_by->clone();
            if (is_time_column_func_now)
                time_now_visitor.visit(sample_by);
            visitor.visit(sample_by);
            new_storage->set(new_storage->sample_by, sample_by);
        }
        if (storage->settings)
            new_storage->set(new_storage->settings, storage->settings->clone());
    }

    auto new_columns = std::make_shared<ASTColumns>();
    new_columns->set(new_columns->columns, columns_list);
    inner_create_query->set(inner_create_query->columns_list, new_columns);
    inner_create_query->set(inner_create_query->storage, new_storage);

    return inner_create_query;
}

inline UInt32 StorageWindowView::getWindowLowerBound(UInt32 time_sec)
{
    IntervalKind window_interval_kind;
    if (is_tumble)
        window_interval_kind = window_kind;
    else
        window_interval_kind = hop_kind;

    switch (window_interval_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        if (is_tumble) \
            return ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, time_zone); \
        else \
            return ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, hop_num_units, time_zone); \
    }
        CASE_WINDOW_KIND(Second)
        CASE_WINDOW_KIND(Minute)
        CASE_WINDOW_KIND(Hour)
        CASE_WINDOW_KIND(Day)
        CASE_WINDOW_KIND(Week)
        CASE_WINDOW_KIND(Month)
        CASE_WINDOW_KIND(Quarter)
        CASE_WINDOW_KIND(Year)
#undef CASE_WINDOW_KIND
    }
    __builtin_unreachable();
}

inline UInt32 StorageWindowView::getWindowUpperBound(UInt32 time_sec)
{
    switch (window_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        UInt32 start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, time_zone); \
        return AddTime<IntervalKind::KIND>::execute(start, window_num_units, time_zone); \
    }
        CASE_WINDOW_KIND(Second)
        CASE_WINDOW_KIND(Minute)
        CASE_WINDOW_KIND(Hour)
        CASE_WINDOW_KIND(Day)
        CASE_WINDOW_KIND(Week)
        CASE_WINDOW_KIND(Month)
        CASE_WINDOW_KIND(Quarter)
        CASE_WINDOW_KIND(Year)
#undef CASE_WINDOW_KIND
    }
    __builtin_unreachable();
}

inline void StorageWindowView::addFireSignal(std::set<UInt32> & signals)
{
    std::lock_guard lock(fire_signal_mutex);
    for (auto & signal : signals)
        fire_signal.push_back(signal);
    fire_signal_condition.notify_all();
}

inline void StorageWindowView::updateMaxTimestamp(UInt32 timestamp)
{
    std::lock_guard lock(fire_signal_mutex);
    if (timestamp > max_timestamp)
        max_timestamp = timestamp;
}

inline void StorageWindowView::updateMaxWatermark(UInt32 watermark)
{
    std::lock_guard lock(fire_signal_mutex);
    if (max_watermark == 0)
    {
        max_watermark = watermark;
        return;
    }

    bool updated;
    if (is_watermark_strictly_ascending)
    {
        updated = max_watermark < watermark;
        while (max_watermark < watermark)
        {
            fire_signal.push_back(max_watermark);
            max_fired_watermark = max_watermark;
            max_watermark = addTime(max_watermark, window_kind, window_num_units, time_zone);
        }
    }
    else // strictly || bounded
    {
        UInt32 max_watermark_bias = addTime(max_watermark, watermark_kind, watermark_num_units, time_zone);
        updated = max_watermark_bias <= watermark;
        while (max_watermark_bias <= max_timestamp)
        {
            fire_signal.push_back(max_watermark);
            max_fired_watermark = max_watermark;
            max_watermark = addTime(max_watermark, window_kind, window_num_units, time_zone);
            max_watermark_bias = addTime(max_watermark, window_kind, window_num_units, time_zone);
        }
    }
    if (updated)
        fire_signal_condition.notify_all();
}

void StorageWindowView::threadFuncCleanCache()
{
    while (!shutdown_called)
    {
        try
        {
            sleep(clean_interval);
            cleanCache();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            break;
        }
    }
    if (!shutdown_called)
        clean_cache_task->scheduleAfter(RESCHEDULE_MS);
}

void StorageWindowView::threadFuncFireProc()
{
    std::unique_lock lock(fire_signal_mutex);
    while (!shutdown_called)
    {
        UInt32 timestamp_now = std::time(nullptr);
        while (next_fire_signal <= timestamp_now)
        {
            fire(next_fire_signal);
            max_fired_watermark = next_fire_signal;
            next_fire_signal = addTime(next_fire_signal, window_kind, window_num_units, time_zone);
        }

        next_fire_signal = getWindowUpperBound(timestamp_now);
        UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
        fire_signal_condition.wait_for(lock, std::chrono::microseconds(static_cast<UInt64>(next_fire_signal) * 1000000 - timestamp_usec));
    }
    if (!shutdown_called)
        fire_task->scheduleAfter(RESCHEDULE_MS);
}

void StorageWindowView::threadFuncFireEvent()
{
    std::unique_lock lock(fire_signal_mutex);
    while (!shutdown_called)
    {
        bool signaled = std::cv_status::no_timeout == fire_signal_condition.wait_for(lock, std::chrono::seconds(5));
        if (!signaled)
            continue;

        while (!fire_signal.empty())
        {
            fire(fire_signal.front());
            fire_signal.pop_front();
        }
    }
    if (!shutdown_called)
        fire_task->scheduleAfter(RESCHEDULE_MS);
}

BlockInputStreams StorageWindowView::watch(
    const Names & /*column_names*/,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    ASTWatchQuery & query = typeid_cast<ASTWatchQuery &>(*query_info.query);

    bool has_limit = false;
    UInt64 limit = 0;
    if (query.limit_length)
    {
        has_limit = true;
        limit = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value);
    }

    auto reader = std::make_shared<WindowViewBlockInputStream>(
        std::static_pointer_cast<StorageWindowView>(shared_from_this()),
        has_limit,
        limit,
        context.getSettingsRef().window_view_heartbeat_interval.totalSeconds());

    std::lock_guard lock(fire_signal_mutex);
    watch_streams.push_back(reader);
    processed_stage = QueryProcessingStage::Complete;

    return {reader};
}

StorageWindowView::StorageWindowView(
    const StorageID & table_id_,
    Context & local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_)
    : IStorage(table_id_)
    , global_context(local_context.getGlobalContext())
    , time_zone(DateLUT::instance())
{
    setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for Window View", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);

    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*query.select->list_of_selects->children.at(0));
    String select_database_name = local_context.getCurrentDatabase();
    String select_table_name;
    extractDependentTable(select_query, select_database_name, select_table_name);
    select_table_id = StorageID(select_database_name, select_table_name);
    inner_query = innerQueryParser(select_query);

    final_query = inner_query->clone();
    ReplaceFuncNowVisitorData final_query_data;
    InDepthNodeVisitor<OneTypeMatcher<ReplaceFuncNowVisitorData>, true>(final_query_data).visit(final_query);
    is_time_column_func_now = final_query_data.is_time_column_func_now;
    if (is_time_column_func_now && is_tumble)
        window_column_name = final_query_data.window_column_name;
    is_watermark_strictly_ascending = query.is_watermark_strictly_ascending;
    is_watermark_ascending = query.is_watermark_ascending;
    is_watermark_bounded = query.is_watermark_bounded;

    /// If the table is not specified - use the table `system.one`
    if (select_table_name.empty())
    {
        select_database_name = "system";
        select_table_name = "one";
    }

    DatabaseCatalog::instance().addDependency(select_table_id, table_id_);

    if (!query.to_table.empty())
        target_table_id = StorageID(query.to_database, query.to_table);

    clean_interval = local_context.getSettingsRef().window_view_clean_interval.totalSeconds();
    next_fire_signal = getWindowUpperBound(std::time(nullptr));

    if (query.is_watermark_strictly_ascending || query.is_watermark_ascending || query.is_watermark_bounded)
    {
        is_proctime = false;
        if (is_time_column_func_now)
            throw Exception("now() is not support for Event time processing.", ErrorCodes::INCORRECT_QUERY);
        if (query.is_watermark_ascending)
        {
            is_watermark_bounded = true;
            watermark_kind = IntervalKind::Second;
            watermark_num_units = 1;
        }
        else if (query.is_watermark_bounded)
        {
            // parser watermark function
            const auto & watermark_function = std::static_pointer_cast<ASTFunction>(query.watermark_function);
            if (!startsWith(watermark_function->name, "toInterval"))
                throw Exception("Illegal type of WATERMARK function, should be Interval", ErrorCodes::ILLEGAL_COLUMN);

            const auto & interval_units_p1 = std::static_pointer_cast<ASTLiteral>(watermark_function->children.front()->children.front());
            watermark_kind = strToIntervalKind(watermark_function->name.substr(10));
            try
            {
                watermark_num_units = boost::lexical_cast<int>(interval_units_p1->value.get<String>());
            }
            catch (const boost::bad_lexical_cast &)
            {
                throw Exception("Cannot parse string '" + interval_units_p1->value.get<String>() + "' as Integer.", ErrorCodes::CANNOT_PARSE_TEXT);
            }
            if (watermark_num_units <= 0)
                throw Exception("Value for WATERMARK function must be positive.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
    }

    if (query.allowed_lateness)
    {
        allowed_lateness = true;

        // parser lateness function
        const auto & lateness_function = std::static_pointer_cast<ASTFunction>(query.lateness_function);
        if (!startsWith(lateness_function->name, "toInterval"))
            throw Exception("Illegal type of ALLOWED_LATENESS function, should be Interval", ErrorCodes::ILLEGAL_COLUMN);

        const auto & interval_units_p1 = std::static_pointer_cast<ASTLiteral>(lateness_function->children.front()->children.front());
        lateness_kind = strToIntervalKind(lateness_function->name.substr(10));
        try
        {
            lateness_num_units = boost::lexical_cast<int>(interval_units_p1->value.get<String>());
        }
        catch (const boost::bad_lexical_cast &)
        {
            throw Exception(
                "Cannot parse string '" + interval_units_p1->value.get<String>() + "' as Integer.", ErrorCodes::CANNOT_PARSE_TEXT);
        }
        if (lateness_num_units <= 0)
            throw Exception("Value for ALLOWED_LATENESS function must be positive.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    if (attach_)
    {
        inner_table_id = StorageID(table_id_.database_name, generateInnerTableName(table_id_.table_name));
    }
    else
    {
        auto inner_create_query
            = generateInnerTableCreateQuery(query.storage, table_id_.database_name, generateInnerTableName(table_id_.table_name));

        InterpreterCreateQuery create_interpreter(inner_create_query, local_context);
        create_interpreter.setInternal(true);
        create_interpreter.execute();
        inner_storage = DatabaseCatalog::instance().getTable(StorageID(inner_create_query->database, inner_create_query->table));
        inner_table_id = inner_storage->getStorageID();
    }

    clean_cache_task = global_context.getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncCleanCache(); });
    if (is_proctime)
        fire_task = global_context.getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncFireProc(); });
    else
        fire_task = global_context.getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncFireEvent(); });
    clean_cache_task->deactivate();
    fire_task->deactivate();
}


ASTPtr StorageWindowView::innerQueryParser(ASTSelectQuery & query)
{
    if (!query.groupBy())
        throw Exception("GROUP BY query is required for " + getName(), ErrorCodes::INCORRECT_QUERY);

    // parse stage mergeable
    ASTPtr result = query.clone();
    ASTPtr expr_list = result;
    StageMergeableVisitorData stage_mergeable_data;
    InDepthNodeVisitor<OneTypeMatcher<StageMergeableVisitorData, false>, true>(stage_mergeable_data).visit(expr_list);
    if (!stage_mergeable_data.is_tumble && !stage_mergeable_data.is_hop)
        throw Exception("WINDOW FUNCTION is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);
    window_column_name = stage_mergeable_data.window_column_name;
    window_column_alias = stage_mergeable_data.window_column_alias;
    timestamp_column_name = stage_mergeable_data.timestamp_column_name;
    is_tumble = stage_mergeable_data.is_tumble;

    // parser window function
    ASTFunction & window_function = typeid_cast<ASTFunction &>(*stage_mergeable_data.window_function);
    const auto & arguments = window_function.arguments->children;
    const auto & arg1 = std::static_pointer_cast<ASTFunction>(arguments.at(1));
    if (!arg1 || !startsWith(arg1->name, "toInterval"))
        throw Exception("Illegal type of second argument of function " + arg1->name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);
    window_kind = strToIntervalKind(arg1->name.substr(10));
    const auto & interval_units_p1 = std::static_pointer_cast<ASTLiteral>(arg1->children.front()->children.front());
    window_num_units = stoi(interval_units_p1->value.get<String>());
    if (window_num_units <= 0)
        throw Exception("Interval value for WINDOW function must be positive.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (!is_tumble)
    {
        const auto & arg2 = std::static_pointer_cast<ASTFunction>(arguments.at(2));
        if (!arg2 || !startsWith(arg2->name, "toInterval"))
            throw Exception("Illegal type of last argument of function " + arg2->name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);
        hop_kind = strToIntervalKind(arg2->name.substr(10));
        const auto & interval_units_p2 = std::static_pointer_cast<ASTLiteral>(arg2->children.front()->children.front());
        hop_num_units = stoi(interval_units_p2->value.get<String>());
        if (hop_num_units <= 0)
            throw Exception("Interval value for WINDOW function must be positive.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }
    return result;
}

void StorageWindowView::writeIntoWindowView(StorageWindowView & window_view, const Block & block, const Context & context)
{
    Pipe pipe(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), Chunk(block.getColumns(), block.rows())));
    BlockInputStreamPtr source_stream;

    std::shared_lock<std::shared_mutex> fire_signal_lock;
    if (window_view.is_proctime)
    {
        fire_signal_lock = std::shared_lock<std::shared_mutex>(window_view.fire_signal_mutex);
        if (window_view.is_tumble)
        {
            UInt32 timestamp_now = std::time(nullptr);
            pipe.addSimpleTransform(std::make_shared<AddingConstColumnTransform<UInt32>>(
                pipe.getHeader(), std::make_shared<DataTypeDateTime>(), timestamp_now, "____timestamp"));
        }
        InterpreterSelectQuery select_block(window_view.getFinalQuery(), context, {std::move(pipe)}, QueryProcessingStage::WithMergeableState);

        source_stream = select_block.execute().in;
        source_stream = std::make_shared<SquashingBlockInputStream>(
            source_stream, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);
    }
    else
    {
        UInt32 t_max_fired_watermark = 0;
        if (window_view.allowed_lateness)
        {
            UInt32 t_max_timstamp = 0;
            UInt32 t_max_watermark = 0;
            {
                std::lock_guard lock(window_view.fire_signal_mutex);
                t_max_fired_watermark = window_view.max_fired_watermark;
                t_max_watermark = window_view.max_watermark;
                t_max_timstamp = window_view.max_timestamp;
            }

            if (t_max_timstamp!= 0)
            {
                UInt32 lateness_bound
                    = addTime(t_max_timstamp, window_view.lateness_kind, -1 * window_view.lateness_num_units, window_view.time_zone);
                if (window_view.is_watermark_bounded)
                {
                    UInt32 watermark_lower_bound = window_view.is_tumble
                        ? addTime(t_max_watermark, window_view.window_kind, -1 * window_view.window_num_units, window_view.time_zone)
                        : addTime(t_max_watermark, window_view.hop_kind, -1 * window_view.hop_num_units, window_view.time_zone);
                    if (watermark_lower_bound < lateness_bound)
                        lateness_bound = watermark_lower_bound;
                }

                ColumnsWithTypeAndName columns;
                columns.emplace_back(nullptr, std::make_shared<DataTypeDateTime>(), window_view.timestamp_column_name);
                ExpressionActionsPtr filter_expressions = std::make_shared<ExpressionActions>(columns, context);
                filter_expressions->add(
                    ExpressionAction::addColumn({std::make_shared<DataTypeDateTime>()->createColumnConst(1, toField(lateness_bound)),
                                                 std::make_shared<DataTypeDateTime>(),
                                                 "____lateness_bound"}));
                const auto & function_greater = FunctionFactory::instance().get("greaterOrEquals", context);
                filter_expressions->add(ExpressionAction::applyFunction(
                    function_greater, Names{window_view.timestamp_column_name, "____lateness_bound"}, "____filter"));
                pipe.addSimpleTransform(std::make_shared<FilterTransform>(pipe.getHeader(), filter_expressions, "____filter", true));
            }
        }

        UInt32 t_max_timstamp = 0;
        if (!window_view.is_tumble || window_view.is_watermark_bounded || window_view.allowed_lateness)
        {
            auto & column_timestamp = block.getByName(window_view.timestamp_column_name).column;
            const ColumnUInt32::Container & timestamp_data = static_cast<const ColumnUInt32 &>(*column_timestamp).getData();
            for (auto& timestamp : timestamp_data)
            {
                if (timestamp > t_max_timstamp)
                    t_max_timstamp = timestamp;
            }
        }

        InterpreterSelectQuery select_block(window_view.getFinalQuery(), context, {std::move(pipe)}, QueryProcessingStage::WithMergeableState);

        source_stream = select_block.execute().in;
        source_stream = std::make_shared<SquashingBlockInputStream>(
            source_stream, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);

        if (!window_view.is_tumble)
            source_stream
                = std::make_shared<WatermarkBlockInputStream>(source_stream, window_view, window_view.window_column_name, window_view.getWindowUpperBound(t_max_timstamp));
        else
            source_stream = std::make_shared<WatermarkBlockInputStream>(source_stream, window_view, window_view.window_column_name);

        if (window_view.is_watermark_bounded || window_view.allowed_lateness)
            std::static_pointer_cast<WatermarkBlockInputStream>(source_stream)->setMaxTimestamp(t_max_timstamp);

        if (window_view.allowed_lateness && t_max_fired_watermark != 0)
            std::static_pointer_cast<WatermarkBlockInputStream>(source_stream)->setAllowedLateness(t_max_fired_watermark);
    }

    auto & inner_storage = window_view.getInnerStorage();
    auto lock = inner_storage->lockStructureForShare(context.getCurrentQueryId());
    auto stream = inner_storage->write(window_view.getInnerQuery(), context);
    copyData(*source_stream, *stream);
}

void StorageWindowView::startup()
{
    // Start the working thread
    clean_cache_task->activateAndSchedule();
    fire_task->activateAndSchedule();
}

void StorageWindowView::shutdown()
{
    bool expected = false;
    if (!shutdown_called.compare_exchange_strong(expected, true))
        return;
    clean_cache_task->deactivate();
    fire_task->deactivate();
}

StorageWindowView::~StorageWindowView()
{
    shutdown();
}

Block & StorageWindowView::getHeader() const
{
    std::lock_guard lock(sample_block_lock);
    if (!sample_block)
    {
        sample_block = InterpreterSelectQuery(
                           getInnerQuery(), global_context, getParentStorage(), SelectQueryOptions(QueryProcessingStage::Complete))
                           .getSampleBlock();
        for (size_t i = 0; i < sample_block.columns(); ++i)
            sample_block.safeGetByPosition(i).column = sample_block.safeGetByPosition(i).column->convertToFullColumnIfConst();
    }
    return sample_block;
}

StoragePtr StorageWindowView::getParentStorage() const
{
    if (parent_storage == nullptr)
        parent_storage = DatabaseCatalog::instance().getTable(select_table_id);
    return parent_storage;
}

StoragePtr & StorageWindowView::getInnerStorage() const
{
    if (inner_storage == nullptr)
        inner_storage = DatabaseCatalog::instance().getTable(inner_table_id);
    return inner_storage;
}

ASTPtr StorageWindowView::getFetchColumnQuery(UInt32 watermark) const
{
    auto res_query = std::make_shared<ASTSelectQuery>();
    auto select = std::make_shared<ASTExpressionList>();
    select->children.push_back(std::make_shared<ASTAsterisk>());
    res_query->setExpression(ASTSelectQuery::Expression::SELECT, select);
    res_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();
    res_query->tables()->children.push_back(tables_elem);
    tables_elem->table_expression = table_expr;
    tables_elem->children.push_back(table_expr);
    table_expr->database_and_table_name = createTableIdentifier(inner_table_id.database_name, inner_table_id.table_name);
    table_expr->children.push_back(table_expr->database_and_table_name);

    auto func_tuple
        = makeASTFunction("tupleElement", std::make_shared<ASTIdentifier>(window_column_name), std::make_shared<ASTLiteral>(Field{UInt8(2)}));
    auto func_equals = makeASTFunction("equals", func_tuple, std::make_shared<ASTLiteral>(watermark));
    res_query->setExpression(ASTSelectQuery::Expression::WHERE, func_equals);

    return res_query;
}

StoragePtr & StorageWindowView::getTargetStorage() const
{
    if (target_storage == nullptr && !target_table_id.empty())
        target_storage = DatabaseCatalog::instance().getTable(target_table_id);
    return target_storage;
}

BlockInputStreamPtr StorageWindowView::getNewBlocksInputStreamPtr(UInt32 watermark)
{
    Pipes pipes;

    auto & storage = getInnerStorage();
    InterpreterSelectQuery fetch(getFetchColumnQuery(watermark), global_context, storage, SelectQueryOptions(QueryProcessingStage::FetchColumns));
    QueryPipeline pipeline;
    BlockInputStreams streams = fetch.executeWithMultipleStreams(pipeline);
    for (auto & stream : streams)
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::move(stream)));

    auto proxy_storage = std::make_shared<WindowViewProxyStorage>(
        StorageID(global_context.getCurrentDatabase(), "WindowViewProxyStorage"), getParentStorage()->getColumns(), std::move(pipes), QueryProcessingStage::WithMergeableState);

    InterpreterSelectQuery select(getFinalQuery(), global_context, proxy_storage, QueryProcessingStage::Complete);
    BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);

    /// Squashing is needed here because the view query can generate a lot of blocks
    /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
    /// and two-level aggregation is triggered).
    data = std::make_shared<SquashingBlockInputStream>(
        data, global_context.getSettingsRef().min_insert_block_size_rows,
        global_context.getSettingsRef().min_insert_block_size_bytes);
    return data;
}

void registerStorageWindowView(StorageFactory & factory)
{
    factory.registerStorage("WindowView", [](const StorageFactory::Arguments & args)
    {
        if (!args.attach && !args.local_context.getSettingsRef().allow_experimental_window_view)
            throw Exception(
                "Experimental WINDOW VIEW feature is not enabled (the setting 'allow_experimental_window_view')",
                ErrorCodes::SUPPORT_IS_DISABLED);

        return StorageWindowView::create(args.table_id, args.local_context, args.query, args.columns, args.attach);
    });
}

}
