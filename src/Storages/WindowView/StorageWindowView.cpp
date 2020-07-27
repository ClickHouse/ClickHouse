#include <numeric>
#include <regex>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsWindow.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
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
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/AddingConstColumnTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/StorageFactory.h>
#include <boost/lexical_cast.hpp>
#include <Common/typeid_cast.h>

#include <Storages/WindowView/ReplaceWindowColumnBlockInputStream.h>
#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/WindowView/WatermarkBlockInputStream.h>
#include <Storages/WindowView/WindowViewBlockInputStream.h>
#include <Storages/WindowView/WindowViewProxyStorage.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_WAS_NOT_DROPPED;
}

namespace
{
    const auto RESCHEDULE_MS = 500;

    struct MergeableQueryMatcher
    {
        using Visitor = InDepthNodeVisitor<MergeableQueryMatcher, true>;
        using TypeToVisit = ASTFunction;

        struct Data
        {
            ASTPtr window_function;
            String window_id_name;
            String window_id_alias;
            String serialized_window_function;
            String timestamp_column_name;
            bool is_tumble = false;
            bool is_hop = false;
        };

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * t = ast->as<ASTFunction>())
            {
                if (t->name == "TUMBLE" || t->name == "HOP")
                {
                    data.is_tumble = t->name == "TUMBLE";
                    data.is_hop = t->name == "HOP";
                    if (!data.window_function)
                    {
                        t->name = "WINDOW_ID";
                        data.window_id_name = t->getColumnName();
                        data.window_id_alias = t->alias;
                        data.window_function = t->clone();
                        data.window_function->setAlias("");
                        data.serialized_window_function = serializeAST(*data.window_function);
                        data.timestamp_column_name = t->arguments->children[0]->getColumnName();
                    }
                    else
                    {
                        auto temp_node = t->clone();
                        temp_node->setAlias("");
                        if (serializeAST(*temp_node) != data.serialized_window_function)
                            throw Exception("WINDOW VIEW only support ONE WINDOW FUNCTION", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
                    }
                }
            }
        }
    };

    struct ReplaceWindowIdMatcher
    {
    public:
        using Visitor = InDepthNodeVisitor<ReplaceWindowIdMatcher, true>;
        struct Data
        {
            String window_name;
        };

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * t = ast->as<ASTFunction>())
            {
                if (t->name == "WINDOW_ID")
                    t->name = data.window_name;
            }
        }
    };

    struct ReplaceFunctionNowData
    {
        using TypeToVisit = ASTFunction;

        bool is_time_column_func_now = false;
        String window_id_name;

        void visit(ASTFunction & node, ASTPtr & node_ptr)
        {
            if (node.name == "WINDOW_ID")
            {
                if (const auto * t = node.arguments->children[0]->as<ASTFunction>(); t && t->name == "now")
                {
                    is_time_column_func_now = true;
                    node_ptr->children[0]->children[0] = std::make_shared<ASTIdentifier>("____timestamp");
                    window_id_name = node.getColumnName();
                }
            }
        }
    };

    using ReplaceFunctionNowVisitor = InDepthNodeVisitor<OneTypeMatcher<ReplaceFunctionNowData>, true>;

    struct ReplaceFunctionWindowMatcher
    {
        using Visitor = InDepthNodeVisitor<ReplaceFunctionWindowMatcher, true>;

        struct Data{};

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data &)
        {
            if (auto * t = ast->as<ASTFunction>())
            {
            if (t->name == "HOP" || t->name == "TUMBLE")
                t->name = "WINDOW_ID";
            }
        }
    };

    class ToIdentifierMatcher
    {
    public:
        using Visitor = InDepthNodeVisitor<ToIdentifierMatcher, true>;

        struct Data
        {
            String window_id_name;
            String window_id_alias;
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
            else
                node_ptr = std::make_shared<ASTIdentifier>(node.getColumnName());
        }

        static void visit(const ASTIdentifier & node, ASTPtr & node_ptr, Data & data)
        {
            if (node.getColumnName() == data.window_id_alias)
                dynamic_cast<ASTIdentifier *>(node_ptr.get())->name = data.window_id_name;
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

    void extractWindowArgument(const ASTPtr & ast, IntervalKind::Kind & kind, Int64 & num_units, String err_msg)
    {
        const auto * arg = ast->as<ASTFunction>();
        if (!arg || !startsWith(arg->name, "toInterval"))
            throw Exception(err_msg, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        kind = strToIntervalKind(arg->name.substr(10));
        const auto * interval_unit = arg->children.front()->children.front()->as<ASTLiteral>();
        if (!interval_unit
            || (interval_unit->value.getType() != Field::Types::String && interval_unit->value.getType() != Field::Types::UInt64))
            throw Exception("Interval argument must be integer", ErrorCodes::BAD_ARGUMENTS);
        if (interval_unit->value.getType() == Field::Types::String)
            num_units = std::stoi(interval_unit->value.safeGet<String>());
        else
            num_units = interval_unit->value.safeGet<UInt64>();

        if (num_units <= 0)
            throw Exception("Value for Interval argument must be positive.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    UInt32 addTime(UInt32 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone)
    {
        switch (kind)
        {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: { \
        return AddTime<IntervalKind::KIND>::execute(time_sec, num_units, time_zone); \
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

        auto & inner_select_query = ast_select->list_of_selects->children.at(0);

        extractDependentTable(inner_select_query->as<ASTSelectQuery &>(), select_database_name, select_table_name);
    }
    else
        throw Exception(
            "Logical error while creating StorageWindowView."
            " Could not retrieve table name from select query.",
            DB::ErrorCodes::LOGICAL_ERROR);
}

UInt32 StorageWindowView::getCleanupBound()
{
    UInt32 w_bound;
    {
        std::lock_guard lock(fire_signal_mutex);
        w_bound = max_fired_watermark;
        if (w_bound == 0)
            return 0;

        if (!is_proctime)
        {
            if (max_watermark == 0)
                return 0;
            if (allowed_lateness)
            {
                UInt32 lateness_bound = addTime(max_timestamp, lateness_kind, -1 * lateness_num_units, *time_zone);
                lateness_bound = getWindowLowerBound(lateness_bound);
                if (lateness_bound < w_bound)
                    w_bound = lateness_bound;
            }
        }
    }
    return w_bound;
}

ASTPtr StorageWindowView::generateCleanupQuery()
{
    ASTPtr function_equal;
    function_equal = makeASTFunction("less", std::make_shared<ASTIdentifier>(window_id_name), std::make_shared<ASTLiteral>(getCleanupBound()));

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

static void executeDropQuery(ASTDropQuery::Kind kind, Context & context, const StorageID & target_table_id)
{
    if (DatabaseCatalog::instance().tryGetTable(target_table_id, context))
    {
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = target_table_id.database_name;
        drop_query->table = target_table_id.table_name;
        drop_query->kind = kind;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, context);
        drop_interpreter.execute();
    }
}

void StorageWindowView::drop()
{
    auto table_id = getStorageID();
    DatabaseCatalog::instance().removeDependency(select_table_id, table_id);
    executeDropQuery(ASTDropQuery::Kind::Drop, global_context, inner_table_id);

    std::lock_guard lock(mutex);
    is_dropped = true;
    fire_condition.notify_all();
}

void StorageWindowView::truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &)
{
    executeDropQuery(ASTDropQuery::Kind::Truncate, global_context, inner_table_id);
}

bool StorageWindowView::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Context & context)
{
    return getInnerStorage()->optimize(query, metadata_snapshot, partition, final, deduplicate, context);
}

Pipes StorageWindowView::blocksToPipes(BlocksList & blocks, Block & sample_block)
{
    Pipes pipes;
    for (auto & block : blocks)
        pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(block.getColumns(), block.rows())));
    return pipes;
}

inline void StorageWindowView::cleanup()
{
    InterpreterAlterQuery alt_query(generateCleanupQuery(), *wv_context);
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
        StoragePtr target_table = getTargetStorage();
        auto metadata_snapshot = target_table->getInMemoryMetadataPtr();
        auto lock = target_table->lockForShare(wv_context->getCurrentQueryId(), wv_context->getSettingsRef().lock_acquire_timeout);
        auto out_stream = target_table->write(getFinalQuery(), metadata_snapshot, *wv_context);
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
    fire_condition.notify_all();
}

std::shared_ptr<ASTCreateQuery> StorageWindowView::generateInnerTableCreateQuery(const ASTPtr & inner_query, ASTStorage * storage, const String & database_name, const String & table_name)
{
    /// We will create a query to create an internal table.
    auto inner_create_query = std::make_shared<ASTCreateQuery>();
    inner_create_query->database = database_name;
    inner_create_query->table = table_name;

    auto inner_select_query = std::static_pointer_cast<ASTSelectQuery>(inner_query);

    auto t_sample_block
        = InterpreterSelectQuery(
              inner_select_query, *wv_context, getParentStorage(), nullptr, SelectQueryOptions(QueryProcessingStage::WithMergeableState))
              .getSampleBlock();

    auto columns_list = std::make_shared<ASTExpressionList>();

    if (is_time_column_func_now)
    {
        auto column_window = std::make_shared<ASTColumnDeclaration>();
        column_window->name = window_id_name;
        column_window->type = std::make_shared<ASTIdentifier>("UInt32");
        columns_list->children.push_back(column_window);
    }

    for (const auto & column : t_sample_block.getColumnsWithTypeAndName())
    {
        ParserIdentifierWithOptionalParameters parser;
        String sql = column.type->getName();
        ASTPtr ast = parseQuery(parser, sql.data(), sql.data() + sql.size(), "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        auto column_dec = std::make_shared<ASTColumnDeclaration>();
        column_dec->name = column.name;
        column_dec->type = ast;
        columns_list->children.push_back(column_dec);
    }

    ToIdentifierMatcher::Data query_data;
    query_data.window_id_name = window_id_name;
    query_data.window_id_alias = window_id_alias;
    ToIdentifierMatcher::Visitor to_identifier_visitor(query_data);

    ReplaceFunctionNowData time_now_data;
    ReplaceFunctionNowVisitor time_now_visitor(time_now_data);
    ReplaceFunctionWindowMatcher::Data func_hop_data;
    ReplaceFunctionWindowMatcher::Visitor func_window_visitor(func_hop_data);

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
        to_identifier_visitor.visit(order_by_ptr);

        for (auto & child : order_by->arguments->children)
        {
            if (child->getColumnName() == window_id_name)
            {
                ASTPtr tmp = child;
                child = order_by->arguments->children[0];
                order_by->arguments->children[0] = tmp;
                break;
            }
        }
        new_storage->set(new_storage->order_by, order_by_ptr);
        new_storage->set(new_storage->primary_key, std::make_shared<ASTIdentifier>(window_id_name));
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

        auto visit = [&](const IAST * ast, IAST * & field)
        {
            if (ast)
            {
                auto node = ast->clone();
                if (is_time_column_func_now)
                    time_now_visitor.visit(node);
                func_window_visitor.visit(node);
                to_identifier_visitor.visit(node);
                new_storage->set(field, node);
            }
        };

        visit(storage->partition_by, new_storage->partition_by);
        visit(storage->primary_key, new_storage->primary_key);
        visit(storage->order_by, new_storage->order_by);
        visit(storage->sample_by, new_storage->sample_by);

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
            return ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, *time_zone); \
        else \
        {\
            UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, hop_num_units, *time_zone); \
            UInt32 w_end = AddTime<IntervalKind::KIND>::execute(w_start, hop_num_units, *time_zone);\
            return AddTime<IntervalKind::KIND>::execute(w_end, -1 * window_num_units, *time_zone);\
        }\
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
        {\
            UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, *time_zone); \
            return AddTime<IntervalKind::KIND>::execute(w_start, window_num_units, *time_zone); \
        }\
        else \
        {\
            UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, hop_num_units, *time_zone); \
            return AddTime<IntervalKind::KIND>::execute(w_start, hop_num_units, *time_zone);\
        }\
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
    for (const auto & signal : signals)
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
        max_watermark = getWindowUpperBound(watermark - 1);
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
            max_watermark
                = is_tumble ? addTime(max_watermark, window_kind, window_num_units, *time_zone) : addTime(max_watermark, hop_kind, hop_num_units, *time_zone);
        }
    }
    else // strictly || bounded
    {
        UInt32 max_watermark_bias = addTime(max_watermark, watermark_kind, watermark_num_units, *time_zone);
        updated = max_watermark_bias <= watermark;
        while (max_watermark_bias <= max_timestamp)
        {
            fire_signal.push_back(max_watermark);
            max_fired_watermark = max_watermark;
            if (is_tumble)
            {
                max_watermark = addTime(max_watermark, window_kind, window_num_units, *time_zone);
                max_watermark_bias = addTime(max_watermark, window_kind, window_num_units, *time_zone);
            }
            else
            {
                max_watermark = addTime(max_watermark, hop_kind, hop_num_units, *time_zone);
                max_watermark_bias = addTime(max_watermark, hop_kind, hop_num_units, *time_zone);
            }
        }
    }
    if (updated)
        fire_signal_condition.notify_all();
}

void StorageWindowView::threadFuncCleanup()
{
    while (!shutdown_called)
    {
        try
        {
            sleep(clean_interval);
            cleanup();
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
            try
            {
                fire(next_fire_signal);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
            max_fired_watermark = next_fire_signal;
            next_fire_signal = addTime(next_fire_signal, window_kind, window_num_units, *time_zone);
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
{
    wv_context = std::make_unique<Context>(global_context);
    wv_context->makeQueryContext();

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for Window View", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);

    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*query.select->list_of_selects->children.at(0));
    String select_database_name = global_context.getCurrentDatabase();
    String select_table_name;
    extractDependentTable(select_query, select_database_name, select_table_name);
    /// If the table is not specified - use the table `system.one`
    if (select_table_name.empty())
    {
        select_database_name = "system";
        select_table_name = "one";
    }
    select_table_id = StorageID(select_database_name, select_table_name);
    DatabaseCatalog::instance().addDependency(select_table_id, table_id_);

    // Parse inner query
    auto inner_query = innerQueryParser(select_query);

    // Parse mergeable query
    mergeable_query = inner_query->clone();
    ReplaceFunctionNowData func_now_data;
    ReplaceFunctionNowVisitor(func_now_data).visit(mergeable_query);
    is_time_column_func_now = func_now_data.is_time_column_func_now;
    if (is_time_column_func_now)
        window_id_name = func_now_data.window_id_name;

    // Parse final query
    final_query = mergeable_query->clone();
    ReplaceWindowIdMatcher::Data final_query_data;
    if (is_tumble)
        final_query_data.window_name = "TUMBLE";
    else
        final_query_data.window_name = "HOP";
    ReplaceWindowIdMatcher::Visitor(final_query_data).visit(final_query);

    is_watermark_strictly_ascending = query.is_watermark_strictly_ascending;
    is_watermark_ascending = query.is_watermark_ascending;
    is_watermark_bounded = query.is_watermark_bounded;
    target_table_id = query.to_table_id;

    eventTimeParser(query);

    if (is_tumble)
        window_column_name = std::regex_replace(window_id_name, std::regex("WINDOW_ID"), "TUMBLE");
    else
        window_column_name = std::regex_replace(window_id_name, std::regex("WINDOW_ID"), "HOP");

    auto generate_inner_table_name = [](const String & table_name) { return ".inner." + table_name; };
    if (attach_)
    {
        inner_table_id = StorageID(table_id_.database_name, generate_inner_table_name(table_id_.table_name));
    }
    else
    {
        auto inner_create_query
            = generateInnerTableCreateQuery(inner_query, query.storage, table_id_.database_name, generate_inner_table_name(table_id_.table_name));

        InterpreterCreateQuery create_interpreter(inner_create_query, *wv_context);
        create_interpreter.setInternal(true);
        create_interpreter.execute();
        inner_storage = DatabaseCatalog::instance().getTable(StorageID(inner_create_query->database, inner_create_query->table), global_context);
        inner_table_id = inner_storage->getStorageID();
    }

    clean_interval = global_context.getSettingsRef().window_view_clean_interval.totalSeconds();
    next_fire_signal = getWindowUpperBound(std::time(nullptr));

    clean_cache_task = wv_context->getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncCleanup(); });
    if (is_proctime)
        fire_task = wv_context->getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncFireProc(); });
    else
        fire_task = wv_context->getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncFireEvent(); });
    clean_cache_task->deactivate();
    fire_task->deactivate();
}


ASTPtr StorageWindowView::innerQueryParser(ASTSelectQuery & query)
{
    if (!query.groupBy())
        throw Exception("GROUP BY query is required for " + getName(), ErrorCodes::INCORRECT_QUERY);

    // Parse stage mergeable
    ASTPtr result = query.clone();
    ASTPtr expr_list = result;
    MergeableQueryMatcher::Data stage_mergeable_data;
    MergeableQueryMatcher::Visitor(stage_mergeable_data).visit(expr_list);
    if (!stage_mergeable_data.is_tumble && !stage_mergeable_data.is_hop)
        throw Exception("WINDOW FUNCTION is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);
    window_id_name = stage_mergeable_data.window_id_name;
    window_id_alias = stage_mergeable_data.window_id_alias;
    timestamp_column_name = stage_mergeable_data.timestamp_column_name;
    is_tumble = stage_mergeable_data.is_tumble;

    // Parse window function
    ASTFunction & window_function = typeid_cast<ASTFunction &>(*stage_mergeable_data.window_function);
    const auto & arguments = window_function.arguments->children;
    extractWindowArgument(
        arguments.at(1),
        window_kind,
        window_num_units,
        "Illegal type of second argument of function " + window_function.name + " should be Interval");

    if (!is_tumble)
    {
        hop_kind = window_kind;
        hop_num_units = window_num_units;
        extractWindowArgument(
            arguments.at(2),
            window_kind,
            window_num_units,
            "Illegal type of third argument of function " + window_function.name + " should be Interval");
        slice_num_units= std::gcd(hop_num_units, window_num_units);
    }

    // parse time zone
    size_t time_zone_arg_num = is_tumble ? 2 : 3;
    if (arguments.size() > time_zone_arg_num)
    {
        const auto & ast = arguments.at(time_zone_arg_num);
        const auto * time_zone_ast = ast->as<ASTLiteral>();
        if (!time_zone_ast || time_zone_ast->value.getType() != Field::Types::String)
            throw Exception(
                "Illegal column #" + std::to_string(time_zone_arg_num) + " of time zone argument of function, must be constant string",
                ErrorCodes::ILLEGAL_COLUMN);
        time_zone = &DateLUT::instance(time_zone_ast->value.safeGet<String>());
    }
    else
        time_zone = &DateLUT::instance();

    return result;
}

void StorageWindowView::eventTimeParser(const ASTCreateQuery & query)
{
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
            extractWindowArgument(
                query.watermark_function, watermark_kind, watermark_num_units, "Illegal type WATERMARK function should be Interval");
        }
    }

    if (query.allowed_lateness)
    {
        allowed_lateness = true;
        extractWindowArgument(
            query.lateness_function, lateness_kind, lateness_num_units, "Illegal type ALLOWED_LATENESS function should be Interval");
    }
}

void StorageWindowView::writeIntoWindowView(StorageWindowView & window_view, const Block & block, const Context & context)
{
    Pipe pipe(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), Chunk(block.getColumns(), block.rows())));
    BlockInputStreamPtr source_stream;

    UInt32 lateness_bound = 0;
    UInt32 t_max_watermark = 0;
    UInt32 t_max_timestamp = 0;
    UInt32 t_max_fired_watermark = 0;
    {
        std::lock_guard lock(window_view.fire_signal_mutex);
        t_max_fired_watermark = window_view.max_fired_watermark;
        t_max_watermark = window_view.max_watermark;
        t_max_timestamp = window_view.max_timestamp;
    }

    // Filter outdated data
    if (window_view.allowed_lateness && t_max_timestamp != 0)
    {
        lateness_bound
            = addTime(t_max_timestamp, window_view.lateness_kind, -1 * window_view.lateness_num_units, *window_view.time_zone);
        if (window_view.is_watermark_bounded)
        {
            UInt32 watermark_lower_bound = window_view.is_tumble
                ? addTime(t_max_watermark, window_view.window_kind, -1 * window_view.window_num_units, *window_view.time_zone)
                : addTime(t_max_watermark, window_view.hop_kind, -1 * window_view.hop_num_units, *window_view.time_zone);
            if (watermark_lower_bound < lateness_bound)
                lateness_bound = watermark_lower_bound;
        }
    }
    else if (! window_view.is_time_column_func_now)
    {
        lateness_bound = t_max_fired_watermark;
    }

    if (lateness_bound > 0)
    {
        ColumnsWithTypeAndName columns;
        columns.emplace_back(nullptr, std::make_shared<DataTypeDateTime>(), window_view.timestamp_column_name);
        ExpressionActionsPtr filter_expressions = std::make_shared<ExpressionActions>(columns, context);
        filter_expressions->add(ExpressionAction::addColumn(
            {std::make_shared<DataTypeDateTime>()->createColumnConst(1, toField(lateness_bound)),
             std::make_shared<DataTypeDateTime>(),
             "____lateness_bound"}));
        const auto & function_greater = FunctionFactory::instance().get("greaterOrEquals", context);
        filter_expressions->add(ExpressionAction::applyFunction(
            function_greater, Names{window_view.timestamp_column_name, "____lateness_bound"}, "____filter"));
        pipe.addSimpleTransform(std::make_shared<FilterTransform>(pipe.getHeader(), filter_expressions, "____filter", true));
    }

    std::shared_lock<std::shared_mutex> fire_signal_lock;
    if (window_view.is_proctime)
    {
        fire_signal_lock = std::shared_lock<std::shared_mutex>(window_view.fire_signal_mutex);
        if (window_view.is_time_column_func_now)
            pipe.addSimpleTransform(std::make_shared<AddingConstColumnTransform<UInt32>>(
                pipe.getHeader(), std::make_shared<DataTypeDateTime>(), std::time(nullptr), "____timestamp"));
        InterpreterSelectQuery select_block(window_view.getMergeableQuery(), context, {std::move(pipe)}, QueryProcessingStage::WithMergeableState);

        source_stream = select_block.execute().getInputStream();
        source_stream = std::make_shared<SquashingBlockInputStream>(
            source_stream, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);
    }
    else
    {
        InterpreterSelectQuery select_block(window_view.getMergeableQuery(), context, {std::move(pipe)}, QueryProcessingStage::WithMergeableState);

        source_stream = select_block.execute().getInputStream();
        source_stream = std::make_shared<SquashingBlockInputStream>(
            source_stream, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);
        source_stream = std::make_shared<WatermarkBlockInputStream>(source_stream, window_view, window_view.window_id_name);

        if (window_view.is_watermark_bounded || window_view.allowed_lateness)
        {
            UInt32 block_max_timestamp = 0;
            if (window_view.is_watermark_bounded || window_view.allowed_lateness)
            {
                const auto & timestamp_data = typeid_cast<const ColumnUInt32 &>(*block.getByName(window_view.timestamp_column_name).column).getData();
                for (const auto & timestamp : timestamp_data)
                {
                    if (timestamp > block_max_timestamp)
                        block_max_timestamp = timestamp;
                }
            }
            std::static_pointer_cast<WatermarkBlockInputStream>(source_stream)->setMaxTimestamp(block_max_timestamp);
        }

        if (window_view.allowed_lateness && t_max_fired_watermark != 0)
            std::static_pointer_cast<WatermarkBlockInputStream>(source_stream)->setAllowedLateness(t_max_fired_watermark);
    }

    auto & inner_storage = window_view.getInnerStorage();
    auto metadata_snapshot = inner_storage->getInMemoryMetadataPtr();
    auto lock = inner_storage->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
    auto stream = inner_storage->write(window_view.getMergeableQuery(), metadata_snapshot, context);
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
                           getFinalQuery(), *wv_context, getParentStorage(), nullptr, SelectQueryOptions(QueryProcessingStage::Complete))
                           .getSampleBlock();
        for (size_t i = 0; i < sample_block.columns(); ++i)
            sample_block.safeGetByPosition(i).column = sample_block.safeGetByPosition(i).column->convertToFullColumnIfConst();
    }
    return sample_block;
}

StoragePtr StorageWindowView::getParentStorage() const
{
    if (parent_storage == nullptr)
        parent_storage = DatabaseCatalog::instance().getTable(select_table_id, global_context);
    return parent_storage;
}

StoragePtr & StorageWindowView::getInnerStorage() const
{
    if (inner_storage == nullptr)
        inner_storage = DatabaseCatalog::instance().getTable(inner_table_id, global_context);
    return inner_storage;
}

ASTPtr StorageWindowView::getFetchColumnQuery(UInt32 w_start, UInt32 w_end) const
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

    if (is_tumble)
    {
        auto func_equals = makeASTFunction("equals", std::make_shared<ASTIdentifier>(window_id_name), std::make_shared<ASTLiteral>(w_end));
        res_query->setExpression(ASTSelectQuery::Expression::PREWHERE, func_equals);
    }
    else
    {
        auto func_array = makeASTFunction("array");
        while (w_end > w_start)
        {
            func_array ->arguments->children.push_back(std::make_shared<ASTLiteral>(w_end));
            w_end = addTime(w_end, window_kind, -1 * slice_num_units, *time_zone);
        }
        auto func_has = makeASTFunction("has", func_array, std::make_shared<ASTIdentifier>(window_id_name));
        res_query->setExpression(ASTSelectQuery::Expression::PREWHERE, func_has);
    }

    return res_query;
}

StoragePtr & StorageWindowView::getTargetStorage() const
{
    if (target_storage == nullptr && !target_table_id.empty())
        target_storage = DatabaseCatalog::instance().getTable(target_table_id, global_context);
    return target_storage;
}

BlockInputStreamPtr StorageWindowView::getNewBlocksInputStreamPtr(UInt32 watermark)
{
    UInt32 w_start = addTime(watermark, window_kind, -1 * window_num_units, *time_zone);

    InterpreterSelectQuery fetch(
        getFetchColumnQuery(w_start, watermark),
        *wv_context,
        getInnerStorage(),
        nullptr,
        SelectQueryOptions(QueryProcessingStage::FetchColumns));
    BlockInputStreamPtr in_stream = fetch.execute().getInputStream();

    in_stream = std::make_shared<ReplaceWindowColumnBlockInputStream>(in_stream, window_column_name, w_start, watermark);

    Pipes pipes;
    pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::move(in_stream)));

    auto parent_table_metadata = getParentStorage()->getInMemoryMetadataPtr();
    auto proxy_storage = std::make_shared<WindowViewProxyStorage>(
        StorageID(getStorageID().database_name, "WindowViewProxyStorage"), parent_table_metadata->getColumns(), std::move(pipes), QueryProcessingStage::WithMergeableState);

    SelectQueryOptions query_options(QueryProcessingStage::Complete);
    query_options.ignore_limits = true;
    query_options.ignore_quota = true;
    InterpreterSelectQuery select(getFinalQuery(), *wv_context, proxy_storage, nullptr, query_options);
    BlockInputStreamPtr data = select.execute().getInputStream();

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
