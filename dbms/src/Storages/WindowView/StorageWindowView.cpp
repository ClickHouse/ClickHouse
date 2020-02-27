#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/AddingConstColumnBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
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
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/formatAST.h>
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
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_PARSE_TEXT;
}

namespace
{
    const auto RESCHEDULE_MS = 500;

    class ParserStageMergeableOneMatcher
    {
    public:
        using Visitor = InDepthNodeVisitor<ParserStageMergeableOneMatcher, true>;

        struct Data
        {
            ASTPtr window_function;
            String window_column_name;
            bool is_tumble = false;
            bool is_hop = false;
        };

        static bool needChildVisit(ASTPtr & node, const ASTPtr &)
        {
            if (node->as<ASTFunction>())
                return false;
            return true;
        }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (const auto * t = ast->as<ASTFunction>())
                visit(*t, ast, data);
        }

    private:
        static void visit(const ASTFunction & node, ASTPtr & node_ptr, Data & data)
        {
            if (node.name == "TUMBLE")
            {
                if (!data.window_function)
                {
                    data.is_tumble = true;
                    data.window_column_name = node.getColumnName();
                    data.window_function = node.clone();
                }
                else if (serializeAST(node) != serializeAST(*data.window_function))
                    throw Exception("WINDOW VIEW only support ONE WINDOW FUNCTION", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
            }
            else if (node.name == "HOP")
            {
                if (!data.window_function)
                {
                    data.is_hop = true;
                    data.window_function = node.clone();
                    auto ptr_ = node.clone();
                    std::static_pointer_cast<ASTFunction>(ptr_)->setAlias("");
                    auto arrayJoin = makeASTFunction("arrayJoin", ptr_);
                    arrayJoin->alias = node.alias;
                    node_ptr = arrayJoin;
                    data.window_column_name = arrayJoin->getColumnName();
                }
                else if (serializeAST(node) != serializeAST(*data.window_function))
                    throw Exception("WINDOW VIEW only support ONE WINDOW FUNCTION", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
            }
        }
    };

    class ParserProcTimeFinalMatcher
    {
    public:
        using Visitor = InDepthNodeVisitor<ParserProcTimeFinalMatcher, true>;

        struct Data
        {
            bool is_proctime_tumble = false;
            String window_column_name;
        };

        static bool needChildVisit(ASTPtr &, const ASTPtr &)
        {
            return true;
        }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (const auto * t = ast->as<ASTFunction>())
                visit(*t, ast, data);
        }

    private:
        static void visit(const ASTFunction & node, ASTPtr & node_ptr, Data & data)
        {
            if (node.name == "TUMBLE")
            {
                if (const auto * t = node.arguments->children[0]->as<ASTFunction>(); t && t->name == "now")
                {
                    data.is_proctime_tumble = true;
                    node_ptr->children[0]->children[0] = std::make_shared<ASTIdentifier>("____timestamp");
                    data.window_column_name = node.getColumnName();
                }
            }
        }
    };

    static inline IntervalKind strToIntervalKind(const String& interval_str)
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

    static inline String generateInnerTableName(const String & table_name) { return ".inner." + table_name; }

    static ASTPtr generateDeleteRetiredQuery(StorageID inner_table_id, UInt32 timestamp)
    {
        auto function_equal = makeASTFunction(
            "less", std::make_shared<ASTIdentifier>("____w_end"), std::make_shared<ASTLiteral>(timestamp));

        auto alterCommand = std::make_shared<ASTAlterCommand>();
        alterCommand->type = ASTAlterCommand::DELETE;
        alterCommand->predicate = function_equal;
        alterCommand->children.push_back(alterCommand->predicate);

        auto alterCommandList = std::make_shared<ASTAlterCommandList>();
        alterCommandList->add(alterCommand);

        auto alterQuery = std::make_shared<ASTAlterQuery>();
        alterQuery->database = inner_table_id.database_name;
        alterQuery->table = inner_table_id.table_name;
        alterQuery->set(alterQuery->command_list, alterCommandList);
        return alterQuery;
    }

    static std::shared_ptr<ASTSelectQuery> generateFetchColumnsQuery(const StorageID & inner_storage)
    {
        auto res_query = std::make_shared<ASTSelectQuery>();
        auto select = std::make_shared<ASTExpressionList>();
        select->children.push_back(std::make_shared<ASTAsterisk>());
        res_query->setExpression(ASTSelectQuery::Expression::SELECT, select);

        auto tableInSelectQuery = std::make_shared<ASTTablesInSelectQuery>();
        auto tableInSelectQueryElement = std::make_shared<ASTTablesInSelectQueryElement>();
        res_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
        auto tables = res_query->tables();
        auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
        auto table_expr = std::make_shared<ASTTableExpression>();
        tables->children.push_back(tables_elem);
        tables_elem->table_expression = table_expr;
        tables_elem->children.push_back(table_expr);
        table_expr->database_and_table_name = createTableIdentifier(inner_storage.database_name, inner_storage.table_name);
        table_expr->children.push_back(table_expr->database_and_table_name);

        return res_query;
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

        auto & inner_query = ast_select->list_of_selects->children.at(0);

        extractDependentTable(inner_query->as<ASTSelectQuery &>(), select_database_name, select_table_name);
    }
    else
        throw Exception(
            "Logical error while creating StorageWindowView."
            " Could not retrieve table name from select query.",
            DB::ErrorCodes::LOGICAL_ERROR);
}

void StorageWindowView::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    Dependencies dependencies = global_context.getDependencies(table_id);
    if (!dependencies.empty())
    {
        StorageID dependent_table_id = dependencies.front();
        throw Exception("Table has dependency " + dependent_table_id.getNameForLogs(), ErrorCodes::TABLE_WAS_NOT_DROPPED);
    }
}

static void executeDropQuery(ASTDropQuery::Kind kind, Context & global_context, const StorageID & target_table_id)
{
    if (global_context.tryGetTable(target_table_id))
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
    global_context.removeDependency(select_table_id, table_id);

    if (!inner_table_id.empty())
        executeDropQuery(ASTDropQuery::Kind::Drop, global_context, inner_table_id);

    std::lock_guard lock(mutex);
    is_dropped = true;
    fire_condition.notify_all();
}

void StorageWindowView::truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &)
{
    if (!inner_table_id.empty())
        executeDropQuery(ASTDropQuery::Kind::Truncate, global_context, inner_table_id);
    else
    {
        std::lock_guard lock(mutex);
        mergeable_blocks = std::make_shared<std::list<BlocksListPtr>>();
    }
}

bool StorageWindowView::optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    if (inner_table_id.empty())
        throw Exception(
            "OPTIMIZE only supported when creating WINDOW VIEW within INNER table.", ErrorCodes::INCORRECT_QUERY);
    return getInnerStorage()->optimize(query, partition, final, deduplicate, context);
}

Pipes StorageWindowView::blocksToPipes(BlocksListPtrs & blocks, Block & sample_block)
{
    Pipes pipes;
    for (auto & blocks_ : *blocks)
        pipes.emplace_back(std::make_shared<BlocksListSource>(blocks_, sample_block));

    return pipes;
}

inline void StorageWindowView::cleanCache()
{
    //delete fired blocks
    UInt32 timestamp_now = std::time(nullptr);
    UInt32 w_lower_bound = getWindowLowerBound(timestamp_now, -1);
    if (watermark_num_units != 0)
        w_lower_bound = getWatermark(w_lower_bound);
    if (!inner_table_id.empty())
    {
        auto sql = generateDeleteRetiredQuery(inner_table_id, w_lower_bound);
        InterpreterAlterQuery alt_query(sql, global_context);
        alt_query.execute();
    }
    else
    {
        std::lock_guard lock(mutex);
        for (BlocksListPtr mergeable_block : *mergeable_blocks)
        {
            mergeable_block->remove_if([&w_lower_bound](Block & block_)
            {
                auto & column_ = block_.getByName("____w_end").column;
                const auto & data = static_cast<const ColumnUInt32 &>(*column_).getData();
                for (size_t i = 0; i < column_->size(); ++i)
                {
                    if (data[i] >= w_lower_bound)
                        return false;
                }
                return true;
            });
        }
        mergeable_blocks->remove_if([](BlocksListPtr & ptr) { return ptr->size() == 0; });
    }

    {
        std::unique_lock lock(fire_signal_mutex);
        watch_streams.remove_if([](std::weak_ptr<WindowViewBlockInputStream> & ptr) { return ptr.expired(); });
    }
}

inline void StorageWindowView::fire(UInt32 timestamp_)
{
    if (target_table_id.empty() && watch_streams.empty())
        return;

    auto in_stream = getNewBlocksInputStreamPtr(timestamp_);
    if (target_table_id.empty())
    {
        while (auto block_ = in_stream->read())
        {
            for (auto & watch_stream : watch_streams)
            {
                if (auto watch_stream_ = watch_stream.lock())
                    watch_stream_->addBlock(block_);
            }
        }
    }
    else
    {
        StoragePtr target_table = getTargetStorage();
        auto _lock = target_table->lockStructureForShare(true, global_context.getCurrentQueryId());
        auto out_stream = target_table->write(getInnerQuery(), global_context);

        while (auto block_ = in_stream->read())
        {
            for (auto & watch_stream : watch_streams)
            {
                if (auto watch_stream_ = watch_stream.lock())
                    watch_stream_->addBlock(block_);
            }
            out_stream->write(std::move(block_));
        }
    }
    fire_condition.notify_all();
}

std::shared_ptr<ASTCreateQuery> StorageWindowView::generateInnerTableCreateQuery(const ASTCreateQuery & inner_create_query, const String & database_name, const String & table_name)
{
    /// We will create a query to create an internal table.
    auto manual_create_query = std::make_shared<ASTCreateQuery>();
    manual_create_query->database = database_name;
    manual_create_query->table = table_name;

    auto new_columns_list = std::make_shared<ASTColumns>();

    auto sample_block_
        = InterpreterSelectQuery(getInnerQuery(), global_context, getParentStorage(), SelectQueryOptions(QueryProcessingStage::WithMergeableState))
              .getSampleBlock();

    auto columns_list = std::make_shared<ASTExpressionList>();

    if (is_proctime_tumble)
    {
        auto column_window = std::make_shared<ASTColumnDeclaration>();
        column_window->name = window_column_name;
        column_window->type
            = makeASTFunction("Tuple", std::make_shared<ASTIdentifier>("DateTime"), std::make_shared<ASTIdentifier>("DateTime"));
        columns_list->children.push_back(column_window);
    }

    for (auto & column_ : sample_block_.getColumnsWithTypeAndName())
    {
        ParserIdentifierWithOptionalParameters parser;
        String sql = column_.type->getName();
        ASTPtr ast = parseQuery(parser, sql.data(), sql.data() + sql.size(), "data type", 0);
        auto column_dec = std::make_shared<ASTColumnDeclaration>();
        column_dec->name = column_.name;
        column_dec->type = ast;
        columns_list->children.push_back(column_dec);
    }
    auto column_wend = std::make_shared<ASTColumnDeclaration>();
    column_wend->name = "____w_end";
    column_wend->type = std::make_shared<ASTIdentifier>("DateTime");
    columns_list->children.push_back(column_wend);

    new_columns_list->set(new_columns_list->columns, columns_list);
    manual_create_query->set(manual_create_query->columns_list, new_columns_list);
    manual_create_query->set(manual_create_query->storage, inner_create_query.storage->ptr());

    return manual_create_query;
}

inline UInt32 StorageWindowView::getWindowLowerBound(UInt32 time_sec, int window_id_skew)
{
    switch (window_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        UInt32 res = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, time_zone); \
        if (window_id_skew != 0) \
            return AddTime<IntervalKind::KIND>::execute(res, window_id_skew * window_num_units, time_zone); \
        else \
            return res; \
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

inline UInt32 StorageWindowView::getWindowUpperBound(UInt32 time_sec, int window_id_skew)
{
    switch (window_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        UInt32 start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, time_zone); \
        UInt32 res = AddTime<IntervalKind::KIND>::execute(start, window_num_units, time_zone); \
        if (window_id_skew != 0) \
            return AddTime<IntervalKind::KIND>::execute(res, window_id_skew * window_num_units, time_zone); \
        else \
            return res; \
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

static UInt32 addTime(UInt32 time_sec, IntervalKind::Kind window_kind, int window_num_units, const DateLUTImpl & time_zone)
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

inline UInt32 StorageWindowView::getWatermark(UInt32 time_sec)
{
    switch (watermark_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        return AddTime<IntervalKind::KIND>::execute(time_sec, watermark_num_units, time_zone); \
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

inline void StorageWindowView::addLateFireSignal(UInt32 timestamp_)
{
    std::unique_lock lock(fire_signal_mutex);
    late_fire_signal.push_back(timestamp_);
    late_signal_condition.notify_all();
}

void StorageWindowView::threadFuncCleanCache()
{
    while (!shutdown_called)
    {
        try
        {
            cleanCache();
            sleep(clean_interval);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            break;
        }
    }
    if (!shutdown_called)
        cleanCacheTask->scheduleAfter(RESCHEDULE_MS);
}

void StorageWindowView::threadFuncFire()
{
    std::unique_lock lock(fire_signal_mutex);
    while (!shutdown_called)
    {
        UInt32 timestamp_now = std::time(nullptr);
        while (next_fire_signal <= timestamp_now)
        {
            fire(next_fire_signal);
            next_fire_signal = addTime(next_fire_signal, window_kind, window_num_units, time_zone);
        }

        next_fire_signal = (getWindowUpperBound(timestamp_now));
        UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
        late_signal_condition.wait_for(lock, std::chrono::microseconds(static_cast<UInt64>(next_fire_signal) * 1000000 - timestamp_usec));

        // fire late events
        while (true)
        {
            UInt32 timestamp_;
            {
                if (late_fire_signal.empty())
                    break;
                timestamp_ = late_fire_signal.front();
                late_fire_signal.pop_front();
            }
            fire(timestamp_);
        }
    }
    if (!shutdown_called)
        fireTask->scheduleAfter(RESCHEDULE_MS);
}

BlockInputStreams StorageWindowView::watch(
    const Names & /*column_names*/,
    const SelectQueryInfo & query_info,
    const Context & /*context*/,
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
        limit);

    {
        std::unique_lock lock(fire_signal_mutex);
        watch_streams.push_back(reader);
    }

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

    auto inner_query_ = query.select->list_of_selects->children.at(0);

    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*inner_query_);
    String select_database_name = local_context.getCurrentDatabase();
    String select_table_name;
    extractDependentTable(select_query, select_database_name, select_table_name);
    select_table_id = StorageID(select_database_name, select_table_name);
    inner_query = innerQueryParser(select_query);

    final_query = inner_query->clone();
    ParserProcTimeFinalMatcher::Data final_query_data;
    ParserProcTimeFinalMatcher::Visitor(final_query_data).visit(final_query);
    is_proctime_tumble = final_query_data.is_proctime_tumble;
    if (is_proctime_tumble)
        window_column_name = final_query_data.window_column_name;

    /// If the table is not specified - use the table `system.one`
    if (select_table_name.empty())
    {
        select_database_name = "system";
        select_table_name = "one";
    }

    global_context.addDependency(select_table_id, table_id_);

    if (!query.to_table.empty())
        target_table_id = StorageID(query.to_database, query.to_table);

    clean_interval = local_context.getSettingsRef().window_view_clean_interval.totalSeconds();
    mergeable_blocks = std::make_shared<std::list<BlocksListPtr>>();
    next_fire_signal = getWindowUpperBound(std::time(nullptr));

    if (query.watermark_function)
    {
        if (is_proctime_tumble)
            throw Exception("WATERMARK is not support for Processing time processing.", ErrorCodes::INCORRECT_QUERY);

        // parser watermark function
        const auto & watermark_function = std::static_pointer_cast<ASTFunction>(query.watermark_function);
        if (!startsWith(watermark_function->name, "toInterval"))
            throw Exception(
                "Illegal type WATERMARK function " + watermark_function->name + ", should be Interval", ErrorCodes::ILLEGAL_COLUMN);

        String interval_str = watermark_function->name.substr(10);
        const auto & interval_units_p1 = std::static_pointer_cast<ASTLiteral>(watermark_function->children.front()->children.front());
        watermark_kind = strToIntervalKind(interval_str);
        try
        {
            watermark_num_units = boost::lexical_cast<int>(interval_units_p1->value.get<String>());
        }
        catch (const boost::bad_lexical_cast &)
        {
            throw Exception("Cannot parse string '" + interval_units_p1->value.get<String>() + "' as Integer.", ErrorCodes::CANNOT_PARSE_TEXT);
        }
        if (watermark_num_units > 0)
            watermark_num_units *= -1;
    }

    if (query.storage)
    {
        if (attach_)
        {
            inner_table_id = StorageID(table_id_.database_name, generateInnerTableName(table_id_.table_name));
        }
        else
        {
            if (query.storage->engine->name != "MergeTree")
                throw Exception(
                    "The ENGINE of WindowView must be MergeTree family of table engines including the engines with replication support",
                    ErrorCodes::INCORRECT_QUERY);

            auto manual_create_query
                = generateInnerTableCreateQuery(query, table_id_.database_name, generateInnerTableName(table_id_.table_name));
            InterpreterCreateQuery create_interpreter(manual_create_query, local_context);
            create_interpreter.setInternal(true);
            create_interpreter.execute();
            inner_storage = global_context.getTable(manual_create_query->database, manual_create_query->table);
            inner_table_id = inner_storage->getStorageID();
        }
        fetch_column_query = generateFetchColumnsQuery(inner_table_id);
    }

    {
        // generate write expressions
        ColumnsWithTypeAndName columns__;
        columns__.emplace_back(
            nullptr,
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeDateTime>()}),
            window_column_name);
        columns__.emplace_back(nullptr, std::make_shared<DataTypeDateTime>(), "____timestamp");
        columns__.emplace_back(nullptr, std::make_shared<DataTypeDateTime>(), "____watermark");
        const auto & function_tuple = FunctionFactory::instance().get("tupleElement", global_context);
        writeExpressions = std::make_shared<ExpressionActions>(columns__, global_context);
        writeExpressions->add(ExpressionAction::addColumn(
            {std::make_shared<DataTypeUInt8>()->createColumnConst(1, toField(2)), std::make_shared<DataTypeUInt8>(), "____tuple_arg"}));
        writeExpressions->add(ExpressionAction::applyFunction(function_tuple, Names{window_column_name, "____tuple_arg"}, "____w_end"));
        writeExpressions->add(ExpressionAction::removeColumn("____tuple_arg"));

        const auto & function_greater = FunctionFactory::instance().get("greaterOrEquals", global_context);
        writeExpressions->add(ExpressionAction::applyFunction(function_greater, Names{"____w_end", "____watermark"}, "____filter"));
        writeExpressions->add(ExpressionAction::removeColumn("____watermark"));
    }

    cleanCacheTask = global_context.getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncCleanCache(); });
    fireTask = global_context.getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncFire(); });
    cleanCacheTask->deactivate();
    fireTask->deactivate();
}


ASTPtr StorageWindowView::innerQueryParser(ASTSelectQuery & query)
{
    if (!query.groupBy())
        throw Exception("GROUP BY query is required for " + getName(), ErrorCodes::INCORRECT_QUERY);

    // parse stage mergeable
    ASTPtr result = query.clone();
    ASTPtr expr_list = result;
    ParserStageMergeableOneMatcher::Data stageMergeableOneData;
    ParserStageMergeableOneMatcher::Visitor(stageMergeableOneData).visit(expr_list);
    if (!stageMergeableOneData.is_tumble && !stageMergeableOneData.is_hop)
        throw Exception("WINDOW FUNCTION is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);
    window_column_name = stageMergeableOneData.window_column_name;

    // parser window function
    ASTFunction & window_function = typeid_cast<ASTFunction &>(*stageMergeableOneData.window_function);
    const auto & arguments = window_function.arguments->children;
    const auto & arg1 = std::static_pointer_cast<ASTFunction>(arguments.at(1));
    if (!arg1 || !startsWith(arg1->name, "toInterval"))
        throw Exception("Illegal type of last argument of function " + arg1->name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);

    String interval_str = arg1->name.substr(10);
    window_kind = strToIntervalKind(interval_str);
    const auto & interval_units_p1 = std::static_pointer_cast<ASTLiteral>(arg1->children.front()->children.front());

    window_num_units = stoi(interval_units_p1->value.get<String>());
    return result;
}

void StorageWindowView::writeIntoWindowView(StorageWindowView & window_view, const Block & block, const Context & context)
{
    UInt32 timestamp_now;
    UInt32 watermark;

    Pipe pipe(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), Chunk(block.getColumns(), block.rows())));

    std::shared_lock<std::shared_mutex> fire_signal_lock;
    if (window_view.is_proctime_tumble)
    {
        fire_signal_lock = std::shared_lock<std::shared_mutex>(window_view.fire_signal_mutex);
        timestamp_now = std::time(nullptr);
        watermark = window_view.getWindowLowerBound(timestamp_now);
        pipe.addSimpleTransform(std::make_shared<AddingConstColumnTransform<UInt32>>(
            pipe.getHeader(), std::make_shared<DataTypeDateTime>(), timestamp_now, "____timestamp"));
    }
    else
    {
        timestamp_now = std::time(nullptr);
        watermark = window_view.getWatermark(timestamp_now);
    }

    InterpreterSelectQuery select_block(window_view.getFinalQuery(), context, {std::move(pipe)}, QueryProcessingStage::WithMergeableState);

    BlockInputStreamPtr source_stream = std::make_shared<AddingConstColumnBlockInputStream<UInt32>>(select_block.execute().in, std::make_shared<DataTypeDateTime>(), watermark, "____watermark");

    source_stream = std::make_shared<FilterBlockInputStream>(source_stream, window_view.writeExpressions, "____filter", true);
    source_stream = std::make_shared<SquashingBlockInputStream>(
        source_stream, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);

    if (window_view.watermark_num_units != 0)
        source_stream = std::make_shared<WatermarkBlockInputStream>(source_stream, window_view, timestamp_now);

    if (!window_view.inner_table_id.empty())
    {
        auto & inner_storage = window_view.getInnerStorage();
        auto lock_ = inner_storage->lockStructureForShare(true, context.getCurrentQueryId());
        auto stream = inner_storage->write(window_view.getInnerQuery(), context);
        copyData(*source_stream, *stream);
    }
    else
    {
        BlocksListPtr new_mergeable_blocks = std::make_shared<BlocksList>();
        while (Block block_ = source_stream->read())
            new_mergeable_blocks->push_back(std::move(block_));

        std::unique_lock lock(window_view.mutex);
        window_view.getMergeableBlocksList()->push_back(new_mergeable_blocks);
    }
}

void StorageWindowView::startup()
{
    // Start the working thread
    cleanCacheTask->activateAndSchedule();
    fireTask->activateAndSchedule();
}

void StorageWindowView::shutdown()
{
    bool expected = false;
    if (!shutdown_called.compare_exchange_strong(expected, true))
        return;
    cleanCacheTask->deactivate();
    fireTask->deactivate();
}

StorageWindowView::~StorageWindowView()
{
    shutdown();
}

BlockInputStreamPtr StorageWindowView::getNewBlocksInputStreamPtr(UInt32 timestamp_)
{
    Pipes pipes;

    if (!inner_table_id.empty())
    {
        auto & storage = getInnerStorage();
        InterpreterSelectQuery fetch(fetch_column_query, global_context, storage, SelectQueryOptions(QueryProcessingStage::FetchColumns));
        QueryPipeline pipeline;
        BlockInputStreams streams = fetch.executeWithMultipleStreams(pipeline);
        for (auto & stream : streams)
            pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::move(stream)));
    }
    else
    {
        std::unique_lock lock(mutex);
        if (mergeable_blocks->empty())
            return std::make_shared<NullBlockInputStream>(getHeader());
        pipes = blocksToPipes(mergeable_blocks, getMergeableHeader());
    }

    ColumnsWithTypeAndName columns_;
    columns_.emplace_back(nullptr, std::make_shared<DataTypeDateTime>(), "____w_end");

    ExpressionActionsPtr actions_ = std::make_shared<ExpressionActions>(columns_, global_context);
    actions_->add(ExpressionAction::addColumn({std::make_shared<DataTypeDateTime>()->createColumnConst(1, toField(timestamp_)),
                                                std::make_shared<DataTypeDateTime>(),
                                                "____timestamp_now"}));
    const auto & function_equals = FunctionFactory::instance().get("equals", global_context);
    ExpressionActionsPtr apply_function_actions = std::make_shared<ExpressionActions>(columns_, global_context);
    actions_->add(ExpressionAction::applyFunction(function_equals, Names{"____w_end", "____timestamp_now"}, "____filter"));
    actions_->add(ExpressionAction::removeColumn("____w_end"));
    actions_->add(ExpressionAction::removeColumn("____timestamp_now"));

    for (auto & pipe : pipes)
        pipe.addSimpleTransform(std::make_shared<FilterTransform>(pipe.getHeader(), actions_,
        "____filter", true));

    auto proxy_storage = std::make_shared<WindowViewProxyStorage>(
        StorageID("", "WindowViewProxyStorage"), getParentStorage()->getColumns(), std::move(pipes), QueryProcessingStage::WithMergeableState);

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
