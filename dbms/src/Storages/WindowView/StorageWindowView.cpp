#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/BlocksBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionsWindow.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/StorageFactory.h>
#include <Common/typeid_cast.h>

#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/WindowView/WindowViewBlockInputStream.h>
#include <Storages/WindowView/WindowViewBlocksBlockInputStream.h>
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
            // String window_column_name_or_alias;
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
                    data.window_column_name = arrayJoin->getColumnName();
                    node_ptr = arrayJoin;
                }
                else if (serializeAST(node) != serializeAST(*data.window_function))
                    throw Exception("WINDOW VIEW only support ONE WINDOW FUNCTION", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
            }
        }
    };
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
    Dependencies dependencies = global_context.getDependencies(database_name, table_name);
    if (!dependencies.empty())
    {
        DatabaseAndTableName database_and_table_name = dependencies.front();
        throw Exception(
            "Table has dependency " + database_and_table_name.first + "." + database_and_table_name.second,
            ErrorCodes::TABLE_WAS_NOT_DROPPED);
    }
}

void StorageWindowView::drop(TableStructureWriteLockHolder &)
{
    global_context.removeDependency(
        DatabaseAndTableName(select_database_name, select_table_name), DatabaseAndTableName(database_name, table_name));

    std::lock_guard lock(mutex);
    is_dropped = true;
    condition.notify_all();
}

inline void StorageWindowView::flushToTable()
{
    //write into dependent table
    StoragePtr target_table = getTargetTable();
    auto _blockInputStreamPtr = getNewBlocksInputStreamPtr();
    auto _lock = target_table->lockStructureForShare(true, global_context.getCurrentQueryId());
    auto stream = target_table->write(getInnerQuery(), global_context);
    copyData(*_blockInputStreamPtr, *stream);
}

UInt32 StorageWindowView::getWindowUpperBound(UInt32 time_sec)
{
    switch (window_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        UInt32 start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, time_zone); \
        return AddTime<IntervalKind::KIND>::execute(start, window_num_units, time_zone); \
    }
        CASE_WINDOW_KIND(Second);
        CASE_WINDOW_KIND(Minute);
        CASE_WINDOW_KIND(Hour);
        CASE_WINDOW_KIND(Day);
        CASE_WINDOW_KIND(Week);
        CASE_WINDOW_KIND(Month);
        CASE_WINDOW_KIND(Quarter);
        CASE_WINDOW_KIND(Year);
#undef CASE_WINDOW_KIND
    }
    __builtin_unreachable();
}

void StorageWindowView::threadFuncToTable()
{
    while (!shutdown_called && has_target_table)
    {
        std::unique_lock lock(flushTableMutex);
        UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
        UInt64 w_end = static_cast<UInt64>(getWindowUpperBound(static_cast<UInt32>(timestamp_usec / 1000000))) * 1000000;
        condition.wait_for(lock, std::chrono::microseconds(w_end - timestamp_usec));
        try
        {
            if (refreshBlockStatus())
                flushToTable();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            break;
        }
    }
    if (!shutdown_called)
        toTableTask->scheduleAfter(RESCHEDULE_MS);
}

BlockInputStreams StorageWindowView::watch(
    const Names & /*column_names*/,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    if (has_target_table)
        throw Exception("WATCH query is disabled for " + getName() + " when constructed with 'TO' clause.", ErrorCodes::INCORRECT_QUERY);

    if (active_ptr.use_count() > 1)
        throw Exception("WATCH query is already attached, WINDOW VIEW only supports attaching one watch query.", ErrorCodes::INCORRECT_QUERY);

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
        active_ptr,
        has_limit,
        limit,
        context.getSettingsRef().temporary_window_view_timeout.totalSeconds());

    {
        std::lock_guard no_users_thread_lock(no_users_thread_mutex);
        if (no_users_thread.joinable())
        {
            std::lock_guard lock(no_users_thread_wakeup_mutex);
            no_users_thread_wakeup = true;
            no_users_thread_condition.notify_one();
        }
    }

    processed_stage = QueryProcessingStage::Complete;

    return {reader};
}

Block StorageWindowView::getHeader() const
{
    if (!sample_block)
    {
        auto storage = global_context.getTable(select_database_name, select_table_name);
        sample_block = InterpreterSelectQuery(
                           getInnerQuery(), global_context, storage, SelectQueryOptions(QueryProcessingStage::Complete))
                           .getSampleBlock();
        for (size_t i = 0; i < sample_block.columns(); ++i)
            sample_block.safeGetByPosition(i).column = sample_block.safeGetByPosition(i).column->convertToFullColumnIfConst();
    }

    return sample_block;
}

StoragePtr & StorageWindowView::getParentStorage()
{
    if (!parent_storage)
        parent_storage = global_context.getTable(getSelectDatabaseName(), getSelectTableName());
    return parent_storage;
}

StorageWindowView::StorageWindowView(
    const String & table_name_,
    const String & database_name_,
    Context & local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_)
    : table_name(table_name_)
    , database_name(database_name_)
    , global_context(local_context.getGlobalContext())
    , time_zone(DateLUT::instance())
    , log(&Poco::Logger::get("StorageWindowView"))
{
    setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    /// Default value, if only table name exist in the query
    select_database_name = local_context.getCurrentDatabase();
    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for Window View", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);

    auto inner_query_ = query.select->list_of_selects->children.at(0);

    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*inner_query_);
    extractDependentTable(select_query, select_database_name, select_table_name);
    inner_query = innerQueryParser(select_query);

    /// If the table is not specified - use the table `system.one`
    if (select_table_name.empty())
    {
        select_database_name = "system";
        select_table_name = "one";
    }

    global_context.addDependency(
        DatabaseAndTableName(select_database_name, select_table_name), DatabaseAndTableName(database_name, table_name));

    if (!query.to_table.empty())
    {
        has_target_table = true;
        target_database_name = query.to_database;
        target_table_name = query.to_table;
    }

    is_temporary = query.temporary;
    temporary_window_view_timeout = local_context.getSettingsRef().temporary_window_view_timeout.totalSeconds();

    mergeable_blocks = std::make_shared<std::list<BlocksListPtr>>();

    active_ptr = std::make_shared<bool>(true);

    toTableTask = global_context.getSchedulePool().createTask(log->name(), [this] { threadFuncToTable(); });
    toTableTask->deactivate();
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
    ASTExpressionList &window_function_args = typeid_cast<ASTExpressionList&>(*window_function.arguments);
    const auto & children = window_function_args.children;
    const auto & interval_p1 = std::static_pointer_cast<ASTFunction>(children.at(1));
    if (!interval_p1 || !startsWith(interval_p1->name, "toInterval"))
        throw Exception("Illegal type of last argument of function " + interval_p1->name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);

    String interval_str = interval_p1->name.substr(10);
    if (interval_str == "Second")
        window_kind = IntervalKind::Second;
    else if (interval_str == "Minute")
        window_kind = IntervalKind::Minute;
    else if (interval_str == "Hour")
        window_kind = IntervalKind::Hour;
    else if (interval_str == "Day")
        window_kind = IntervalKind::Day;
    else if (interval_str == "Week")
        window_kind = IntervalKind::Week;
    else if (interval_str == "Month")
        window_kind = IntervalKind::Month;
    else if (interval_str == "Quarter")
        window_kind = IntervalKind::Quarter;
    else if (interval_str == "Year")
        window_kind = IntervalKind::Year;

    const auto & interval_units_p1 = std::static_pointer_cast<ASTLiteral>(interval_p1->children.front()->children.front());

    window_num_units = stoi(interval_units_p1->value.get<String>());
    return result;
}

void StorageWindowView::writeIntoWindowView(StorageWindowView & window_view, const Block & block, const Context & context)
{
    BlockInputStreams streams = {std::make_shared<OneBlockInputStream>(block)};
    auto window_proxy_storage = std::make_shared<WindowViewProxyStorage>(
        window_view.getParentStorage(), std::move(streams), QueryProcessingStage::FetchColumns);
    InterpreterSelectQuery select_block(
        window_view.getInnerQuery(), context, window_proxy_storage, QueryProcessingStage::WithMergeableState);

    auto data_mergeable_stream = std::make_shared<MaterializingBlockInputStream>(select_block.execute().in);

    BlocksListPtr new_mergeable_blocks = std::make_shared<BlocksList>();
    while (Block block_ = data_mergeable_stream->read())
    {
        const ColumnTuple * column_tuple = checkAndGetColumn<ColumnTuple>(block_.getByName(window_view.window_column_name).column.get());
        block_.insert(
            {ColumnUInt8::create(block_.rows(), WINDOW_VIEW_FIRE_STATUS::WAITING), std::make_shared<DataTypeUInt8>(), "____fire_status"});
        block_.insert({column_tuple->getColumnPtr(1), std::make_shared<DataTypeDateTime>(), "____w_end"});
        new_mergeable_blocks->push_back(block_);
    }

    if (new_mergeable_blocks->empty())
        return;

    {
        std::unique_lock lock(window_view.mutex);
        window_view.getMergeableBlocksList()->push_back(new_mergeable_blocks);
    }
    window_view.condition.notify_all();
}

StoragePtr StorageWindowView::getTargetTable() const
{
    return global_context.getTable(target_database_name, target_table_name);
}

StoragePtr StorageWindowView::tryGetTargetTable() const
{
    return global_context.tryGetTable(target_database_name, target_table_name);
}

void StorageWindowView::startup()
{
    // Start the working thread
    if (has_target_table)
        toTableTask->activateAndSchedule();
    startNoUsersThread(temporary_window_view_timeout);
}

void StorageWindowView::shutdown()
{
    bool expected = false;
    if (!shutdown_called.compare_exchange_strong(expected, true))
        return;
    toTableTask->deactivate();
    {
        std::lock_guard no_users_thread_lock(no_users_thread_mutex);
        if (no_users_thread.joinable())
        {
            {
                std::lock_guard lock(no_users_thread_wakeup_mutex);
                no_users_thread_wakeup = true;
                no_users_thread_condition.notify_one();
            }
        }
    }
}

StorageWindowView::~StorageWindowView()
{
    shutdown();

    {
        std::lock_guard lock(no_users_thread_mutex);
        if (no_users_thread.joinable())
            no_users_thread.detach();
    }
}

bool StorageWindowView::refreshBlockStatus()
{
    UInt32 timestamp_now = std::time(nullptr);
    for (BlocksListPtr mergeable_block : *mergeable_blocks)
    {
        for (Block & block : *mergeable_block)
        {
            auto & col_wend = block.getByName("____w_end").column;
            const auto & wend_data = static_cast<const ColumnUInt32 &>(*col_wend).getData();
            auto & col_status = block.getByName("____fire_status").column;
            const auto & col_status_data = static_cast<const ColumnUInt8 &>(*col_status).getData();

            for (size_t i = 0; i < col_wend->size(); ++i)
            {
                if (wend_data[i] < timestamp_now && col_status_data[i] == WINDOW_VIEW_FIRE_STATUS::WAITING)
                    return true;
            }
        }
    }
    return false;
}

BlockInputStreamPtr StorageWindowView::getNewBlocksInputStreamPtr()
{
    if (mergeable_blocks->empty())
        return {std::make_shared<NullBlockInputStream>(getHeader())};
    {
        std::lock_guard lock(mutex);
        //delete fired blocks
        for (BlocksListPtr mergeable_block : *mergeable_blocks)
        {
            mergeable_block->remove_if([](Block & block_)
            {
                auto & column_ = block_.getByName("____fire_status").column;
                const auto & data = static_cast<const ColumnUInt8 &>(*column_).getData();
                for (size_t i = 0; i < column_->size(); ++i)
                {
                    if (data[i] != WINDOW_VIEW_FIRE_STATUS::RETIRED)
                        return false;
                }
                return true;
            });
        }
        mergeable_blocks->remove_if([](BlocksListPtr & ptr) { return ptr->size() == 0; });

        if (mergeable_blocks->empty())
            return {std::make_shared<NullBlockInputStream>(getHeader())};

        // mark blocks can be fired
        UInt32 timestamp_now = std::time(nullptr);
        for (BlocksListPtr mergeable_block : *mergeable_blocks)
        {
            for (Block & block : *mergeable_block)
            {
                auto & col_wend = block.getByName("____w_end").column;
                const auto & wend_data = static_cast<const ColumnUInt32 &>(*col_wend).getData();
                auto & col_status = block.getByName("____fire_status").column;
                auto col_status_mutable = col_status->assumeMutable();
                auto & col_status_data = static_cast<ColumnUInt8 &>(*col_status_mutable).getData();

                for (size_t i = 0; i < col_wend->size(); ++i)
                {
                    if (wend_data[i] < timestamp_now && col_status_data[i] == WINDOW_VIEW_FIRE_STATUS::WAITING)
                        col_status_data[i] = WINDOW_VIEW_FIRE_STATUS::READY;
                }
                col_status = std::move(col_status_mutable);
            }
        }
    }

    BlockInputStreams from;
    auto sample_block_ = mergeable_blocks->front()->front().cloneEmpty(); //TODO: 改为全局
    BlockInputStreamPtr stream = std::make_shared<WindowViewBlocksBlockInputStream>(mergeable_blocks, sample_block_, mutex);
    from.push_back(std::move(stream));
    auto proxy_storage = std::make_shared<WindowViewProxyStorage>(
        getParentStorage(), std::move(from), QueryProcessingStage::WithMergeableState);
    InterpreterSelectQuery select(getInnerQuery(), global_context, proxy_storage, QueryProcessingStage::Complete);
    BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
    return data;
}

BlocksPtr StorageWindowView::getNewBlocks()
{
    auto res = getNewBlocksInputStreamPtr();
    BlocksPtr blocks = std::make_shared<Blocks>();
    while (Block this_block = res->read())
        blocks->push_back(this_block);
    return blocks;
}

void StorageWindowView::noUsersThread(std::shared_ptr<StorageWindowView> storage, const UInt64 & timeout)
{
    bool drop_table = false;

    if (storage->shutdown_called)
        return;

    {
        while (1)
        {
            std::unique_lock lock(storage->no_users_thread_wakeup_mutex);
            if (!storage->no_users_thread_condition.wait_for(lock, std::chrono::seconds(timeout), [&] { return storage->no_users_thread_wakeup; }))
            {
                storage->no_users_thread_wakeup = false;
                if (storage->shutdown_called)
                    return;
                if (!storage->global_context.getDependencies(storage->database_name, storage->table_name).empty())
                    continue;
                drop_table = true;
            }
            break;
        }
    }

    if (drop_table)
    {
        if (storage->global_context.tryGetTable(storage->database_name, storage->table_name))
        {
            try
            {
                /// We create and execute `drop` query for this table
                auto drop_query = std::make_shared<ASTDropQuery>();
                drop_query->database = storage->database_name;
                drop_query->table = storage->table_name;
                drop_query->kind = ASTDropQuery::Kind::Drop;
                ASTPtr ast_drop_query = drop_query;
                InterpreterDropQuery drop_interpreter(ast_drop_query, storage->global_context);
                drop_interpreter.execute();
            }
            catch (...)
            {
            }
        }
    }
}

void StorageWindowView::startNoUsersThread(const UInt64 & timeout)
{
    bool expected = false;
    if (!start_no_users_thread_called.compare_exchange_strong(expected, true))
        return;

    if (is_temporary)
    {
        std::lock_guard no_users_thread_lock(no_users_thread_mutex);

        if (shutdown_called)
            return;

        if (no_users_thread.joinable())
        {
            {
                std::lock_guard lock(no_users_thread_wakeup_mutex);
                no_users_thread_wakeup = true;
                no_users_thread_condition.notify_one();
            }
            no_users_thread.join();
        }
        {
            std::lock_guard lock(no_users_thread_wakeup_mutex);
            no_users_thread_wakeup = false;
        }
        if (!is_dropped)
            no_users_thread = std::thread(&StorageWindowView::noUsersThread,
                std::static_pointer_cast<StorageWindowView>(shared_from_this()), timeout);
    }

    start_no_users_thread_called = false;
}


void registerStorageWindowView(StorageFactory & factory)
{
    factory.registerStorage("WindowView", [](const StorageFactory::Arguments & args)
    {
        if (!args.attach && !args.local_context.getSettingsRef().allow_experimental_window_view)
            throw Exception(
                "Experimental WINDOW VIEW feature is not enabled (the setting 'allow_experimental_window_view')",
                ErrorCodes::SUPPORT_IS_DISABLED);
        return StorageWindowView::create(args.table_name, args.database_name, args.local_context, args.query, args.columns);
    });
}
}
