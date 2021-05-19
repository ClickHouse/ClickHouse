#include <DataStreams/narrowBlockInputStreams.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/AlterCommands.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Databases/IDatabase.h>
#include <ext/range.h>
#include <algorithm>
#include <Parsers/queryToString.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Transforms/ExpressionTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_PREWHERE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

namespace
{

TreeRewriterResult modifySelect(ASTSelectQuery & select, const TreeRewriterResult & rewriter_result, ContextPtr context)
{

    TreeRewriterResult new_rewriter_result = rewriter_result;
    if (removeJoin(select))
    {
        /// Also remove GROUP BY cause ExpressionAnalyzer would check if it has all aggregate columns but joined columns would be missed.
        select.setExpression(ASTSelectQuery::Expression::GROUP_BY, {});
        new_rewriter_result.aggregates.clear();

        /// Replace select list to remove joined columns
        auto select_list = std::make_shared<ASTExpressionList>();
        for (const auto & column : rewriter_result.required_source_columns)
            select_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

        select.setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        const DB::IdentifierMembershipCollector membership_collector{select, context};

        /// Remove unknown identifiers from where, leave only ones from left table
        auto replace_where = [&membership_collector](ASTSelectQuery & query, ASTSelectQuery::Expression expr)
        {
            auto where = query.getExpression(expr, false);
            if (!where)
                return;

            const size_t left_table_pos = 0;
            /// Test each argument of `and` function and select ones related to only left table
            std::shared_ptr<ASTFunction> new_conj = makeASTFunction("and");
            for (const auto & node : collectConjunctions(where))
            {
                if (membership_collector.getIdentsMembership(node) == left_table_pos)
                    new_conj->arguments->children.push_back(std::move(node));
            }

            if (new_conj->arguments->children.empty())
                /// No identifiers from left table
                query.setExpression(expr, {});
            else if (new_conj->arguments->children.size() == 1)
                /// Only one expression, lift from `and`
                query.setExpression(expr, std::move(new_conj->arguments->children[0]));
            else
                /// Set new expression
                query.setExpression(expr, std::move(new_conj));
        };
        replace_where(select,ASTSelectQuery::Expression::WHERE);
        replace_where(select,ASTSelectQuery::Expression::PREWHERE);
        select.setExpression(ASTSelectQuery::Expression::HAVING, {});
        select.setExpression(ASTSelectQuery::Expression::ORDER_BY, {});
    }
    return new_rewriter_result;
}

}

StorageMerge::StorageMerge(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    const String & source_database_,
    const Strings & source_tables_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , source_database(source_database_)
    , source_tables(std::in_place, source_tables_.begin(), source_tables_.end())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

StorageMerge::StorageMerge(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    const String & source_database_,
    const String & source_table_regexp_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , source_database(source_database_)
    , source_table_regexp(source_table_regexp_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

template <typename F>
StoragePtr StorageMerge::getFirstTable(F && predicate) const
{
    auto iterator = getDatabaseIterator(getContext());

    while (iterator->isValid())
    {
        const auto & table = iterator->table();
        if (table.get() != this && predicate(table))
            return table;

        iterator->next();
    }

    return {};
}


bool StorageMerge::isRemote() const
{
    auto first_remote_table = getFirstTable([](const StoragePtr & table) { return table && table->isRemote(); });
    return first_remote_table != nullptr;
}


bool StorageMerge::mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & /*metadata_snapshot*/) const
{
    /// It's beneficial if it is true for at least one table.
    StorageListWithLocks selected_tables = getSelectedTables(query_context);

    size_t i = 0;
    for (const auto & table : selected_tables)
    {
        const auto & storage_ptr = std::get<0>(table);
        auto metadata_snapshot = storage_ptr->getInMemoryMetadataPtr();
        if (storage_ptr->mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot))
            return true;

        ++i;
        /// For simplicity reasons, check only first ten tables.
        if (i > 10)
            break;
    }

    return false;
}


QueryProcessingStage::Enum StorageMerge::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageMetadataPtr &,
    SelectQueryInfo & query_info) const
{
    /// In case of JOIN the first stage (which includes JOIN)
    /// should be done on the initiator always.
    ///
    /// Since in case of JOIN query on shards will receive query w/o JOIN (and their columns).
    /// (see modifySelect()/removeJoin())
    ///
    /// And for this we need to return FetchColumns.
    if (const auto * select = query_info.query->as<ASTSelectQuery>(); select && hasJoin(*select))
        return QueryProcessingStage::FetchColumns;

    auto stage_in_source_tables = QueryProcessingStage::FetchColumns;

    DatabaseTablesIteratorPtr iterator = getDatabaseIterator(local_context);

    size_t selected_table_size = 0;

    while (iterator->isValid())
    {
        const auto & table = iterator->table();
        if (table && table.get() != this)
        {
            ++selected_table_size;
            stage_in_source_tables = std::max(
                stage_in_source_tables,
                table->getQueryProcessingStage(local_context, to_stage, table->getInMemoryMetadataPtr(), query_info));
        }

        iterator->next();
    }

    return selected_table_size == 1 ? stage_in_source_tables : std::min(stage_in_source_tables, QueryProcessingStage::WithMergeableState);
}


Pipe StorageMerge::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    unsigned num_streams)
{
    Pipes pipes;

    bool has_table_virtual_column = false;
    Names real_column_names;
    real_column_names.reserve(column_names.size());

    for (const auto & column_name : column_names)
    {
        if (column_name == "_table" && isVirtualColumn(column_name, metadata_snapshot))
            has_table_virtual_column = true;
        else
            real_column_names.push_back(column_name);
    }

    /** Just in case, turn off optimization "transfer to PREWHERE",
      * since there is no certainty that it works when one of table is MergeTree and other is not.
      */
    auto modified_context = Context::createCopy(local_context);
    modified_context->setSetting("optimize_move_to_prewhere", Field{false});

    /// What will be result structure depending on query processed stage in source tables?
    Block header = getHeaderForProcessingStage(*this, column_names, metadata_snapshot, query_info, local_context, processed_stage);

    /** First we make list of selected tables to find out its size.
      * This is necessary to correctly pass the recommended number of threads to each table.
      */
    StorageListWithLocks selected_tables = getSelectedTables(local_context, query_info.query, has_table_virtual_column);

    if (selected_tables.empty())
        /// FIXME: do we support sampling in this case?
        return createSources(
            {}, query_info, processed_stage, max_block_size, header, {}, real_column_names, modified_context, 0, has_table_virtual_column);

    size_t tables_count = selected_tables.size();
    Float64 num_streams_multiplier
        = std::min(unsigned(tables_count), std::max(1U, unsigned(local_context->getSettingsRef().max_streams_multiplier_for_merge_tables)));
    num_streams *= num_streams_multiplier;
    size_t remaining_streams = num_streams;

    InputOrderInfoPtr input_sorting_info;
    if (query_info.order_optimizer)
    {
        for (auto it = selected_tables.begin(); it != selected_tables.end(); ++it)
        {
            auto storage_ptr = std::get<0>(*it);
            auto storage_metadata_snapshot = storage_ptr->getInMemoryMetadataPtr();
            auto current_info = query_info.order_optimizer->getInputOrder(storage_metadata_snapshot, local_context);
            if (it == selected_tables.begin())
                input_sorting_info = current_info;
            else if (!current_info || (input_sorting_info && *current_info != *input_sorting_info))
                input_sorting_info.reset();

            if (!input_sorting_info)
                break;
        }

        query_info.input_order_info = input_sorting_info;
    }

    for (const auto & table : selected_tables)
    {
        size_t current_need_streams = tables_count >= num_streams ? 1 : (num_streams / tables_count);
        size_t current_streams = std::min(current_need_streams, remaining_streams);
        remaining_streams -= current_streams;
        current_streams = std::max(size_t(1), current_streams);

        const auto & storage = std::get<0>(table);

        /// If sampling requested, then check that table supports it.
        if (query_info.query->as<ASTSelectQuery>()->sampleSize() && !storage->supportsSampling())
            throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

        auto storage_metadata_snapshot = storage->getInMemoryMetadataPtr();

        auto source_pipe = createSources(
            storage_metadata_snapshot, query_info, processed_stage,
            max_block_size, header, table, real_column_names, modified_context,
            current_streams, has_table_virtual_column);

        pipes.emplace_back(std::move(source_pipe));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    if (!pipe.empty())
        // It's possible to have many tables read from merge, resize(num_streams) might open too many files at the same time.
        // Using narrowPipe instead.
        narrowPipe(pipe, num_streams);

    return pipe;
}

Pipe StorageMerge::createSources(
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const QueryProcessingStage::Enum & processed_stage,
    const UInt64 max_block_size,
    const Block & header,
    const StorageWithLockAndName & storage_with_lock,
    Names & real_column_names,
    ContextPtr modified_context,
    size_t streams_num,
    bool has_table_virtual_column,
    bool concat_streams)
{
    const auto & [storage, struct_lock, table_name] = storage_with_lock;
    SelectQueryInfo modified_query_info = query_info;
    modified_query_info.query = query_info.query->clone();

    /// Original query could contain JOIN but we need only the first joined table and its columns.
    auto & modified_select = modified_query_info.query->as<ASTSelectQuery &>();
    auto new_analyzer_res = modifySelect(modified_select, *query_info.syntax_analyzer_result, modified_context);
    modified_query_info.syntax_analyzer_result = std::make_shared<TreeRewriterResult>(std::move(new_analyzer_res));

    VirtualColumnUtils::rewriteEntityInAst(modified_query_info.query, "_table", table_name);

    Pipe pipe;

    if (!storage)
    {
        pipe = QueryPipeline::getPipe(InterpreterSelectQuery(
            modified_query_info.query, modified_context,
            std::make_shared<OneBlockInputStream>(header),
            SelectQueryOptions(processed_stage).analyze()).execute().pipeline);

        pipe.addInterpreterContext(modified_context);
        return pipe;
    }

    auto storage_stage
        = storage->getQueryProcessingStage(modified_context, QueryProcessingStage::Complete, metadata_snapshot, modified_query_info);
    if (processed_stage <= storage_stage)
    {
        /// If there are only virtual columns in query, you must request at least one other column.
        if (real_column_names.empty())
            real_column_names.push_back(ExpressionActions::getSmallestColumn(metadata_snapshot->getColumns().getAllPhysical()));

        pipe = storage->read(
            real_column_names,
            metadata_snapshot,
            modified_query_info,
            modified_context,
            processed_stage,
            max_block_size,
            UInt32(streams_num));
    }
    else if (processed_stage > storage_stage)
    {
        modified_select.replaceDatabaseAndTable(source_database, table_name);

        /// Maximum permissible parallelism is streams_num
        modified_context->setSetting("max_threads", streams_num);
        modified_context->setSetting("max_streams_to_max_threads_ratio", 1);

        InterpreterSelectQuery interpreter{modified_query_info.query, modified_context, SelectQueryOptions(processed_stage)};


        pipe = QueryPipeline::getPipe(interpreter.execute().pipeline);

        /** Materialization is needed, since from distributed storage the constants come materialized.
          * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
          * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
          */
        pipe.addSimpleTransform([](const Block & stream_header) { return std::make_shared<MaterializingTransform>(stream_header); });
    }

    if (!pipe.empty())
    {
        if (concat_streams && pipe.numOutputPorts() > 1)
            // It's possible to have many tables read from merge, resize(1) might open too many files at the same time.
            // Using concat instead.
            pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));

        if (has_table_virtual_column)
        {
            ColumnWithTypeAndName column;
            column.name = "_table";
            column.type = std::make_shared<DataTypeString>();
            column.column = column.type->createColumnConst(0, Field(table_name));

            auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
            auto adding_column_actions = std::make_shared<ExpressionActions>(
                std::move(adding_column_dag),
                ExpressionActionsSettings::fromContext(modified_context, CompileExpressions::yes));

            pipe.addSimpleTransform([&](const Block & stream_header)
            {
                return std::make_shared<ExpressionTransform>(stream_header, adding_column_actions);
            });
        }

        /// Subordinary tables could have different but convertible types, like numeric types of different width.
        /// We must return streams with structure equals to structure of Merge table.
        convertingSourceStream(header, metadata_snapshot, modified_context, modified_query_info.query, pipe, processed_stage);

        pipe.addTableLock(struct_lock);
        pipe.addStorageHolder(storage);
        pipe.addInterpreterContext(modified_context);
    }

    return pipe;
}

StorageMerge::StorageListWithLocks StorageMerge::getSelectedTables(
        ContextPtr query_context,
        const ASTPtr & query /* = nullptr */,
        bool filter_by_virtual_column /* = false */) const
{
    assert(!filter_by_virtual_column || query);

    const Settings & settings = query_context->getSettingsRef();
    StorageListWithLocks selected_tables;
    DatabaseTablesIteratorPtr iterator = getDatabaseIterator(getContext());

    MutableColumnPtr table_name_virtual_column;
    if (filter_by_virtual_column)
        table_name_virtual_column = ColumnString::create();

    while (iterator->isValid())
    {
        StoragePtr storage = iterator->table();
        if (!storage)
            continue;

        if (query && query->as<ASTSelectQuery>()->prewhere() && !storage->supportsPrewhere())
            throw Exception("Storage " + storage->getName() + " doesn't support PREWHERE.", ErrorCodes::ILLEGAL_PREWHERE);

        if (storage.get() != this)
        {
            auto table_lock = storage->lockForShare(query_context->getCurrentQueryId(), settings.lock_acquire_timeout);
            selected_tables.emplace_back(storage, std::move(table_lock), iterator->name());
            if (filter_by_virtual_column)
                table_name_virtual_column->insert(iterator->name());
        }

        iterator->next();
    }

    if (filter_by_virtual_column)
    {
        /// Filter names of selected tables if there is a condition on "_table" virtual column in WHERE clause
        Block virtual_columns_block = Block{ColumnWithTypeAndName(std::move(table_name_virtual_column), std::make_shared<DataTypeString>(), "_table")};
        VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, query_context);
        auto values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_table");

        /// Remove unused tables from the list
        selected_tables.remove_if([&] (const auto & elem) { return values.find(std::get<2>(elem)) == values.end(); });
    }

    return selected_tables;
}

DatabaseTablesIteratorPtr StorageMerge::getDatabaseIterator(ContextPtr local_context) const
{
    try
    {
        checkStackSize();
    }
    catch (Exception & e)
    {
        e.addMessage("while getting table iterator of Merge table. Maybe caused by two Merge tables that will endlessly try to read each other's data");
        throw;
    }

    auto database = DatabaseCatalog::instance().getDatabase(source_database);

    auto table_name_match = [this](const String & table_name_) -> bool
    {
        if (source_tables)
            return source_tables->count(table_name_);
        else
            return source_table_regexp->match(table_name_);
    };

    return database->getTablesIterator(local_context, table_name_match);
}


void StorageMerge::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    auto name_deps = getDependentViewsByColumn(local_context);
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN)
            throw Exception(
                "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
        if (command.type == AlterCommand::Type::DROP_COLUMN && !command.clear)
        {
            const auto & deps_mv = name_deps[command.column_name];
            if (!deps_mv.empty())
            {
                throw Exception(
                    "Trying to ALTER DROP column " + backQuoteIfNeed(command.column_name) + " which is referenced by materialized view "
                        + toString(deps_mv),
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
        }
    }
}

void StorageMerge::alter(
    const AlterCommands & params, ContextPtr local_context, TableLockHolder &)
{
    auto table_id = getStorageID();

    StorageInMemoryMetadata storage_metadata = getInMemoryMetadata();
    params.apply(storage_metadata, local_context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, storage_metadata);
    setInMemoryMetadata(storage_metadata);
}

void StorageMerge::convertingSourceStream(
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    ASTPtr & query,
    Pipe & pipe,
    QueryProcessingStage::Enum processed_stage)
{
    Block before_block_header = pipe.getHeader();

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);
    auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes));

    pipe.addSimpleTransform([&](const Block & stream_header)
    {
        return std::make_shared<ExpressionTransform>(stream_header, convert_actions);
    });

    auto where_expression = query->as<ASTSelectQuery>()->where();

    if (!where_expression)
        return;

    for (size_t column_index : ext::range(0, header.columns()))
    {
        ColumnWithTypeAndName header_column = header.getByPosition(column_index);
        ColumnWithTypeAndName before_column = before_block_header.getByName(header_column.name);
        /// If the processed_stage greater than FetchColumns and the block structure between streams is different.
        /// the where expression maybe invalid because of convertingBlockInputStream.
        /// So we need to throw exception.
        if (!header_column.type->equals(*before_column.type.get()) && processed_stage > QueryProcessingStage::FetchColumns)
        {
            NamesAndTypesList source_columns = metadata_snapshot->getSampleBlock().getNamesAndTypesList();
            auto virtual_column = *getVirtuals().tryGetByName("_table");
            source_columns.emplace_back(NameAndTypePair{virtual_column.name, virtual_column.type});
            auto syntax_result = TreeRewriter(local_context).analyze(where_expression, source_columns);
            ExpressionActionsPtr actions = ExpressionAnalyzer{where_expression, syntax_result, local_context}.getActions(false, false);
            Names required_columns = actions->getRequiredColumns();

            for (const auto & required_column : required_columns)
            {
                if (required_column == header_column.name)
                    throw Exception("Block structure mismatch in Merge Storage: different types:\n" + before_block_header.dumpStructure()
                                    + "\n" + header.dumpStructure(), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
}

IStorage::ColumnSizeByName StorageMerge::getColumnSizes() const
{

    auto first_materialize_mysql = getFirstTable([](const StoragePtr & table) { return table && table->getName() == "MaterializeMySQL"; });
    if (!first_materialize_mysql)
        return {};
    return first_materialize_mysql->getColumnSizes();
}

void registerStorageMerge(StorageFactory & factory)
{
    factory.registerStorage("Merge", [](const StorageFactory::Arguments & args)
    {
        /** In query, the name of database is specified as table engine argument which contains source tables,
          *  as well as regex for source-table names.
          */

        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception("Storage Merge requires exactly 2 parameters"
                " - name of source database and regexp for table names.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionForDatabaseName(engine_args[0], args.getLocalContext());
        engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.getLocalContext());

        String source_database = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        String table_name_regexp = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageMerge::create(args.table_id, args.columns, args.comment, source_database, table_name_regexp, args.getContext());
    });
}

NamesAndTypesList StorageMerge::getVirtuals() const
{
    NamesAndTypesList virtuals{{"_table", std::make_shared<DataTypeString>()}};

    auto first_table = getFirstTable([](auto && table) { return table; });
    if (first_table)
    {
        auto table_virtuals = first_table->getVirtuals();
        virtuals.insert(virtuals.end(), table_virtuals.begin(), table_virtuals.end());
    }

    return virtuals;
}
}
