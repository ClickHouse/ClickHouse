#include <algorithm>
#include <functional>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnString.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Databases/IDatabase.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Planner/Utils.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/narrowPipe.h>
#include <Storages/AlterCommands.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageView.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <base/defines.h>
#include <base/range.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/checkStackSize.h>
#include <Common/typeid_cast.h>

namespace
{

using namespace DB;
bool columnIsPhysical(ColumnDefaultKind kind)
{
    return kind == ColumnDefaultKind::Default || kind == ColumnDefaultKind::Materialized;
}
bool columnDefaultKindHasSameType(ColumnDefaultKind lhs, ColumnDefaultKind rhs)
{
    if (lhs == rhs)
        return true;

    if (columnIsPhysical(lhs) == columnIsPhysical(rhs))
        return true;

    return false;
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int LOGICAL_ERROR;
}

StorageMerge::DatabaseNameOrRegexp::DatabaseNameOrRegexp(
    const String & source_database_name_or_regexp_,
    bool database_is_regexp_,
    std::optional<OptimizedRegularExpression> source_database_regexp_,
    std::optional<OptimizedRegularExpression> source_table_regexp_,
    std::optional<DBToTableSetMap> source_databases_and_tables_)
    : source_database_name_or_regexp(source_database_name_or_regexp_)
    , database_is_regexp(database_is_regexp_)
    , source_database_regexp(std::move(source_database_regexp_))
    , source_table_regexp(std::move(source_table_regexp_))
    , source_databases_and_tables(std::move(source_databases_and_tables_))
{
}

StorageMerge::StorageMerge(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    const String & source_database_name_or_regexp_,
    bool database_is_regexp_,
    const DBToTableSetMap & source_databases_and_tables_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , database_name_or_regexp(
        source_database_name_or_regexp_,
        database_is_regexp_,
        source_database_name_or_regexp_, {},
        source_databases_and_tables_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_.empty() ? getColumnsDescriptionFromSourceTables() : columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

StorageMerge::StorageMerge(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    const String & source_database_name_or_regexp_,
    bool database_is_regexp_,
    const String & source_table_regexp_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , database_name_or_regexp(
        source_database_name_or_regexp_,
        database_is_regexp_,
        source_database_name_or_regexp_,
        source_table_regexp_, {})
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_.empty() ? getColumnsDescriptionFromSourceTables() : columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

StorageMerge::DatabaseTablesIterators StorageMerge::getDatabaseIterators(ContextPtr context_) const
{
    return database_name_or_regexp.getDatabaseIterators(context_);
}

ColumnsDescription StorageMerge::getColumnsDescriptionFromSourceTables() const
{
    auto table = getFirstTable([](auto && t) { return t; });
    if (!table)
        throw Exception{ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "There are no tables satisfied provided regexp, you must specify table structure manually"};
    return table->getInMemoryMetadataPtr()->getColumns();
}

template <typename F>
StoragePtr StorageMerge::getFirstTable(F && predicate) const
{
    auto database_table_iterators = database_name_or_regexp.getDatabaseIterators(getContext());

    for (auto & iterator : database_table_iterators)
    {
        while (iterator->isValid())
        {
            const auto & table = iterator->table();
            if (table.get() != this && predicate(table))
                return table;

            iterator->next();
        }
    }

    return {};
}

template <typename F>
void StorageMerge::forEachTable(F && func) const
{
    getFirstTable([&func](const auto & table)
    {
        func(table);
        return false;
    });
}

bool StorageMerge::isRemote() const
{
    auto first_remote_table = getFirstTable([](const StoragePtr & table) { return table && table->isRemote(); });
    return first_remote_table != nullptr;
}

bool StorageMerge::tableSupportsPrewhere() const
{
    /// NOTE: This check is used during query analysis as condition for applying
    /// "move to PREWHERE" optimization. However, it contains a logical race:
    /// If new table that matches regexp for current storage and doesn't support PREWHERE
    /// will appear after this check and before calling "read" method, the optimized query may fail.
    /// Since it's quite rare case, we just ignore this possibility.
    ///
    /// NOTE: Type can be different, and in this case, PREWHERE cannot be
    /// applied for those columns, but there a separate method to return
    /// supported columns for PREWHERE - supportedPrewhereColumns().
    return getFirstTable([](const auto & table) { return !table->canMoveConditionsToPrewhere(); }) == nullptr;
}

bool StorageMerge::canMoveConditionsToPrewhere() const
{
    return tableSupportsPrewhere();
}

std::optional<NameSet> StorageMerge::supportedPrewhereColumns() const
{
    bool supports_prewhere = true;

    const auto & metadata = getInMemoryMetadata();
    const auto & columns = metadata.getColumns();

    NameSet supported_columns;

    std::unordered_map<std::string, std::pair<const IDataType *, ColumnDefaultKind>> column_info;
    for (const auto & name_type : columns.getAll())
    {
        const auto & column_default = columns.getDefault(name_type.name).value_or(ColumnDefault{});
        column_info.emplace(name_type.name, std::make_pair(
            name_type.type.get(),
            column_default.kind));
        supported_columns.emplace(name_type.name);
    }

    forEachTable([&](const StoragePtr & table)
    {
        const auto & table_metadata_ptr = table->getInMemoryMetadataPtr();
        if (!table_metadata_ptr)
            supports_prewhere = false;
        if (!supports_prewhere)
            return;

        const auto & table_columns = table_metadata_ptr->getColumns();
        for (const auto & column : table_columns.getAll())
        {
            const auto & column_default = table_columns.getDefault(column.name).value_or(ColumnDefault{});
            const auto & [root_type, src_default_kind] = column_info[column.name];
            if ((root_type && !root_type->equals(*column.type)) ||
                !columnDefaultKindHasSameType(src_default_kind, column_default.kind))
            {
                supported_columns.erase(column.name);
            }
        }
    });

    return supported_columns;
}

QueryProcessingStage::Enum StorageMerge::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr &,
    SelectQueryInfo & query_info) const
{
    /// In case of JOIN the first stage (which includes JOIN)
    /// should be done on the initiator always.
    ///
    /// Since in case of JOIN query on shards will receive query without JOIN (and their columns).
    /// (see removeJoin())
    ///
    /// And for this we need to return FetchColumns.
    if (const auto * select = query_info.query->as<ASTSelectQuery>(); select && hasJoin(*select))
        return QueryProcessingStage::FetchColumns;

    auto stage_in_source_tables = QueryProcessingStage::FetchColumns;

    DatabaseTablesIterators database_table_iterators = database_name_or_regexp.getDatabaseIterators(local_context);

    size_t selected_table_size = 0;

    for (const auto & iterator : database_table_iterators)
    {
        while (iterator->isValid())
        {
            const auto & table = iterator->table();
            if (table && table.get() != this)
            {
                ++selected_table_size;
                stage_in_source_tables = std::max(
                    stage_in_source_tables,
                    table->getQueryProcessingStage(local_context, to_stage,
                        table->getStorageSnapshot(table->getInMemoryMetadataPtr(), local_context), query_info));
            }

            iterator->next();
        }
    }

    return selected_table_size == 1 ? stage_in_source_tables : std::min(stage_in_source_tables, QueryProcessingStage::WithMergeableState);
}

void StorageMerge::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    size_t num_streams)
{
    /** Just in case, turn off optimization "transfer to PREWHERE",
      * since there is no certainty that it works when one of table is MergeTree and other is not.
      */
    auto modified_context = Context::createCopy(local_context);
    modified_context->setSetting("optimize_move_to_prewhere", false);
    query_plan.addInterpreterContext(modified_context);

    /// What will be result structure depending on query processed stage in source tables?
    Block common_header = getHeaderForProcessingStage(column_names, storage_snapshot, query_info, local_context, processed_stage);

    auto step = std::make_unique<ReadFromMerge>(
        common_header,
        column_names,
        max_block_size,
        num_streams,
        shared_from_this(),
        storage_snapshot,
        query_info,
        std::move(modified_context),
        processed_stage);

    query_plan.addStep(std::move(step));
}

ReadFromMerge::ReadFromMerge(
    Block common_header_,
    Names all_column_names_,
    size_t max_block_size,
    size_t num_streams,
    StoragePtr storage,
    StorageSnapshotPtr storage_snapshot,
    const SelectQueryInfo & query_info_,
    ContextMutablePtr context_,
    QueryProcessingStage::Enum processed_stage)
    : SourceStepWithFilter(DataStream{.header = common_header_})
    , required_max_block_size(max_block_size)
    , requested_num_streams(num_streams)
    , common_header(std::move(common_header_))
    , all_column_names(std::move(all_column_names_))
    , storage_merge(std::move(storage))
    , merge_storage_snapshot(std::move(storage_snapshot))
    , query_info(query_info_)
    , context(std::move(context_))
    , common_processed_stage(processed_stage)
{
}

void ReadFromMerge::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    filterTablesAndCreateChildrenPlans();

    if (selected_tables.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        return;
    }

    QueryPlanResourceHolder resources;
    std::vector<std::unique_ptr<QueryPipelineBuilder>> pipelines;

    auto table_it = selected_tables.begin();
    for (size_t i = 0; i < selected_tables.size(); ++i, ++table_it)
    {
        auto & child_plan = child_plans->at(i);
        const auto & table = *table_it;

        const auto storage = std::get<1>(table);
        const auto storage_metadata_snapshot = storage->getInMemoryMetadataPtr();
        const auto nested_storage_snaphsot = storage->getStorageSnapshot(storage_metadata_snapshot, context);

        auto modified_query_info = getModifiedQueryInfo(query_info, context, table, nested_storage_snaphsot);

        auto source_pipeline = createSources(
            child_plan.plan,
            nested_storage_snaphsot,
            modified_query_info,
            common_processed_stage,
            common_header,
            child_plan.table_aliases,
            child_plan.row_policy_data_opt,
            table,
            context);

        if (source_pipeline && source_pipeline->initialized())
        {
            resources.storage_holders.push_back(std::get<1>(table));
            resources.table_locks.push_back(std::get<2>(table));

            pipelines.emplace_back(std::move(source_pipeline));
        }
    }

    if (pipelines.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        return;
    }

    pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines));

    if (!query_info.input_order_info)
    {
        size_t tables_count = selected_tables.size();
        Float64 num_streams_multiplier = std::min(
            static_cast<size_t>(tables_count),
            std::max(1UL, static_cast<size_t>(context->getSettingsRef().max_streams_multiplier_for_merge_tables)));
        size_t num_streams = static_cast<size_t>(requested_num_streams * num_streams_multiplier);

        // It's possible to have many tables read from merge, resize(num_streams) might open too many files at the same time.
        // Using narrowPipe instead. But in case of reading in order of primary key, we cannot do it,
        // because narrowPipe doesn't preserve order.
        pipeline.narrow(num_streams);
    }

    pipeline.addResources(std::move(resources));
}

void ReadFromMerge::filterTablesAndCreateChildrenPlans()
{
    if (child_plans)
        return;

    has_database_virtual_column = false;
    has_table_virtual_column = false;
    column_names.clear();
    column_names.reserve(column_names.size());

    for (const auto & column_name : all_column_names)
    {
        if (column_name == "_database" && storage_merge->isVirtualColumn(column_name, merge_storage_snapshot->metadata))
            has_database_virtual_column = true;
        else if (column_name == "_table" && storage_merge->isVirtualColumn(column_name, merge_storage_snapshot->metadata))
            has_table_virtual_column = true;
        else
            column_names.push_back(column_name);
    }

    selected_tables = getSelectedTables(context, has_database_virtual_column, has_table_virtual_column);

    child_plans = createChildrenPlans(query_info);
}

std::vector<ReadFromMerge::ChildPlan> ReadFromMerge::createChildrenPlans(SelectQueryInfo & query_info_) const
{
    if (selected_tables.empty())
        return {};

    std::vector<ChildPlan> res;

    size_t tables_count = selected_tables.size();
    Float64 num_streams_multiplier
        = std::min(static_cast<size_t>(tables_count), std::max(1UL, static_cast<size_t>(context->getSettingsRef().max_streams_multiplier_for_merge_tables)));
    size_t num_streams = static_cast<size_t>(requested_num_streams * num_streams_multiplier);
    size_t remaining_streams = num_streams;

    if (order_info)
    {
        query_info_.input_order_info = order_info;
    }
    else if (query_info.order_optimizer)
    {
        InputOrderInfoPtr input_sorting_info;
        for (auto it = selected_tables.begin(); it != selected_tables.end(); ++it)
        {
            auto storage_ptr = std::get<1>(*it);
            auto storage_metadata_snapshot = storage_ptr->getInMemoryMetadataPtr();
            auto current_info = query_info.order_optimizer->getInputOrder(storage_metadata_snapshot, context);
            if (it == selected_tables.begin())
                input_sorting_info = current_info;
            else if (!current_info || (input_sorting_info && *current_info != *input_sorting_info))
                input_sorting_info.reset();

            if (!input_sorting_info)
                break;
        }

        query_info_.input_order_info = input_sorting_info;
    }

    for (const auto & table : selected_tables)
    {
        size_t current_need_streams = tables_count >= num_streams ? 1 : (num_streams / tables_count);
        size_t current_streams = std::min(current_need_streams, remaining_streams);
        remaining_streams -= current_streams;
        current_streams = std::max(1uz, current_streams);

        const auto & storage = std::get<1>(table);

        bool sampling_requested = query_info.query->as<ASTSelectQuery>()->sampleSize() != nullptr;
        if (query_info.table_expression_modifiers)
            sampling_requested = query_info.table_expression_modifiers->hasSampleSizeRatio();

        /// If sampling requested, then check that table supports it.
        if (sampling_requested && !storage->supportsSampling())
            throw Exception(ErrorCodes::SAMPLING_NOT_SUPPORTED, "Illegal SAMPLE: table {} doesn't support sampling", storage->getStorageID().getNameForLogs());

        res.emplace_back();

        auto & aliases = res.back().table_aliases;
        auto & row_policy_data_opt = res.back().row_policy_data_opt;
        auto storage_metadata_snapshot = storage->getInMemoryMetadataPtr();
        auto nested_storage_snaphsot = storage->getStorageSnapshot(storage_metadata_snapshot, context);

        auto modified_query_info = getModifiedQueryInfo(query_info, context, table, nested_storage_snaphsot);
        Names column_names_as_aliases;
        Names real_column_names = column_names;

        const auto & database_name = std::get<0>(table);
        const auto & table_name = std::get<3>(table);
        auto row_policy_filter_ptr = context->getRowPolicyFilter(
            database_name,
            table_name,
            RowPolicyFilterType::SELECT_FILTER);
        if (row_policy_filter_ptr)
        {
            row_policy_data_opt = RowPolicyData(row_policy_filter_ptr, storage, context);
            row_policy_data_opt->extendNames(real_column_names);
        }

        if (!context->getSettingsRef().allow_experimental_analyzer)
        {
            auto storage_columns = storage_metadata_snapshot->getColumns();
            auto syntax_result = TreeRewriter(context).analyzeSelect(
                modified_query_info.query, TreeRewriterResult({}, storage, nested_storage_snaphsot));

            bool with_aliases = common_processed_stage == QueryProcessingStage::FetchColumns && !storage_columns.getAliases().empty();
            if (with_aliases)
            {
                ASTPtr required_columns_expr_list = std::make_shared<ASTExpressionList>();
                ASTPtr column_expr;

                auto sample_block = merge_storage_snapshot->getMetadataForQuery()->getSampleBlock();

                for (const auto & column : real_column_names)
                {
                    const auto column_default = storage_columns.getDefault(column);
                    bool is_alias = column_default && column_default->kind == ColumnDefaultKind::Alias;

                    if (is_alias)
                    {
                        column_expr = column_default->expression->clone();
                        replaceAliasColumnsInQuery(column_expr, storage_metadata_snapshot->getColumns(),
                                                syntax_result->array_join_result_to_source, context);

                        const auto & column_description = storage_columns.get(column);
                        column_expr = addTypeConversionToAST(std::move(column_expr), column_description.type->getName(),
                                                            storage_metadata_snapshot->getColumns().getAll(), context);
                        column_expr = setAlias(column_expr, column);

                        /// use storage type for transient columns that are not represented in result
                        ///  e.g. for columns that needed to evaluate row policy
                        auto type = sample_block.has(column) ? sample_block.getByName(column).type : column_description.type;

                        aliases.push_back({ .name = column, .type = type, .expression = column_expr->clone() });
                    }
                    else
                        column_expr = std::make_shared<ASTIdentifier>(column);

                    required_columns_expr_list->children.emplace_back(std::move(column_expr));
                }

                syntax_result = TreeRewriter(context).analyze(
                    required_columns_expr_list, storage_columns.getAllPhysical(), storage, storage->getStorageSnapshot(storage_metadata_snapshot, context));

                auto alias_actions = ExpressionAnalyzer(required_columns_expr_list, syntax_result, context).getActionsDAG(true);

                column_names_as_aliases = alias_actions->getRequiredColumns().getNames();
                if (column_names_as_aliases.empty())
                    column_names_as_aliases.push_back(ExpressionActions::getSmallestColumn(storage_metadata_snapshot->getColumns().getAllPhysical()).name);
            }
        }

        res.back().plan = createPlanForTable(
            nested_storage_snaphsot,
            modified_query_info,
            common_processed_stage,
            required_max_block_size,
            table,
            column_names_as_aliases.empty() ? std::move(real_column_names) : std::move(column_names_as_aliases),
            row_policy_data_opt,
            context,
            current_streams);
    }

    return res;
}

SelectQueryInfo ReadFromMerge::getModifiedQueryInfo(const SelectQueryInfo & query_info,
    const ContextPtr & modified_context,
    const StorageWithLockAndName & storage_with_lock_and_name,
    const StorageSnapshotPtr & storage_snapshot)
{
    const auto & [database_name, storage, storage_lock, table_name] = storage_with_lock_and_name;
    const StorageID current_storage_id = storage->getStorageID();

    SelectQueryInfo modified_query_info = query_info;

    if (modified_query_info.table_expression)
    {
        auto replacement_table_expression = std::make_shared<TableNode>(storage, storage_lock, storage_snapshot);
        if (query_info.table_expression_modifiers)
            replacement_table_expression->setTableExpressionModifiers(*query_info.table_expression_modifiers);

        modified_query_info.query_tree = modified_query_info.query_tree->cloneAndReplace(modified_query_info.table_expression,
            replacement_table_expression);
        modified_query_info.table_expression = replacement_table_expression;
        modified_query_info.planner_context->getOrCreateTableExpressionData(replacement_table_expression);

        auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects().withVirtuals();
        if (storage_snapshot->storage.supportsSubcolumns())
            get_column_options.withSubcolumns();

        std::unordered_map<std::string, QueryTreeNodePtr> column_name_to_node;

        if (!storage_snapshot->tryGetColumn(get_column_options, "_table"))
            column_name_to_node.emplace("_table", std::make_shared<ConstantNode>(current_storage_id.table_name));

        if (!storage_snapshot->tryGetColumn(get_column_options, "_database"))
            column_name_to_node.emplace("_database", std::make_shared<ConstantNode>(current_storage_id.database_name));

        if (!column_name_to_node.empty())
        {
            replaceColumns(modified_query_info.query_tree,
                replacement_table_expression,
                column_name_to_node);
        }

        modified_query_info.query = queryNodeToSelectQuery(modified_query_info.query_tree);
    }
    else
    {
        bool is_storage_merge_engine = storage->as<StorageMerge>();
        modified_query_info.query = query_info.query->clone();

        /// Original query could contain JOIN but we need only the first joined table and its columns.
        auto & modified_select = modified_query_info.query->as<ASTSelectQuery &>();
        TreeRewriterResult new_analyzer_res = *modified_query_info.syntax_analyzer_result;
        removeJoin(modified_select, new_analyzer_res, modified_context);
        modified_query_info.syntax_analyzer_result = std::make_shared<TreeRewriterResult>(std::move(new_analyzer_res));

        if (!is_storage_merge_engine)
        {
            VirtualColumnUtils::rewriteEntityInAst(modified_query_info.query, "_table", current_storage_id.table_name);
            VirtualColumnUtils::rewriteEntityInAst(modified_query_info.query, "_database", current_storage_id.database_name);
        }
    }

    return modified_query_info;
}

bool recursivelyApplyToReadingSteps(QueryPlan::Node * node, const std::function<bool(ReadFromMergeTree &)> & func)
{
    bool ok = true;
    for (auto * child : node->children)
        ok &= recursivelyApplyToReadingSteps(child, func);

    // This code is mainly meant to be used to call `requestReadingInOrder` on child steps.
    // In this case it is ok if one child will read in order and other will not (though I don't know when it is possible),
    // the only important part is to acknowledge this at the parent and don't rely on any particular ordering of input data.
    if (!ok)
        return false;

    if (auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(node->step.get()))
        ok &= func(*read_from_merge_tree);

    return ok;
}

QueryPipelineBuilderPtr ReadFromMerge::createSources(
    QueryPlan & plan,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & modified_query_info,
    QueryProcessingStage::Enum processed_stage,
    const Block & header,
    const Aliases & aliases,
    const RowPolicyDataOpt & row_policy_data_opt,
    const StorageWithLockAndName & storage_with_lock,
    ContextMutablePtr modified_context,
    bool concat_streams) const
{
    if (!plan.isInitialized())
        return std::make_unique<QueryPipelineBuilder>();

    QueryPipelineBuilderPtr builder;

    const auto & [database_name, storage, _, table_name] = storage_with_lock;
    bool allow_experimental_analyzer = modified_context->getSettingsRef().allow_experimental_analyzer;
    auto storage_stage
        = storage->getQueryProcessingStage(modified_context, QueryProcessingStage::Complete, storage_snapshot, modified_query_info);

    builder = plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(modified_context), BuildQueryPipelineSettings::fromContext(modified_context));

    if (processed_stage > storage_stage || (allow_experimental_analyzer && processed_stage != QueryProcessingStage::FetchColumns))
    {
        /** Materialization is needed, since from distributed storage the constants come materialized.
          * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
          * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
          */
        builder->addSimpleTransform([](const Block & stream_header) { return std::make_shared<MaterializingTransform>(stream_header); });
    }

    if (builder->initialized())
    {
        if (concat_streams && builder->getNumStreams() > 1)
        {
            // It's possible to have many tables read from merge, resize(1) might open too many files at the same time.
            // Using concat instead.
            builder->addTransform(std::make_shared<ConcatProcessor>(builder->getHeader(), builder->getNumStreams()));
        }

        /// Add virtual columns if we don't already have them.

        Block pipe_header = builder->getHeader();

        if (has_database_virtual_column && !pipe_header.has("_database"))
        {
            ColumnWithTypeAndName column;
            column.name = "_database";
            column.type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
            column.column = column.type->createColumnConst(0, Field(database_name));

            auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
            auto adding_column_actions = std::make_shared<ExpressionActions>(
                std::move(adding_column_dag), ExpressionActionsSettings::fromContext(modified_context, CompileExpressions::yes));

            builder->addSimpleTransform([&](const Block & stream_header)
                                        { return std::make_shared<ExpressionTransform>(stream_header, adding_column_actions); });
        }

        if (has_table_virtual_column && !pipe_header.has("_table"))
        {
            ColumnWithTypeAndName column;
            column.name = "_table";
            column.type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
            column.column = column.type->createColumnConst(0, Field(table_name));

            auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
            auto adding_column_actions = std::make_shared<ExpressionActions>(
                std::move(adding_column_dag), ExpressionActionsSettings::fromContext(modified_context, CompileExpressions::yes));

            builder->addSimpleTransform([&](const Block & stream_header)
                                        { return std::make_shared<ExpressionTransform>(stream_header, adding_column_actions); });
        }

        /// Subordinary tables could have different but convertible types, like numeric types of different width.
        /// We must return streams with structure equals to structure of Merge table.
        convertAndFilterSourceStream(header, storage_snapshot->metadata, aliases, row_policy_data_opt, modified_context, *builder, processed_stage);
    }

    return builder;
}

QueryPlan ReadFromMerge::createPlanForTable(
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & modified_query_info,
    QueryProcessingStage::Enum processed_stage,
    UInt64 max_block_size,
    const StorageWithLockAndName & storage_with_lock,
    Names && real_column_names,
    const RowPolicyDataOpt & row_policy_data_opt,
    ContextMutablePtr modified_context,
    size_t streams_num) const
{
    const auto & [database_name, storage, _, table_name] = storage_with_lock;

    auto & modified_select = modified_query_info.query->as<ASTSelectQuery &>();

    if (!InterpreterSelectQuery::isQueryWithFinal(modified_query_info) && storage->needRewriteQueryWithFinal(real_column_names))
    {
        /// NOTE: It may not work correctly in some cases, because query was analyzed without final.
        /// However, it's needed for MaterializedMySQL and it's unlikely that someone will use it with Merge tables.
        modified_select.setFinal();
    }

    bool allow_experimental_analyzer = modified_context->getSettingsRef().allow_experimental_analyzer;

    auto storage_stage = storage->getQueryProcessingStage(modified_context,
        QueryProcessingStage::Complete,
        storage_snapshot,
        modified_query_info);

    QueryPlan plan;

    if (processed_stage <= storage_stage || (allow_experimental_analyzer && processed_stage == QueryProcessingStage::FetchColumns))
    {
        /// If there are only virtual columns in query, you must request at least one other column.
        if (real_column_names.empty())
            real_column_names.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()).name);

        StorageView * view = dynamic_cast<StorageView *>(storage.get());
        if (!view || allow_experimental_analyzer)
        {
            storage->read(plan,
                real_column_names,
                storage_snapshot,
                modified_query_info,
                modified_context,
                processed_stage,
                max_block_size,
                UInt32(streams_num));
        }
        else
        {
            /// For view storage, we need to rewrite the `modified_query_info.view_query` to optimize read.
            /// The most intuitive way is to use InterpreterSelectQuery.

            /// Intercept the settings
            modified_context->setSetting("max_threads", streams_num);
            modified_context->setSetting("max_streams_to_max_threads_ratio", 1);
            modified_context->setSetting("max_block_size", max_block_size);

            InterpreterSelectQuery interpreter(modified_query_info.query,
                modified_context,
                storage,
                view->getInMemoryMetadataPtr(),
                SelectQueryOptions(processed_stage));
            interpreter.buildQueryPlan(plan);
        }

        if (!plan.isInitialized())
            return {};

        if (row_policy_data_opt)
        {
            if (auto * source_step_with_filter = dynamic_cast<SourceStepWithFilter*>((plan.getRootNode()->step.get())))
            {
                row_policy_data_opt->addStorageFilter(source_step_with_filter);
            }
        }

        applyFilters(plan);
    }
    else if (processed_stage > storage_stage || (allow_experimental_analyzer && processed_stage != QueryProcessingStage::FetchColumns))
    {
        /// Maximum permissible parallelism is streams_num
        modified_context->setSetting("max_threads", streams_num);
        modified_context->setSetting("max_streams_to_max_threads_ratio", 1);

        if (allow_experimental_analyzer)
        {
            InterpreterSelectQueryAnalyzer interpreter(modified_query_info.query_tree,
                modified_context,
                SelectQueryOptions(processed_stage));

            auto & planner = interpreter.getPlanner();
            planner.buildQueryPlanIfNeeded();
            plan = std::move(planner).extractQueryPlan();
        }
        else
        {
            modified_select.replaceDatabaseAndTable(database_name, table_name);
            /// TODO: Find a way to support projections for StorageMerge
            InterpreterSelectQuery interpreter{modified_query_info.query,
                modified_context,
                SelectQueryOptions(processed_stage)};

            interpreter.buildQueryPlan(plan);
        }
    }

    return plan;
}

ReadFromMerge::RowPolicyData::RowPolicyData(RowPolicyFilterPtr row_policy_filter_ptr,
    std::shared_ptr<DB::IStorage> storage,
    ContextPtr local_context)
{
    storage_metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto storage_columns = storage_metadata_snapshot->getColumns();
    auto needed_columns = storage_columns.getAll();

    ASTPtr expr = row_policy_filter_ptr->expression;

    auto syntax_result = TreeRewriter(local_context).analyze(expr, needed_columns);
    auto expression_analyzer = ExpressionAnalyzer{expr, syntax_result, local_context};

    actions_dag = expression_analyzer.getActionsDAG(false /* add_aliases */, false /* project_result */);
    filter_actions = std::make_shared<ExpressionActions>(actions_dag,
        ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes));
    const auto & required_columns = filter_actions->getRequiredColumnsWithTypes();
    const auto & sample_block_columns = filter_actions->getSampleBlock().getNamesAndTypesList();

    NamesAndTypesList added, deleted;
    sample_block_columns.getDifference(required_columns, added, deleted);
    if (!deleted.empty() || added.size() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot determine row level filter; {} columns deleted, {} columns added",
            deleted.size(), added.size());
    }

    filter_column_name = added.getNames().front();
}

void ReadFromMerge::RowPolicyData::extendNames(Names & names) const
{
    boost::container::flat_set<std::string_view> names_set(names.begin(), names.end());
    NameSet added_names;

    for (const auto & req_column : filter_actions->getRequiredColumns())
    {
        if (!names_set.contains(req_column))
        {
            added_names.emplace(req_column);
        }
    }

    if (!added_names.empty())
    {
        std::copy(added_names.begin(), added_names.end(), std::back_inserter(names));
    }
}

void ReadFromMerge::RowPolicyData::addStorageFilter(SourceStepWithFilter * step) const
{
    step->addFilter(actions_dag, filter_column_name);
}

void ReadFromMerge::RowPolicyData::addFilterTransform(QueryPipelineBuilder & builder) const
{
    builder.addSimpleTransform([&](const Block & stream_header)
    {
        return std::make_shared<FilterTransform>(stream_header, filter_actions, filter_column_name, true /* remove filter column */);
    });
}

StorageMerge::StorageListWithLocks ReadFromMerge::getSelectedTables(
    ContextPtr query_context,
    bool filter_by_database_virtual_column,
    bool filter_by_table_virtual_column) const
{
    const Settings & settings = query_context->getSettingsRef();
    StorageListWithLocks res;
    DatabaseTablesIterators database_table_iterators = assert_cast<StorageMerge &>(*storage_merge).getDatabaseIterators(query_context);

    MutableColumnPtr database_name_virtual_column;
    MutableColumnPtr table_name_virtual_column;
    if (filter_by_database_virtual_column)
    {
        database_name_virtual_column = ColumnString::create();
    }

    if (filter_by_table_virtual_column)
    {
        table_name_virtual_column = ColumnString::create();
    }

    for (const auto & iterator : database_table_iterators)
    {
        if (filter_by_database_virtual_column)
            database_name_virtual_column->insert(iterator->databaseName());
        while (iterator->isValid())
        {
            StoragePtr storage = iterator->table();
            if (!storage)
                continue;

            if (storage.get() != storage_merge.get())
            {
                auto table_lock = storage->lockForShare(query_context->getCurrentQueryId(), settings.lock_acquire_timeout);
                res.emplace_back(iterator->databaseName(), storage, std::move(table_lock), iterator->name());
                if (filter_by_table_virtual_column)
                    table_name_virtual_column->insert(iterator->name());
            }

            iterator->next();
        }
    }

    if (!filter_by_database_virtual_column && !filter_by_table_virtual_column)
        return res;

    auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes, {}, context);
    if (!filter_actions_dag)
        return res;

    const auto * predicate = filter_actions_dag->getOutputs().at(0);

    if (filter_by_database_virtual_column)
    {
        /// Filter names of selected tables if there is a condition on "_database" virtual column in WHERE clause
        Block virtual_columns_block
            = Block{ColumnWithTypeAndName(std::move(database_name_virtual_column), std::make_shared<DataTypeString>(), "_database")};
        VirtualColumnUtils::filterBlockWithPredicate(predicate, virtual_columns_block, query_context);
        auto values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_database");

        /// Remove unused databases from the list
        res.remove_if([&](const auto & elem) { return values.find(std::get<0>(elem)) == values.end(); });
    }

    if (filter_by_table_virtual_column)
    {
        /// Filter names of selected tables if there is a condition on "_table" virtual column in WHERE clause
        Block virtual_columns_block = Block{ColumnWithTypeAndName(std::move(table_name_virtual_column), std::make_shared<DataTypeString>(), "_table")};
        VirtualColumnUtils::filterBlockWithPredicate(predicate, virtual_columns_block, query_context);
        auto values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_table");

        /// Remove unused tables from the list
        res.remove_if([&](const auto & elem) { return values.find(std::get<3>(elem)) == values.end(); });
    }

    return res;
}

DatabaseTablesIteratorPtr StorageMerge::DatabaseNameOrRegexp::getDatabaseIterator(const String & database_name, ContextPtr local_context) const
{
    auto database = DatabaseCatalog::instance().getDatabase(database_name);

    auto table_name_match = [this, database_name](const String & table_name_) -> bool
    {
        if (source_databases_and_tables)
        {
            if (auto it = source_databases_and_tables->find(database_name); it != source_databases_and_tables->end())
                return it->second.contains(table_name_);
            else
                return false;
        }
        else
            return source_table_regexp->match(table_name_);
    };

    return database->getTablesIterator(local_context, table_name_match);
}

StorageMerge::DatabaseTablesIterators StorageMerge::DatabaseNameOrRegexp::getDatabaseIterators(ContextPtr local_context) const
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

    DatabaseTablesIterators database_table_iterators;

    /// database_name argument is not a regexp
    if (!database_is_regexp)
        database_table_iterators.emplace_back(getDatabaseIterator(source_database_name_or_regexp, local_context));

    /// database_name argument is a regexp
    else
    {
        auto databases = DatabaseCatalog::instance().getDatabases();

        for (const auto & db : databases)
        {
            if (source_database_regexp->match(db.first))
                database_table_iterators.emplace_back(getDatabaseIterator(db.first, local_context));
        }
    }

    return database_table_iterators;
}


void StorageMerge::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    std::optional<NameDependencies> name_deps{};
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::COMMENT_TABLE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());

        if (command.type == AlterCommand::Type::DROP_COLUMN && !command.clear)
        {
            if (!name_deps)
                name_deps = getDependentViewsByColumn(local_context);
            const auto & deps_mv = name_deps.value()[command.column_name];
            if (!deps_mv.empty())
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP column {} which is referenced by materialized view {}",
                    backQuoteIfNeed(command.column_name), toString(deps_mv));
            }
        }
    }
}

void StorageMerge::alter(
    const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    auto table_id = getStorageID();

    StorageInMemoryMetadata storage_metadata = getInMemoryMetadata();
    params.apply(storage_metadata, local_context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, storage_metadata);
    setInMemoryMetadata(storage_metadata);
}

void ReadFromMerge::convertAndFilterSourceStream(
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot,
    const Aliases & aliases,
    const RowPolicyDataOpt & row_policy_data_opt,
    ContextPtr local_context,
    QueryPipelineBuilder & builder,
    QueryProcessingStage::Enum processed_stage)
{
    Block before_block_header = builder.getHeader();

    auto storage_sample_block = metadata_snapshot->getSampleBlock();
    auto pipe_columns = builder.getHeader().getNamesAndTypesList();

    for (const auto & alias : aliases)
    {
        pipe_columns.emplace_back(NameAndTypePair(alias.name, alias.type));
        ASTPtr expr = alias.expression;
        auto syntax_result = TreeRewriter(local_context).analyze(expr, pipe_columns);
        auto expression_analyzer = ExpressionAnalyzer{alias.expression, syntax_result, local_context};

        auto dag = std::make_shared<ActionsDAG>(pipe_columns);
        auto actions_dag = expression_analyzer.getActionsDAG(true, false);
        auto actions = std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes));

        builder.addSimpleTransform([&](const Block & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, actions);
        });
    }

    ActionsDAG::MatchColumnsMode convert_actions_match_columns_mode = ActionsDAG::MatchColumnsMode::Name;

    if (local_context->getSettingsRef().allow_experimental_analyzer && processed_stage != QueryProcessingStage::FetchColumns)
        convert_actions_match_columns_mode = ActionsDAG::MatchColumnsMode::Position;

    if (row_policy_data_opt)
    {
        row_policy_data_opt->addFilterTransform(builder);
    }

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(builder.getHeader().getColumnsWithTypeAndName(),
                                                                header.getColumnsWithTypeAndName(),
                                                                convert_actions_match_columns_mode);
    auto actions = std::make_shared<ExpressionActions>(
        std::move(convert_actions_dag),
        ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes));

    builder.addSimpleTransform([&](const Block & stream_header)
    {
        return std::make_shared<ExpressionTransform>(stream_header, actions);
    });
}

const ReadFromMerge::StorageListWithLocks & ReadFromMerge::getSelectedTables()
{
    filterTablesAndCreateChildrenPlans();
    return selected_tables;
}

bool ReadFromMerge::requestReadingInOrder(InputOrderInfoPtr order_info_)
{
    filterTablesAndCreateChildrenPlans();

    /// Disable read-in-order optimization for reverse order with final.
    /// Otherwise, it can lead to incorrect final behavior because the implementation may rely on the reading in direct order).
    if (order_info_->direction != 1 && InterpreterSelectQuery::isQueryWithFinal(query_info))
        return false;

    auto request_read_in_order = [order_info_](ReadFromMergeTree & read_from_merge_tree)
    {
        return read_from_merge_tree.requestReadingInOrder(
            order_info_->used_prefix_of_sorting_key_size, order_info_->direction, order_info_->limit);
    };

    bool ok = true;
    for (const auto & child_plan : *child_plans)
        if (child_plan.plan.isInitialized())
            ok &= recursivelyApplyToReadingSteps(child_plan.plan.getRootNode(), request_read_in_order);

    if (!ok)
        return false;

    order_info = order_info_;
    query_info.input_order_info = order_info;
    return true;
}

void ReadFromMerge::applyFilters(const QueryPlan & plan) const
{
    auto apply_filters = [this](ReadFromMergeTree & read_from_merge_tree)
    {
        size_t filters_dags_size = filter_dags.size();
        for (size_t i = 0; i < filters_dags_size; ++i)
            read_from_merge_tree.addFilter(filter_dags[i], filter_nodes.nodes[i]);

        read_from_merge_tree.applyFilters();
        return true;
    };

    recursivelyApplyToReadingSteps(plan.getRootNode(), apply_filters);
}

void ReadFromMerge::applyFilters()
{
    filterTablesAndCreateChildrenPlans();

    for (const auto & child_plan : *child_plans)
        if (child_plan.plan.isInitialized())
            applyFilters(child_plan.plan);
}

IStorage::ColumnSizeByName StorageMerge::getColumnSizes() const
{
    ColumnSizeByName column_sizes;

    forEachTable([&](const auto & table)
    {
        for (const auto & [name, size] : table->getColumnSizes())
            column_sizes[name].add(size);
    });

    return column_sizes;
}


std::tuple<bool /* is_regexp */, ASTPtr> StorageMerge::evaluateDatabaseName(const ASTPtr & node, ContextPtr context_)
{
    if (const auto * func = node->as<ASTFunction>(); func && func->name == "REGEXP")
    {
        if (func->arguments->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "REGEXP in Merge ENGINE takes only one argument");

        auto * literal = func->arguments->children[0]->as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::Which::String || literal->value.safeGet<String>().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument for REGEXP in Merge ENGINE should be a non empty String Literal");

        return {true, func->arguments->children[0]};
    }

    auto ast = evaluateConstantExpressionForDatabaseName(node, context_);
    return {false, ast};
}

bool StorageMerge::supportsTrivialCountOptimization() const
{
    return getFirstTable([&](const auto & table) { return !table->supportsTrivialCountOptimization(); }) == nullptr;
}

std::optional<UInt64> StorageMerge::totalRows(const Settings & settings) const
{
    return totalRowsOrBytes([&](const auto & table) { return table->totalRows(settings); });
}

std::optional<UInt64> StorageMerge::totalBytes(const Settings & settings) const
{
    return totalRowsOrBytes([&](const auto & table) { return table->totalBytes(settings); });
}

template <typename F>
std::optional<UInt64> StorageMerge::totalRowsOrBytes(F && func) const
{
    UInt64 total_rows_or_bytes = 0;
    auto first_table = getFirstTable([&](const auto & table)
    {
        if (auto rows_or_bytes = func(table))
        {
            total_rows_or_bytes += *rows_or_bytes;
            return false;
        }
        return true;
    });

    return first_table ? std::nullopt : std::make_optional(total_rows_or_bytes);
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Merge requires exactly 2 parameters - name "
                            "of source database and regexp for table names.");

        auto [is_regexp, database_ast] = StorageMerge::evaluateDatabaseName(engine_args[0], args.getLocalContext());

        if (!is_regexp)
            engine_args[0] = database_ast;

        String source_database_name_or_regexp = checkAndGetLiteralArgument<String>(database_ast, "database_name");

        engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.getLocalContext());
        String table_name_regexp = checkAndGetLiteralArgument<String>(engine_args[1], "table_name_regexp");

        return std::make_shared<StorageMerge>(
            args.table_id, args.columns, args.comment, source_database_name_or_regexp, is_regexp, table_name_regexp, args.getContext());
    },
    {
        .supports_schema_inference = true
    });
}

NamesAndTypesList StorageMerge::getVirtuals() const
{
    NamesAndTypesList virtuals{
        {"_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto first_table = getFirstTable([](auto && table) { return table; });
    if (first_table)
    {
        auto table_virtuals = first_table->getVirtuals();
        virtuals.insert(virtuals.end(), table_virtuals.begin(), table_virtuals.end());
    }

    return virtuals;
}

}
