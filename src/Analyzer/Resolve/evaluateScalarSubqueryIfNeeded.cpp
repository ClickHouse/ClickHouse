#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Planner/Utils.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Chunk.h>

#include <Core/Settings.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Storages/IStorage.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace ProfileEvents
{
    extern const Event ScalarSubqueriesGlobalCacheHit;
    extern const Event ScalarSubqueriesLocalCacheHit;
    extern const Event ScalarSubqueriesCacheMiss;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}

namespace Setting
{
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsBool extremes;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsString implicit_table_at_top_level;
    extern const SettingsUInt64 use_structure_from_insertion_table_in_table_functions;
    extern const SettingsBool enable_scalar_subquery_optimization;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
}

namespace
{

bool subtreeHasViewSource(const IQueryTreeNode * node, const Context & context)
{
    if (!node)
        return false;

    if (const auto * table_node = node->as<TableNode>())
    {
        if (table_node->getStorageID().getFullNameNotQuoted() == context.getViewSource()->getStorageID().getFullNameNotQuoted())
            return true;
    }

    for (const auto & child : node->getChildren())
        if (subtreeHasViewSource(child.get(), context))
            return true;

    return false;
}

}

/// Evaluate scalar subquery and perform constant folding if scalar subquery does not have constant value
void QueryAnalyzer::evaluateScalarSubqueryIfNeeded(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool execute_for_exists)
{
    auto * query_node = node->as<QueryNode>();
    auto * union_node = node->as<UnionNode>();
    if (!query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node must have query or union type. Actual: {} {}",
            node->getNodeTypeName(),
            node->formatASTForErrorMessage());

    bool is_correlated_subquery = (query_node != nullptr && query_node->isCorrelated())
                                || (union_node != nullptr && union_node->isCorrelated());
    if (is_correlated_subquery)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot evaluate correlated scalar subquery");

    auto & context = scope.context;

    Block scalar_block;

    auto node_without_alias = node->clone();
    node_without_alias->removeAlias();

    QueryTreeNodePtrWithHash node_with_hash(node_without_alias);
    auto str_hash = DB::toString(node_with_hash.hash);

    bool can_use_global_scalars = !only_analyze && !(context->getViewSource() && subtreeHasViewSource(node_without_alias.get(), *context));

    auto & scalars_cache = can_use_global_scalars ? scalar_subquery_to_scalar_value_global : scalar_subquery_to_scalar_value_local;

    if (scalars_cache.contains(node_with_hash))
    {
        if (can_use_global_scalars)
            ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
        else
            ProfileEvents::increment(ProfileEvents::ScalarSubqueriesLocalCacheHit);

        scalar_block = scalars_cache.at(node_with_hash);
    }
    else if (context->hasQueryContext() && can_use_global_scalars && context->getQueryContext()->hasScalar(str_hash))
    {
        scalar_block = context->getQueryContext()->getScalar(str_hash);
        scalar_subquery_to_scalar_value_global.emplace(node_with_hash, scalar_block);
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesCacheMiss);
        auto subquery_context = Context::createCopy(context);

        Settings subquery_settings = context->getSettingsCopy();
        subquery_settings[Setting::max_result_rows] = 1;
        subquery_settings[Setting::extremes] = false;
        subquery_settings[Setting::implicit_table_at_top_level] = "";
        /// When execute `INSERT INTO t WITH ... SELECT ...`, it may lead to `Unknown columns`
        /// exception with this settings enabled(https://github.com/ClickHouse/ClickHouse/issues/52494).
        subquery_settings[Setting::use_structure_from_insertion_table_in_table_functions] = false;
        subquery_settings[Setting::allow_experimental_parallel_reading_from_replicas] = 0;
        subquery_context->setSettings(subquery_settings);

        auto query_tree = node->clone();
        /// Update context for the QueryTree, because apparently Planner would use this context.
        if (auto * new_query_node = query_tree->as<QueryNode>())
            new_query_node->getMutableContext() = subquery_context;
        if (auto * new_union_node = query_tree->as<UnionNode>())
            new_union_node->getMutableContext() = subquery_context;

        auto options = SelectQueryOptions(QueryProcessingStage::Complete, scope.subquery_depth, true /*is_subquery*/);
        options.only_analyze = only_analyze;

        QueryTreePassManager query_tree_pass_manager(subquery_context);
        addQueryTreePasses(query_tree_pass_manager, options.only_analyze);
        query_tree_pass_manager.run(query_tree);

        if (auto storage = subquery_context->getViewSource())
            replaceStorageInQueryTree(query_tree, subquery_context, storage);
        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_tree, subquery_context, options);

        auto wrap_with_nullable_or_tuple = [](Block & block)
        {
            block = materializeBlock(block);
            if (block.columns() == 1)
            {
                auto & column = block.getByPosition(0);
                /// Here we wrap type to nullable if we can.
                /// It is needed cause if subquery return no rows, it's result will be Null.
                /// In case of many columns, do not check it cause tuple can't be nullable.
                if (!column.type->isNullable() && column.type->canBeInsideNullable())
                {
                    column.type = makeNullable(column.type);
                    column.column = makeNullable(column.column);
                }
            } else
            {
                /** Make unique column names for tuple.
                *
                * Example: SELECT (SELECT 2 AS x, x)
                */
                makeUniqueColumnNamesInBlock(block);
                block = Block({{
                        ColumnTuple::create(block.getColumns()),
                        std::make_shared<DataTypeTuple>(block.getDataTypes(), block.getNames()),
                        "tuple"
                    }});
            }
        };

        if (only_analyze)
        {
            /// If query is only analyzed, then constants are not correct.
            scalar_block = *interpreter->getSampleBlock();
            for (auto & column : scalar_block)
            {
                if (column.column->empty())
                {
                    auto mut_col = column.column->cloneEmpty();
                    mut_col->insertDefault();
                    column.column = std::move(mut_col);
                }
            }

            wrap_with_nullable_or_tuple(scalar_block);
        }
        else
        {
            BlockIO io;
            auto & query_plan = interpreter->getQueryPlan();
            bool skip_execution_for_exists = false;

            if (execute_for_exists)
            {
                if (optimizePlanForExists(query_plan))
                    skip_execution_for_exists = true;
                else
                {
                    auto limit_step = std::make_unique<LimitStep>(query_plan.getCurrentHeader(), 1, 0);
                    query_plan.addStep(std::move(limit_step));
                }
            }

            if (!skip_execution_for_exists)
            {
                QueryPlanOptimizationSettings optimization_settings(subquery_context);
                BuildQueryPipelineSettings build_pipeline_settings(subquery_context);

                query_plan.setConcurrencyControl(subquery_context->getSettingsRef()[Setting::use_concurrency_control]);

                auto pipeline_builder = std::move(*query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings));

                io.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
                io.pipeline.setQuota(subquery_context->getQuota());
            }

            std::optional<PullingAsyncPipelineExecutor> executor;
            Chunk chunk;

            if (!skip_execution_for_exists)
            {
                io.pipeline.setProgressCallback(context->getProgressCallback());
                io.pipeline.setProcessListElement(context->getProcessListElement());
                io.pipeline.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

                executor.emplace(io.pipeline);
                while (chunk.getNumRows() == 0 && executor->pull(chunk))
                {
                }
            }

            if (chunk.getNumRows() == 0)
            {
                DataTypePtr type;
                if (execute_for_exists)
                    type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
                else
                {
                    const auto & sample_block = interpreter->getSampleBlock();
                    auto types = sample_block->getDataTypes();
                    if (types.size() != 1)
                        types = {std::make_shared<DataTypeTuple>(types)};

                    type = types[0];
                    if (!type->isNullable())
                    {
                        if (!type->canBeInsideNullable())
                            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                                "Scalar subquery returned empty result of type {} which cannot be Nullable",
                                type->getName());

                        type = makeNullable(type);
                    }
                }

                auto scalar_column = type->createColumn();
                if (skip_execution_for_exists)
                    scalar_column->insert(1);
                else
                    scalar_column->insert(Null());
                scalar_block.insert({std::move(scalar_column), type, (execute_for_exists ? "exists" : "null")});
            }
            else
            {
                if (chunk.getNumRows() != 1)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

                Chunk tmp_chunk;
                while (tmp_chunk.getNumRows() == 0 && executor->pull(tmp_chunk))
                {
                }

                if (tmp_chunk.getNumRows() != 0)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

                if (execute_for_exists)
                {
                    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
                    auto scalar_column = type->createColumn();
                    scalar_column->insert(1);
                    scalar_block.insert({std::move(scalar_column), type, "exists"});
                }
                else
                {
                    auto block = executor->getHeader().cloneWithColumns(chunk.getColumns());
                    wrap_with_nullable_or_tuple(block);
                    scalar_block = std::move(block);
                }
            }

            logProcessorProfile(context, io.pipeline.getProcessors());
        }

        scalars_cache.emplace(node_with_hash, scalar_block);
        if (can_use_global_scalars && context->hasQueryContext())
            context->getQueryContext()->addScalar(str_hash, scalar_block);
    }

    const auto & scalar_column_with_type = scalar_block.safeGetByPosition(0);
    const auto & scalar_type = scalar_column_with_type.type;

    const auto * scalar_type_name = scalar_block.safeGetByPosition(0).type->getFamilyName();
    static const std::set<std::string_view> useless_literal_types = {"Array", "Tuple", "AggregateFunction", "Function", "Set", "LowCardinality"};
    auto * nearest_query_scope = scope.getNearestQueryScope();

    /// Always convert to literals when there is no query context
    if (!context->getSettingsRef()[Setting::enable_scalar_subquery_optimization] || !useless_literal_types.contains(scalar_type_name)
        || !context->hasQueryContext() || !nearest_query_scope)
    {
        ConstantValue constant_value{ scalar_column_with_type.column, scalar_type };
        auto constant_node = std::make_shared<ConstantNode>(constant_value, node);

        if (scalar_column_with_type.column->isNullAt(0))
        {
            node = buildCastFunction(constant_node, constant_node->getResultType(), context);
            node = std::make_shared<ConstantNode>(std::move(constant_value), node);
        }
        else
            node = std::move(constant_node);

        return;
    }

    auto & nearest_query_scope_query_node = nearest_query_scope->scope_node->as<QueryNode &>();
    auto & mutable_context = nearest_query_scope_query_node.getMutableContext();

    auto scalar_query_hash_string = DB::toString(node_with_hash.hash) + (only_analyze ? "_analyze" : "");

    if (mutable_context->hasQueryContext())
        mutable_context->getQueryContext()->addScalar(scalar_query_hash_string, scalar_block);

    mutable_context->addScalar(scalar_query_hash_string, scalar_block);

    std::string get_scalar_function_name = "__getScalar";

    auto scalar_query_hash_constant_node = std::make_shared<ConstantNode>(std::move(scalar_query_hash_string), std::make_shared<DataTypeString>());

    auto get_scalar_function_node = std::make_shared<FunctionNode>(get_scalar_function_name);
    get_scalar_function_node->getArguments().getNodes().push_back(std::move(scalar_query_hash_constant_node));

    auto get_scalar_function = FunctionFactory::instance().get(get_scalar_function_name, mutable_context);
    get_scalar_function_node->resolveAsFunction(get_scalar_function->build(get_scalar_function_node->getArgumentColumns()));

    node = std::move(get_scalar_function_node);
}

}
