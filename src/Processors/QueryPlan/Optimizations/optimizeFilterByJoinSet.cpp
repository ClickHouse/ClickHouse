#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>

#include <Core/Settings.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Functions/tuple.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{
// namespace Setting
// {
//     extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
// }


namespace QueryPlanOptimizations
{

ReadFromMergeTree * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return reading;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step) || typeid_cast<ArrayJoinStep *>(step))
        return findReadingStep(*node.children.front());

    if (auto * distinct = typeid_cast<DistinctStep *>(step); distinct && distinct->isPreliminary())
        return findReadingStep(*node.children.front());

    return nullptr;
}


void appendExpression(std::optional<ActionsDAG> & dag, const ActionsDAG & expression)
{
    if (dag)
        dag->mergeInplace(expression.clone());
    else
        dag = expression.clone();
}

/// This function builds a common DAG which is a merge of DAGs from Filter and Expression steps chain.
void buildSortingDAG(QueryPlan::Node & node, std::optional<ActionsDAG> & dag)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (const auto prewhere_info = reading->getPrewhereInfo())
        {
            //std::cerr << "====== Adding prewhere " << std::endl;
            appendExpression(dag, prewhere_info->prewhere_actions);
        }
        return;
    }

    if (node.children.size() != 1)
        return;

    buildSortingDAG(*node.children.front(), dag);

    if (typeid_cast<DistinctStep *>(step))
    {
    }

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        const auto & actions = expression->getExpression();
        appendExpression(dag, actions);
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        appendExpression(dag, filter->getExpression());
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(step))
    {
        const auto & array_joined_columns = array_join->getColumns();

        if (dag)
        {
            std::unordered_set<std::string_view> keys_set(array_joined_columns.begin(), array_joined_columns.end());

            /// Remove array joined columns from outputs.
            /// Types are changed after ARRAY JOIN, and we can't use this columns anyway.
            ActionsDAG::NodeRawConstPtrs outputs;
            outputs.reserve(dag->getOutputs().size());

            for (const auto & output : dag->getOutputs())
            {
                if (!keys_set.contains(output->result_name))
                    outputs.push_back(output);
            }

            dag->getOutputs() = std::move(outputs);
        }
    }
}

void optimizeFilterByJoinSet(QueryPlan::Node & node)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step)
        return;

    // std::cerr << "optimizeFilterByJoinSet\n";

    const auto & join = join_step->getJoin();
    auto * hash_join = typeid_cast<HashJoin *>(join.get());
    if (!hash_join)
        return;

    // std::cerr << "optimizeFilterByJoinSet got hash join\n";

    const auto & table_join = join->getTableJoin();
    if (table_join.kind() != JoinKind::Inner && table_join.kind() != JoinKind::Right)
        return;

    if (table_join.strictness() != JoinStrictness::Any && table_join.strictness() != JoinStrictness::All && table_join.strictness() != JoinStrictness::Semi)
        return;

    const auto & clauses = table_join.getClauses();
    if (clauses.empty())
        return;

    // std::cerr << "optimizeFilterByJoinSetone class\n";

    auto * reading = findReadingStep(*node.children.front());
    if (!reading)
        return;

    if (reading->splitsRangesIntoIntersectionAndNonIntersecting() || reading->isQueryWithFinal())
        return;

    // if (reading->getContext()->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas])
    //     return;

    // std::cerr << "optimizeFilterByJoinSetone reading\n";

    const auto & pk = reading->getStorageMetadata()->getPrimaryKey();
    if (pk.column_names.empty())
        return;

    // std::cerr << "optimizeFilterByJoinSetone pk\n";

    std::optional<ActionsDAG> dag;
    buildSortingDAG(*node.children.front(), dag);

    if (!dag)
        dag = ActionsDAG(reading->getOutputHeader().getColumnsWithTypeAndName());

    // std::cerr << "optimizeFilterByJoinSetone sorting dag " << dag->dumpDAG() << std::endl;

    std::unordered_map<std::string_view, const ActionsDAG::Node *> outputs;
    for (const auto & output : dag->getOutputs())
        outputs.emplace(output->result_name, output);

    const Block & right_source_columns = node.children.back()->step->getOutputHeader();
    ActionsDAG::NodeRawConstPtrs predicates;
    DynamicJoinFilters join_filters;
    std::vector<Names> right_keys_per_clause;

    FunctionOverloadResolverPtr func_tuple_builder = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());

    for (const auto & clause : clauses)
    {
        // Names left_keys;
        Names right_keys;
        std::vector<const ActionsDAG::Node *> left_columns;
        std::vector<ColumnWithTypeAndName> right_columns;

        size_t keys_size = clause.key_names_left.size();

        for (size_t i = 0; i < keys_size; ++i)
        {
            const auto & left_name = clause.key_names_left[i];
            const auto & right_name = clause.key_names_right[i];

            // std::cerr << left_name << ' ' << right_name << std::endl;

            auto it = outputs.find(left_name);
            if (it != outputs.end())
            {
                // left_keys.push_back(left_name);
                right_keys.push_back(right_name);
                left_columns.push_back(it->second);
                right_columns.push_back(right_source_columns.getByName(right_name));
            }
        }

        if (left_columns.empty())
            return;

        // std::cerr << "optimizeFilterByJoinSetone some coluns\n";

        const ActionsDAG::Node * in_lhs_arg = left_columns.front();
        if (left_columns.size() > 1)
            in_lhs_arg = &dag->addFunction(func_tuple_builder, std::move(left_columns), {});

        auto context = reading->getContext();
        auto test_set = std::make_shared<FutureSetFromTuple>(Block(right_columns), context->getSettingsRef());
        auto column_set = ColumnSet::create(1, std::move(test_set));
        ColumnSet * column_set_ptr = column_set.get();
        ColumnPtr set_col = ColumnConst::create(std::move(column_set), 0);

        const ActionsDAG::Node * in_rhs_arg = &dag->addColumn({set_col, std::make_shared<DataTypeSet>(), {}});

        auto func_in = FunctionFactory::instance().get("in", context);
        const ActionsDAG::Node * predicate = &dag->addFunction(func_in, {in_lhs_arg, in_rhs_arg}, {});

        join_filters.clauses.emplace_back(column_set_ptr, right_keys);
        right_keys_per_clause.emplace_back(std::move(right_keys));
        predicates.emplace_back(predicate);
    }

    if (predicates.size() > 1)
    {
        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());
        predicates = {&dag->addFunction(func_builder_and, std::move(predicates), {})};
    }

    dag->getOutputs() = std::move(predicates);
    dag->removeUnusedActions();

    // std::cerr << "optimizeFilterByJoinSetone dag " << dag->dumpDAG() << std::endl;

    auto metadata_snapshot = reading->getStorageMetadata();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const Names & primary_key_column_names = primary_key.column_names;
    auto context = reading->getContext();

    KeyCondition key_condition(&*dag, context, primary_key_column_names, primary_key.expression);

    // std::cerr << "optimizeFilterByJoinSetone matched cond " << key_condition.toString() << std::endl;

    /// Condition is (join keys) IN (empty set).
    if (key_condition.alwaysUnknownOrTrue())
        return;

    // std::cerr << "optimizeFilterByJoinSetone matched cond " << std::endl;

    auto dynamic_parts = reading->useDynamiclyFilteredParts();

    join_filters.actions = std::move(*dag);
    join_filters.parts = dynamic_parts;
    join_filters.context = context;
    join_filters.metadata = metadata_snapshot;

    join_step->setDynamicFilter(std::make_shared<DynamicJoinFilters>(std::move(join_filters)));
    hash_join->saveRightKeyColumnsForFilter(std::move(right_keys_per_clause));
}

}
}
