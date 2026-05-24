#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/RowWrapper.h>

#include <unordered_map>
#include <unordered_set>


namespace ProfileEvents
{
    extern const Event RowWrapperReads;
    extern const Event RowWrapperReadFields;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::QueryPlanOptimizations
{

namespace
{

struct ExtractedField
{
    String name;            /// Source column name (== Row field name).
    size_t one_based_index; /// Field position within the wrapper, passed to __rowElement.
};

struct WrapperPick
{
    String wrapper_name;
    std::vector<ExtractedField> fields_to_extract;
};

/// Greedily assign required columns to wrappers, preferring the wrapper that
/// covers the most uncovered columns (ties broken by the smaller wrapper).
/// Cost guard: only use a wrapper that replaces >= 2 column reads, unless it is
/// fully covered (no wasted fields).
std::vector<WrapperPick> pickWrappers(const Names & required_columns, const std::vector<RowWrapperInfo> & wrappers)
{
    std::vector<WrapperPick> picks;
    std::unordered_set<String> uncovered(required_columns.begin(), required_columns.end());

    while (true)
    {
        const RowWrapperInfo * best = nullptr;
        std::vector<ExtractedField> best_overlap;

        for (const auto & w : wrappers)
        {
            std::vector<ExtractedField> overlap;
            for (size_t i = 0; i < w.wrapped_columns.size(); ++i)
                if (uncovered.contains(w.wrapped_columns[i]))
                    overlap.push_back({w.wrapped_columns[i], i + 1});

            if (overlap.size() < 2 && overlap.size() != w.wrapped_columns.size())
                continue;

            const bool better = !best
                || overlap.size() > best_overlap.size()
                || (overlap.size() == best_overlap.size() && w.wrapped_columns.size() < best->wrapped_columns.size());

            if (better)
            {
                best = &w;
                best_overlap = std::move(overlap);
            }
        }

        if (!best)
            break;

        for (const auto & f : best_overlap)
            uncovered.erase(f.name);
        picks.push_back({best->wrapper_name, std::move(best_overlap)});
    }

    return picks;
}

/// DAG mapping the rewritten ReadFromMergeTree output (wrappers + survivors)
/// back to the original output: each covered column becomes
/// __rowElement(wrapper, index) named after the original column.
ActionsDAG buildUnpackDAG(
    const Block & new_rfmt_output,
    const Block & original_rfmt_output,
    const std::vector<WrapperPick> & picks)
{
    ActionsDAG dag(new_rfmt_output.getColumnsWithTypeAndName());

    auto row_element_fn = FunctionFactory::instance().get("__rowElement", nullptr);
    auto index_type = std::make_shared<DataTypeUInt64>();

    std::unordered_map<String, const ActionsDAG::Node *> extracted;
    for (const auto & pick : picks)
    {
        const auto * wrapper_node = dag.tryFindInOutputs(pick.wrapper_name);
        if (!wrapper_node)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "optimizeUseRowWrappers: wrapper column '{}' missing after rewrite", pick.wrapper_name);

        for (const auto & field : pick.fields_to_extract)
        {
            auto index_const = index_type->createColumnConst(1, field.one_based_index);
            const auto * index_node = &dag.addColumn(ColumnWithTypeAndName(
                std::move(index_const), index_type, std::to_string(field.one_based_index) + "_UInt64"));
            extracted.emplace(field.name, &dag.addFunction(row_element_fn, {wrapper_node, index_node}, field.name));
        }
    }

    ActionsDAG::NodeRawConstPtrs new_outputs;
    new_outputs.reserve(original_rfmt_output.columns());
    for (const auto & col : original_rfmt_output.getColumnsWithTypeAndName())
    {
        if (auto it = extracted.find(col.name); it != extracted.end())
            new_outputs.push_back(it->second);
        else if (const auto * input_node = dag.tryFindInOutputs(col.name))
            new_outputs.push_back(input_node);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "optimizeUseRowWrappers: column '{}' is neither read nor covered by a wrapper", col.name);
    }
    dag.getOutputs() = std::move(new_outputs);
    dag.removeUnusedActions();

    return dag;
}

}

/// Routes column reads through a Row(...) wrapper: mutates a child
/// ReadFromMergeTree to read the wrapper(s) plus uncovered columns, then splices
/// an ExpressionStep that unpacks the original columns via __rowElement.
size_t tryOptimizeUseRowWrappers(
    QueryPlan::Node * parent_node,
    QueryPlan::Nodes & nodes,
    const Optimization::ExtraSettings &)
{
    if (!parent_node || parent_node->children.empty())
        return 0;

    for (size_t child_idx = 0; child_idx < parent_node->children.size(); ++child_idx)
    {
        QueryPlan::Node * child_node = parent_node->children[child_idx];
        if (!child_node)
            continue;

        auto * reading = typeid_cast<ReadFromMergeTree *>(child_node->step.get());
        if (!reading)
            continue;

        auto wrappers = collectRowWrappers(reading->getStorageMetadata()->getColumns());
        if (wrappers.empty())
            continue;

        auto picks = pickWrappers(reading->getAllColumnNames(), wrappers);
        if (picks.empty())
            continue;

        auto original_output_header = *reading->getOutputHeader();

        Names columns_to_remove;
        Names wrappers_to_add;
        wrappers_to_add.reserve(picks.size());
        for (const auto & pick : picks)
        {
            wrappers_to_add.push_back(pick.wrapper_name);
            for (const auto & f : pick.fields_to_extract)
                columns_to_remove.push_back(f.name);
        }
        reading->replaceWithRowWrappers(columns_to_remove, wrappers_to_add);

        ActionsDAG unpack_dag = buildUnpackDAG(*reading->getOutputHeader(), original_output_header, picks);

        auto & expr_node = nodes.emplace_back();
        expr_node.children.push_back(child_node);
        expr_node.step = std::make_unique<ExpressionStep>(reading->getOutputHeader(), std::move(unpack_dag));
        parent_node->children[child_idx] = &expr_node;

        ProfileEvents::increment(ProfileEvents::RowWrapperReads);
        ProfileEvents::increment(ProfileEvents::RowWrapperReadFields, columns_to_remove.size());

        LOG_TRACE(getLogger("QueryPlanOptimizations"),
            "Row wrapper rewrite on {}: {} wrapper(s) cover {} field(s)",
            reading->getMergeTreeData().getStorageID().getNameForLogs(), picks.size(), columns_to_remove.size());

        return 1;
    }

    return 0;
}

}
