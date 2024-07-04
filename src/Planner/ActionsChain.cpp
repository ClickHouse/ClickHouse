#include <Planner/ActionsChain.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

ActionsChainStep::ActionsChainStep(ActionsAndProjectInputsFlagPtr actions_,
    bool use_actions_nodes_as_output_columns_,
    ColumnsWithTypeAndName additional_output_columns_)
    : actions(std::move(actions_))
    , use_actions_nodes_as_output_columns(use_actions_nodes_as_output_columns_)
    , additional_output_columns(std::move(additional_output_columns_))
{
    initialize();
}

void ActionsChainStep::finalizeInputAndOutputColumns(const NameSet & child_input_columns)
{
    child_required_output_columns_names.clear();

    auto child_input_columns_copy = child_input_columns;

    std::unordered_set<std::string_view> output_nodes_names;
    output_nodes_names.reserve(actions->dag.getOutputs().size());

    for (auto & output_node : actions->dag.getOutputs())
        output_nodes_names.insert(output_node->result_name);

    for (const auto & node : actions->dag.getNodes())
    {
        auto it = child_input_columns_copy.find(node.result_name);
        if (it == child_input_columns_copy.end())
            continue;

        child_input_columns_copy.erase(it);
        child_required_output_columns_names.insert(node.result_name);

        if (output_nodes_names.contains(node.result_name))
            continue;

        actions->dag.getOutputs().push_back(&node);
        output_nodes_names.insert(node.result_name);
    }

    actions->dag.removeUnusedActions();
    /// TODO: Analyzer fix ActionsDAG input and constant nodes with same name
    actions->project_input = true;
    initialize();
}

void ActionsChainStep::dump(WriteBuffer & buffer) const
{
    buffer << "DAG" << '\n';
    buffer << actions->dag.dumpDAG();

    if (!available_output_columns.empty())
    {
        buffer << "Available output columns " << available_output_columns.size() << '\n';
        for (const auto & column : available_output_columns)
            buffer << "Name " << column.name << " type " << column.type->getName() << '\n';
    }

    if (!child_required_output_columns_names.empty())
    {
        buffer << "Child required output columns " << boost::join(child_required_output_columns_names, ", ");
        buffer << '\n';
    }
}

String ActionsChainStep::dump() const
{
    WriteBufferFromOwnString buffer;
    dump(buffer);

    return buffer.str();
}

void ActionsChainStep::initialize()
{
    auto required_columns_names = actions->dag.getRequiredColumnsNames();
    input_columns_names = NameSet(required_columns_names.begin(), required_columns_names.end());

    available_output_columns.clear();

    if (use_actions_nodes_as_output_columns)
    {
        std::unordered_set<std::string_view> available_output_columns_names;

        for (const auto & node : actions->dag.getNodes())
        {
            if (available_output_columns_names.contains(node.result_name))
                continue;

            available_output_columns.emplace_back(node.column, node.result_type, node.result_name);
            available_output_columns_names.insert(node.result_name);
        }
    }

    available_output_columns.insert(available_output_columns.end(), additional_output_columns.begin(), additional_output_columns.end());
}

void ActionsChain::finalize()
{
    if (steps.empty())
        return;

    /// For last chain step there are no columns required in child nodes
    NameSet empty_child_input_columns;
    steps.back().get()->finalizeInputAndOutputColumns(empty_child_input_columns);

    Int64 steps_last_index = steps.size() - 1;
    for (Int64 i = steps_last_index; i >= 1; --i)
    {
        auto & current_step = steps[i];
        auto & previous_step = steps[i - 1];

        previous_step->finalizeInputAndOutputColumns(current_step->getInputColumnNames());
    }
}

void ActionsChain::dump(WriteBuffer & buffer) const
{
    size_t steps_size = steps.size();

    for (size_t i = 0; i < steps_size; ++i)
    {
        const auto & step = steps[i];
        buffer << "Step " << i << '\n';
        step->dump(buffer);

        buffer << '\n';
    }
}

String ActionsChain::dump() const
{
    WriteBufferFromOwnString buffer;
    dump(buffer);

    return buffer.str();
}

}
