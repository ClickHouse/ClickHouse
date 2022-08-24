#include <Planner/ActionsChain.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

void ActionsChainStep::finalizeInputAndOutputColumns(const NameSet & child_input_columns)
{
    child_required_output_columns_names.clear();
    std::vector<const ActionsDAG::Node *> required_output_nodes;

    auto child_input_columns_copy = child_input_columns;

    for (const auto & node : actions->getNodes())
    {
        auto it = child_input_columns_copy.find(node.result_name);
        if (it == child_input_columns_copy.end())
            continue;

        child_required_output_columns_names.insert(node.result_name);
        required_output_nodes.push_back(&node);
        child_input_columns_copy.erase(it);
    }

    for (auto & required_output_node : required_output_nodes)
        actions->addOrReplaceInOutputs(*required_output_node);

    actions->removeUnusedActions();
    initialize();
}

void ActionsChainStep::dump(WriteBuffer & buffer) const
{
    buffer << "DAG" << '\n';
    buffer << actions->dumpDAG();
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
    auto required_columns_names = actions->getRequiredColumnsNames();
    input_columns_names = NameSet(required_columns_names.begin(), required_columns_names.end());

    available_output_columns.clear();

    for (const auto & node : actions->getNodes())
    {
        if (available_output_columns_only_aliases)
        {
            if (node.type == ActionsDAG::ActionType::ALIAS)
                available_output_columns.emplace_back(node.column, node.result_type, node.result_name);

            continue;
        }

        if (node.type == ActionsDAG::ActionType::INPUT ||
            node.type == ActionsDAG::ActionType::FUNCTION ||
            node.type == ActionsDAG::ActionType::ALIAS ||
            node.type == ActionsDAG::ActionType::ARRAY_JOIN)
            available_output_columns.emplace_back(node.column, node.result_type, node.result_name);
    }
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
    size_t nodes_size = steps.size();

    for (size_t i = 0; i < nodes_size; ++i)
    {
        const auto & node = steps[i];
        buffer << "Step " << i << '\n';
        node->dump(buffer);

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
