#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

#include <Core/Field.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <Core/SortDescription.h>

#include <stack>

namespace DB::ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace DB
{
MatchedTrees::Matches matchTrees(const ActionsDAG::NodeRawConstPtrs & inner_dag, const ActionsDAG & outer_dag, bool check_monotonicity)
{
    using Parents = std::set<const ActionsDAG::Node *>;
    std::unordered_map<const ActionsDAG::Node *, Parents> inner_parents;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> inner_inputs;

    {
        std::stack<const ActionsDAG::Node *> stack;
        for (const auto * out : inner_dag)
        {
            if (inner_parents.contains(out))
                continue;

            stack.push(out);
            inner_parents.emplace(out, Parents());
            while (!stack.empty())
            {
                const auto * node = stack.top();
                stack.pop();

                if (node->type == ActionsDAG::ActionType::INPUT)
                    inner_inputs.emplace(node->result_name, node);

                for (const auto * child : node->children)
                {
                    auto [it, inserted] = inner_parents.emplace(child, Parents());
                    it->second.emplace(node);

                    if (inserted)
                        stack.push(child);
                }
            }
        }
    }

    struct Frame
    {
        const ActionsDAG::Node * node;
        ActionsDAG::NodeRawConstPtrs mapped_children;
    };

    MatchedTrees::Matches matches;
    std::stack<Frame> stack;

    for (const auto & node : outer_dag.getNodes())
    {
        if (matches.contains(&node))
            continue;

        stack.push(Frame{&node, {}});
        while (!stack.empty())
        {
            auto & frame = stack.top();
            frame.mapped_children.reserve(frame.node->children.size());

            while (frame.mapped_children.size() < frame.node->children.size())
            {
                const auto * child = frame.node->children[frame.mapped_children.size()];
                auto it = matches.find(child);
                if (it == matches.end())
                {
                    /// If match map does not contain a child, it was not visited.
                    stack.push(Frame{child, {}});
                    break;
                }
                /// A node from found match may be nullptr.
                /// It means that node is visited, but no match was found.
                if (it->second.monotonicity)
                    /// Ignore a match with monotonicity.
                    frame.mapped_children.push_back(nullptr);
                else
                    frame.mapped_children.push_back(it->second.node);

            }

            if (frame.mapped_children.size() < frame.node->children.size())
                continue;

            /// Create an empty match for current node.
            /// match.node will be set if match is found.
            auto & match = matches[frame.node];

            if (frame.node->type == ActionsDAG::ActionType::INPUT)
            {
                const ActionsDAG::Node * mapped = nullptr;
                if (auto it = inner_inputs.find(frame.node->result_name); it != inner_inputs.end())
                    mapped = it->second;

                match.node = mapped;
            }
            else if (frame.node->type == ActionsDAG::ActionType::ALIAS)
            {
                match = matches[frame.node->children.at(0)];
            }
            else if (frame.node->type == ActionsDAG::ActionType::FUNCTION)
            {
                //std::cerr << "... Processing " << frame.node->function_base->getName() << std::endl;

                bool found_all_children = true;
                const ActionsDAG::Node * any_child = nullptr;
                size_t num_children = frame.node->children.size();
                for (size_t i = 0; i < num_children; ++i)
                {
                    if (frame.mapped_children[i])
                        any_child = frame.mapped_children[i];
                    else if (!frame.node->children[i]->column || !isColumnConst(*frame.node->children[i]->column))
                        found_all_children = false;
                }

                if (found_all_children && any_child)
                {
                    Parents container;
                    Parents * intersection = &inner_parents[any_child];

                    if (frame.mapped_children.size() > 1)
                    {
                        std::vector<Parents *> other_parents;
                        size_t mapped_children_size = frame.mapped_children.size();
                        other_parents.reserve(mapped_children_size);
                        for (size_t i = 1; i < mapped_children_size; ++i)
                            if (frame.mapped_children[i])
                                other_parents.push_back(&inner_parents[frame.mapped_children[i]]);

                        for (const auto * parent : *intersection)
                        {
                            bool is_common = true;
                            for (const auto * set : other_parents)
                            {
                                if (!set->contains(parent))
                                {
                                    is_common = false;
                                    break;
                                }
                            }

                            if (is_common)
                                container.insert(parent);
                        }

                        intersection = &container;
                    }

                    //std::cerr << ".. Candidate parents " << intersection->size() << std::endl;

                    if (!intersection->empty())
                    {
                        auto func_name = frame.node->function_base->getName();
                        for (const auto * parent : *intersection)
                        {
                            //std::cerr << ".. candidate " << parent->result_name << std::endl;
                            if (parent->type == ActionsDAG::ActionType::FUNCTION && func_name == parent->function_base->getName())
                            {
                                const auto & children = parent->children;
                                if (children.size() == num_children)
                                {
                                    bool all_children_matched = true;
                                    for (size_t i = 0; all_children_matched && i < num_children; ++i)
                                    {
                                        if (frame.mapped_children[i] == nullptr)
                                        {
                                            all_children_matched = children[i]->column && isColumnConst(*children[i]->column)
                                                && children[i]->result_type->equals(*frame.node->children[i]->result_type)
                                                && assert_cast<const ColumnConst &>(*children[i]->column).getField() == assert_cast<const ColumnConst &>(*frame.node->children[i]->column).getField();
                                        }
                                        else
                                            all_children_matched = frame.mapped_children[i] == children[i];
                                    }

                                    if (all_children_matched)
                                    {
                                        match.node = parent;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if (!match.node && check_monotonicity && frame.node->function_base->hasInformationAboutMonotonicity())
                {
                    size_t num_const_args = 0;
                    const ActionsDAG::Node * monotonic_child = nullptr;
                    for (const auto * child : frame.node->children)
                    {
                        if (child->column)
                            ++num_const_args;
                        else
                            monotonic_child = child;
                    }

                    if (monotonic_child && num_const_args + 1 == frame.node->children.size())
                    {
                        const auto & child_match = matches[monotonic_child];
                        if (child_match.node)
                        {
                            auto info = frame.node->function_base->getMonotonicityForRange(*monotonic_child->result_type, {}, {});
                            if (info.is_monotonic)
                            {
                                MatchedTrees::Monotonicity monotonicity;
                                monotonicity.direction *= info.is_positive ? 1 : -1;
                                monotonicity.strict = info.is_strict;
                                monotonicity.child_match = &child_match;
                                monotonicity.child_node = monotonic_child;

                                if (child_match.monotonicity)
                                {
                                    monotonicity.direction *= child_match.monotonicity->direction;
                                    if (!child_match.monotonicity->strict)
                                        monotonicity.strict = false;
                                }

                                match.node = child_match.node;
                                match.monotonicity = monotonicity;
                            }
                        }
                    }
                }
            }

            stack.pop();
        }
    }

    return matches;
}


struct PossiblyMonotonicChain
{
    const ActionsDAG::Node * input_node = nullptr;
    std::vector<size_t> non_const_arg_pos;
    bool changes_order = false;
    bool is_strict = true;
};

/// Build a chain of functions which may be monotonic.
static PossiblyMonotonicChain buildPossiblyMonitinicChain(const ActionsDAG::Node * node)
{
    std::vector<size_t> chain;

    while (node->type != ActionsDAG::ActionType::INPUT)
    {
        if (node->type == ActionsDAG::ActionType::ALIAS)
        {
            node = node->children.at(0);
            continue;
        }

        if (node->type != ActionsDAG::ActionType::FUNCTION)
            break;

        size_t num_children = node->children.size();
        if (num_children == 0)
            break;

        const auto & func = node->function_base;
        if (!func->hasInformationAboutMonotonicity())
            break;

        std::optional<size_t> non_const_arg;
        for (size_t i = 0; i < num_children; ++i)
        {
            const auto * child = node->children[i];
            if (child->type == ActionsDAG::ActionType::COLUMN)
                continue;

            if (non_const_arg != std::nullopt)
            {
                /// Second non-constant arg
                non_const_arg = {};
                break;
            }

            non_const_arg = i;
        }

        if (non_const_arg == std::nullopt)
            break;

        chain.push_back(*non_const_arg);
        node = node->children[*non_const_arg];
    }

    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};

    return {node, std::move(chain)};
}

/// Check whether all the function in chain are monotonic
bool isMonotonicChain(const ActionsDAG::Node * node, PossiblyMonotonicChain & chain)
{
    auto it = chain.non_const_arg_pos.begin();
    while (node != chain.input_node)
    {
        if (node->type != ActionsDAG::ActionType::FUNCTION)
        {
            node = node->children[0];
            continue;
        }

        size_t pos = *it;
        ++it;

        const auto & type = node->children[pos]->result_type;
        const Field field{};
        auto monotonicity = node->function_base->getMonotonicityForRange(*type, field, field);
        if (!monotonicity.is_monotonic)
            break;

        if (!monotonicity.is_positive)
            chain.changes_order = !chain.changes_order;

        chain.is_strict = chain.is_strict && monotonicity.is_strict;

        node = node->children[pos];
    }

    return node == chain.input_node;
}

void applyActionsToSortDescription(
    SortDescription & description,
    const ActionsDAG & dag,
    const ActionsDAG::Node * output_to_skip)
{
    if (description.empty())
        return;

    if (dag.hasArrayJoin())
        return;

    const size_t descr_size = description.size();

    const auto & inputs = dag.getInputs();
    const size_t num_inputs = inputs.size();

    struct SortColumn
    {
        const ActionsDAG::Node * input = nullptr;
        const ActionsDAG::Node * output = nullptr;
        bool is_monotonic_chain = false;
        bool is_strict = true;
        bool changes_order = false;
    };

    std::vector<SortColumn> sort_columns(descr_size);
    std::unordered_map<const ActionsDAG::Node *, size_t> input_to_sort_column;

    {
        std::unordered_map<std::string_view, size_t> desc_name_to_pos;
        for (size_t pos = 0; pos < descr_size; ++pos)
            desc_name_to_pos.emplace(description[pos].column_name, pos);

        for (size_t pos = 0; pos < num_inputs; ++pos)
        {
            auto it = desc_name_to_pos.find(inputs[pos]->result_name);
            if (it != desc_name_to_pos.end() && !sort_columns[it->second].input)
            {
                sort_columns[it->second].input = inputs[pos];
                input_to_sort_column[inputs[pos]] = it->second;
            }
        }
    }

    for (const auto * output : dag.getOutputs())
    {
        if (output == output_to_skip)
            continue;

        auto chain = buildPossiblyMonitinicChain(output);
        if (!chain.input_node)
            break;

        auto it = input_to_sort_column.find(chain.input_node);
        if (it == input_to_sort_column.end())
            break;

        SortColumn & sort_column = sort_columns[it->second];

        /// Already found better chain
        bool has_functions = !chain.non_const_arg_pos.empty();
        bool is_monotonicity_improved = !has_functions && sort_column.is_monotonic_chain;
        if (sort_column.output && !is_monotonicity_improved && sort_column.is_strict)
            break;

        if (has_functions && !isMonotonicChain(output, chain))
            break;

        bool is_strictness_improved = chain.is_strict && !sort_column.is_strict;
        if (sort_column.output && !is_strictness_improved)
            break;

        sort_column.output = output;
        sort_column.is_monotonic_chain = has_functions;
        sort_column.changes_order = chain.changes_order;
        sort_column.is_strict = chain.is_strict;
    }

    size_t prefix_size = 0;
    while (prefix_size < descr_size)
    {
        const auto & sort_colunm = sort_columns[prefix_size];

        /// No input is allowed : it means DAG did not use the column.
        if (sort_colunm.input && !sort_colunm.output)
            break;

        auto & descr = description[prefix_size];
        ++prefix_size;

        if (sort_colunm.output)
            descr.column_name = sort_colunm.output->result_name;

        if (sort_colunm.changes_order)
            descr.direction *= -1;

        if (!sort_colunm.is_strict)
            break;
    }

    description.resize(prefix_size);
}

std::optional<std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *>> resolveMatchedInputs(
    const MatchedTrees::Matches & matches,
    const std::unordered_set<const ActionsDAG::Node *> & allowed_inputs,
    const ActionsDAG::NodeRawConstPtrs & nodes)
{
    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> new_inputs;

    for (const auto * node : nodes)
    {
        if (visited.contains(node))
            continue;

        stack.push({.node = node});

        while (!stack.empty())
        {
            auto & frame = stack.top();

            if (frame.next_child_to_visit == 0)
            {
                auto jt = matches.find(frame.node);
                if (jt != matches.end())
                {
                    const auto & match = jt->second;
                    if (match.node && !match.monotonicity && allowed_inputs.contains(match.node))
                    {
                        visited.insert(frame.node);
                        new_inputs[frame.node] = match.node;
                        stack.pop();
                        continue;
                    }
                }
            }

            if (frame.next_child_to_visit < frame.node->children.size())
            {
                stack.push({.node = frame.node->children[frame.next_child_to_visit]});
                ++frame.next_child_to_visit;
                continue;
            }

            /// Not a match and there is no matched child.
            if (frame.node->type == ActionsDAG::ActionType::INPUT)
                return std::nullopt;

            /// Not a match, but all children matched.
            visited.insert(frame.node);
            stack.pop();
        }
    }

    return new_inputs;
}

bool isInjectiveFunction(const ActionsDAG::Node * node)
{
    if (node->function_base->isInjective({}))
        return true;

    size_t fixed_args = 0;
    for (const auto & child : node->children)
        if (child->type == ActionsDAG::ActionType::COLUMN)
            ++fixed_args;
    static const std::vector<String> injective = {"plus", "minus", "negate", "tuple"};
    return (fixed_args + 1 >= node->children.size()) && (std::ranges::find(injective, node->function_base->getName()) != injective.end());
}

NodeSet removeInjectiveFunctionsFromResultsRecursively(const ActionsDAG & actions)
{
    NodeSet irreducible;
    NodeSet visited;
    for (const auto & node : actions.getOutputs())
        removeInjectiveFunctionsFromResultsRecursively(node, irreducible, visited);
    return irreducible;
}

void removeInjectiveFunctionsFromResultsRecursively(const ActionsDAG::Node * node, NodeSet & irreducible, NodeSet & visited)
{
    if (visited.contains(node))
        return;
    visited.insert(node);

    switch (node->type)
    {
        case ActionsDAG::ActionType::ALIAS:
            assert(node->children.size() == 1);
            removeInjectiveFunctionsFromResultsRecursively(node->children.at(0), irreducible, visited);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            UNREACHABLE();
        case ActionsDAG::ActionType::COLUMN:
            irreducible.insert(node);
            break;
        case ActionsDAG::ActionType::FUNCTION:
            if (!isInjectiveFunction(node))
            {
                irreducible.insert(node);
            }
            else
            {
                for (const auto & child : node->children)
                    removeInjectiveFunctionsFromResultsRecursively(child, irreducible, visited);
            }
            break;
        case ActionsDAG::ActionType::INPUT:
            irreducible.insert(node);
            break;
        case ActionsDAG::ActionType::PLACEHOLDER:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PLACEHOLDER action node must be removed before query plan optimization");
    }
}

bool allOutputsDependsOnlyOnAllowedNodes(
    const NodeSet & irreducible_nodes, const MatchedTrees::Matches & matches, const ActionsDAG::Node * node, NodeMap & visited)
{
    if (visited.contains(node))
        return visited[node];

    bool res = false;
    /// `matches` maps partition key nodes into nodes in group by actions
    if (matches.contains(node))
    {
        const auto & match = matches.at(node);
        /// Function could be mapped into its argument. In this case .monotonicity != std::nullopt (see matchTrees)
        if (match.node && !match.monotonicity)
            res = irreducible_nodes.contains(match.node);
    }

    if (!res)
    {
        switch (node->type)
        {
            case ActionsDAG::ActionType::ALIAS:
                assert(node->children.size() == 1);
                res = allOutputsDependsOnlyOnAllowedNodes(irreducible_nodes, matches, node->children.at(0), visited);
                break;
            case ActionsDAG::ActionType::ARRAY_JOIN:
                UNREACHABLE();
            case ActionsDAG::ActionType::COLUMN:
                /// Constants doesn't matter, so let's always consider them matched.
                res = true;
                break;
            case ActionsDAG::ActionType::FUNCTION:
                res = true;
                for (const auto & child : node->children)
                    res &= allOutputsDependsOnlyOnAllowedNodes(irreducible_nodes, matches, child, visited);
                break;
            case ActionsDAG::ActionType::INPUT:
                break;
            case ActionsDAG::ActionType::PLACEHOLDER:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "PLACEHOLDER action node must be removed before query plan optimization");
        }
    }
    visited[node] = res;
    return res;
}

/// Here we check that partition key expression is a deterministic function of the reduced set of group by key nodes.
/// No need to explicitly check that each function is deterministic, because it is a guaranteed property of partition key expression (checked on table creation).
/// So it is left only to check that each output node depends only on the allowed set of nodes (`irreducible_nodes`).
bool allOutputsDependsOnlyOnAllowedNodes(
    const ActionsDAG & partition_actions, const NodeSet & irreducible_nodes, const MatchedTrees::Matches & matches)
{
    NodeMap visited;
    bool res = true;
    for (const auto & node : partition_actions.getOutputs())
        if (node->type != ActionsDAG::ActionType::INPUT)
            res &= allOutputsDependsOnlyOnAllowedNodes(irreducible_nodes, matches, node, visited);
    return res;
}

}
