#include <Interpreters/ActionsDAGCNF.h>

#include <Functions/FunctionFactory.h>
#include <Functions/logical.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Common/checkStackSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_TEMPORARY_COLUMNS;
}

namespace
{

bool isLogicalFunction(const ActionsDAG::Node * node)
{
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    if (!node->function_base)
        return false;

    const std::string & name = node->function_base->getName();
    return name == "and" || name == "or" || name == "not";
}

size_t countAtoms(const ActionsDAG::Node * node)
{
    checkStackSize();

    if (!isLogicalFunction(node))
        return 1;

    size_t atom_count = 0;
    for (const auto * child : node->children)
        atom_count += countAtoms(child);

    return atom_count;
}

/// Clone a node and its dependencies into a new DAG
const ActionsDAG::Node * cloneNodeIntoDAG(const ActionsDAG::Node * node, ActionsDAG & dag, std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> & cloned_nodes)
{
    checkStackSize();

    if (auto it = cloned_nodes.find(node); it != cloned_nodes.end())
        return it->second;

    const ActionsDAG::Node * result = nullptr;

    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
        {
            result = &dag.addInput({node->column, node->result_type, node->result_name});
            break;
        }
        case ActionsDAG::ActionType::COLUMN:
        {
            result = &dag.addColumn({node->column, node->result_type, node->result_name});
            break;
        }
        case ActionsDAG::ActionType::ALIAS:
        {
            const auto * child = cloneNodeIntoDAG(node->children[0], dag, cloned_nodes);
            result = &dag.addAlias(*child, node->result_name);
            break;
        }
        case ActionsDAG::ActionType::ARRAY_JOIN:
        {
            const auto * child = cloneNodeIntoDAG(node->children[0], dag, cloned_nodes);
            result = &dag.addArrayJoin(*child, node->result_name);
            break;
        }
        case ActionsDAG::ActionType::FUNCTION:
        {
            ActionsDAG::NodeRawConstPtrs children;
            children.reserve(node->children.size());
            for (const auto * child : node->children)
                children.push_back(cloneNodeIntoDAG(child, dag, cloned_nodes));

            /// Recreate the function using FunctionFactory to ensure it's properly initialized
            if (node->function_base)
            {
                auto function_resolver = FunctionFactory::instance().get(node->function_base->getName(), nullptr);
                result = &dag.addFunction(function_resolver, children, node->result_name);
            }
            else
            {
                result = &dag.addFunction(node->function_base, children, node->result_name);
            }
            break;
        }
        case ActionsDAG::ActionType::PLACEHOLDER:
        {
            result = &dag.addPlaceholder(node->result_name, node->result_type);
            break;
        }
    }

    cloned_nodes[node] = result;
    return result;
}

/// Split multi-argument AND/OR into binary operations
class SplitMultiLogicVisitor
{
public:
    explicit SplitMultiLogicVisitor(ActionsDAG & dag_, ContextPtr context_)
        : dag(dag_), context(std::move(context_))
    {}

    const ActionsDAG::Node * visit(const ActionsDAG::Node * node)
    {
        checkStackSize();

        if (!isLogicalFunction(node))
            return node;

        const auto & name = node->function_base->getName();

        if (name == "and" || name == "or")
        {
            ActionsDAG::NodeRawConstPtrs visited_children;
            visited_children.reserve(node->children.size());
            for (const auto * child : node->children)
                visited_children.push_back(visit(child));

            if (visited_children.size() > 2)
            {
                auto function_resolver = name == "and"
                    ? createInternalFunctionAndOverloadResolver()
                    : createInternalFunctionOrOverloadResolver();

                const ActionsDAG::Node * current = visited_children[0];
                for (size_t i = 1; i < visited_children.size(); ++i)
                {
                    ActionsDAG::NodeRawConstPtrs args = {current, visited_children[i]};
                    current = &dag.addFunction(function_resolver, args, "");
                }
                return current;
            }
            else if (visited_children.size() == 2)
            {
                // Already binary, just return with visited children
                auto function_resolver = name == "and"
                    ? createInternalFunctionAndOverloadResolver()
                    : createInternalFunctionOrOverloadResolver();
                return &dag.addFunction(function_resolver, visited_children, "");
            }

            return node;
        }
        else if (name == "not")
        {
            const auto * child = visit(node->children[0]);
            auto function_resolver = createInternalFunctionNotOverloadResolver();
            ActionsDAG::NodeRawConstPtrs args = {child};
            return &dag.addFunction(function_resolver, args, "");
        }

        return node;
    }

private:
    ActionsDAG & dag;
    ContextPtr context;
};

/// Push NOT operations down to leaf nodes using De Morgan's laws
class PushNotVisitor
{
public:
    explicit PushNotVisitor(ActionsDAG & dag_)
        : dag(dag_)
        , not_function_resolver(createInternalFunctionNotOverloadResolver())
        , or_function_resolver(createInternalFunctionOrOverloadResolver())
        , and_function_resolver(createInternalFunctionAndOverloadResolver())
    {}

    const ActionsDAG::Node * visit(const ActionsDAG::Node * node, bool add_negation)
    {
        checkStackSize();

        if (!isLogicalFunction(node))
        {
            if (add_negation)
            {
                ActionsDAG::NodeRawConstPtrs args = {node};
                return &dag.addFunction(not_function_resolver, args, "");
            }
            return node;
        }

        const std::string & function_name = node->function_base->getName();

        if (function_name == "and" || function_name == "or")
        {
            auto function_resolver = function_name == "and" ? and_function_resolver : or_function_resolver;

            if (add_negation)
            {
                // De Morgan's law: NOT(a AND b) = NOT(a) OR NOT(b)
                //                  NOT(a OR b) = NOT(a) AND NOT(b)
                function_resolver = function_name == "and" ? or_function_resolver : and_function_resolver;
            }

            ActionsDAG::NodeRawConstPtrs new_children;
            new_children.reserve(node->children.size());
            for (const auto * child : node->children)
                new_children.push_back(visit(child, add_negation));

            return &dag.addFunction(function_resolver, new_children, "");
        }

        if (function_name == "not")
        {
            return visit(node->children[0], !add_negation);
        }

        return node;
    }

private:
    ActionsDAG & dag;
    const FunctionOverloadResolverPtr not_function_resolver;
    const FunctionOverloadResolverPtr or_function_resolver;
    const FunctionOverloadResolverPtr and_function_resolver;
};

/// Push OR operations down using distributive law: a OR (b AND c) = (a OR b) AND (a OR c)
class PushOrVisitor
{
public:
    explicit PushOrVisitor(ActionsDAG & dag_, size_t max_atoms_)
        : dag(dag_)
        , max_atoms(max_atoms_)
        , and_resolver(createInternalFunctionAndOverloadResolver())
        , or_resolver(createInternalFunctionOrOverloadResolver())
    {}

    bool visit(const ActionsDAG::Node *& node, size_t num_atoms)
    {
        if (max_atoms && num_atoms > max_atoms)
            return false;

        checkStackSize();

        if (!node || node->type != ActionsDAG::ActionType::FUNCTION)
            return true;

        const std::string & name = node->function_base->getName();

        if (name == "or" || name == "and")
        {
            ActionsDAG::NodeRawConstPtrs new_children;
            new_children.reserve(node->children.size());
            for (const auto * child : node->children)
            {
                const ActionsDAG::Node * visited = child;
                if (!visit(visited, num_atoms))
                    return false;
                new_children.push_back(visited);
            }

            if (name == "and")
            {
                node = &dag.addFunction(and_resolver, new_children, "");
                return true;
            }
        }

        if (name == "or")
        {
            // Find if any child is an AND node
            size_t and_node_id = node->children.size();
            for (size_t i = 0; i < node->children.size(); ++i)
            {
                const auto * child = node->children[i];
                if (child->type == ActionsDAG::ActionType::FUNCTION &&
                    child->function_base &&
                    child->function_base->getName() == "and")
                {
                    and_node_id = i;
                    break;
                }
            }

            if (and_node_id == node->children.size())
                return true;

            // Apply distributive law: a OR (b AND c) = (a OR b) AND (a OR c)
            const auto * other_node = node->children[1 - and_node_id];
            const auto * and_node = node->children[and_node_id];

            ActionsDAG::NodeRawConstPtrs lhs_args = {other_node, and_node->children[0]};
            const ActionsDAG::Node * lhs = &dag.addFunction(or_resolver, lhs_args, "");
            num_atoms += countAtoms(other_node);

            ActionsDAG::NodeRawConstPtrs rhs_args = {other_node, and_node->children[1]};
            const ActionsDAG::Node * rhs = &dag.addFunction(or_resolver, rhs_args, "");

            ActionsDAG::NodeRawConstPtrs and_args = {lhs, rhs};
            node = &dag.addFunction(and_resolver, and_args, "");

            return visit(node, num_atoms);
        }

        return true;
    }

private:
    ActionsDAG & dag;
    size_t max_atoms;
    const FunctionOverloadResolverPtr and_resolver;
    const FunctionOverloadResolverPtr or_resolver;
};

/// Collect atomic formulas into CNF structure
class CollectGroupsVisitor
{
public:
    explicit CollectGroupsVisitor() = default;

    void visit(const ActionsDAG::Node * node)
    {
        ActionsDAGCNF::OrGroup or_group;
        visitImpl(node, or_group);
        if (!or_group.empty())
            and_group.insert(std::move(or_group));
    }

    ActionsDAGCNF::AndGroup and_group;

private:
    void visitImpl(const ActionsDAG::Node * node, ActionsDAGCNF::OrGroup & or_group)
    {
        checkStackSize();

        if (!isLogicalFunction(node))
        {
            or_group.insert(ActionsDAGCNF::AtomicFormula{false, ActionsDAGNodeWithHash{node}});
            return;
        }

        const std::string & name = node->function_base->getName();

        if (name == "and")
        {
            for (const auto * child : node->children)
            {
                ActionsDAGCNF::OrGroup child_or_group;
                visitImpl(child, child_or_group);
                if (!child_or_group.empty())
                    and_group.insert(std::move(child_or_group));
            }
        }
        else if (name == "or")
        {
            for (const auto * child : node->children)
                visitImpl(child, or_group);
        }
        else if (name == "not")
        {
            or_group.insert(ActionsDAGCNF::AtomicFormula{true, ActionsDAGNodeWithHash{node->children[0]}});
        }
    }
};

std::optional<ActionsDAGCNF::AtomicFormula> tryInvertFunction(
    const ActionsDAGCNF::AtomicFormula & atom,
    ActionsDAG & dag,
    ContextPtr context,
    const std::unordered_map<std::string, std::string> & inverse_relations)
{
    const auto * node = atom.node_with_hash.node;
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return std::nullopt;

    const std::string & func_name = node->function_base->getName();
    auto it = inverse_relations.find(func_name);
    if (it == inverse_relations.end())
        return std::nullopt;

    auto inverse_function_resolver = FunctionFactory::instance().get(it->second, context);
    const auto * new_node = &dag.addFunction(inverse_function_resolver, node->children, "");

    return ActionsDAGCNF::AtomicFormula{!atom.negative, ActionsDAGNodeWithHash{new_node}};
}

} // anonymous namespace

ActionsDAGCNF::ActionsDAGCNF(AndGroup statements_)
    : statements(std::move(statements_))
    , owned_dag(nullptr)
{}

ActionsDAGCNF::ActionsDAGCNF(AndGroup statements_, std::shared_ptr<ActionsDAG> dag_)
    : statements(std::move(statements_))
    , owned_dag(std::move(dag_))
{}

std::optional<ActionsDAGCNF> ActionsDAGCNF::tryBuildCNF(
    const ActionsDAG::Node * node,
    ContextPtr context,
    size_t max_growth_multiplier)
{
    if (!node)
        return std::nullopt;

    // Create a temporary DAG for transformations
    ActionsDAG temp_dag;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> cloned_nodes;
    const auto * cloned_node = cloneNodeIntoDAG(node, temp_dag, cloned_nodes);

    size_t atom_count = countAtoms(cloned_node);
    size_t max_atoms = max_growth_multiplier
        ? std::max(MAX_ATOMS_WITHOUT_CHECK, atom_count * max_growth_multiplier)
        : 0;

    // Step 1: Split multi-argument AND/OR into binary operations
    {
        SplitMultiLogicVisitor visitor(temp_dag, context);
        cloned_node = visitor.visit(cloned_node);
    }

    // Step 2: Push NOT operations down
    {
        PushNotVisitor visitor(temp_dag);
        cloned_node = visitor.visit(cloned_node, false);
    }

    // Step 3: Push OR operations down (convert to CNF)
    {
        PushOrVisitor visitor(temp_dag, max_atoms);
        if (!visitor.visit(cloned_node, atom_count))
            return std::nullopt;
    }

    // Step 4: Collect atomic formulas into CNF structure
    CollectGroupsVisitor collect_visitor;
    collect_visitor.visit(cloned_node);

    if (collect_visitor.and_group.empty())
        return std::nullopt;

    // Move temp_dag into a shared_ptr to keep nodes alive
    auto owned_dag = std::make_shared<ActionsDAG>(std::move(temp_dag));
    return ActionsDAGCNF(std::move(collect_visitor.and_group), owned_dag);
}

ActionsDAGCNF ActionsDAGCNF::toCNF(
    const ActionsDAG::Node * node,
    ContextPtr context,
    size_t max_growth_multiplier)
{
    auto cnf = tryBuildCNF(node, context, max_growth_multiplier);
    if (!cnf)
        throw Exception(ErrorCodes::TOO_MANY_TEMPORARY_COLUMNS,
            "Cannot convert ActionsDAG node to CNF, because it produces too many clauses. "
            "Size of boolean formula in CNF can be exponential of size of source formula.");

    return *cnf;
}

ActionsDAG ActionsDAGCNF::toActionsDAG(ContextPtr context) const
{
    ActionsDAG dag;
    const auto * node = toCNFNode(dag, context);
    if (node)
        dag.getOutputs().push_back(node);
    return dag;
}

const ActionsDAG::Node * ActionsDAGCNF::toCNFNode(ActionsDAG & dag, ContextPtr /* context */) const
{
    if (statements.empty())
        return nullptr;

    auto not_resolver = createInternalFunctionNotOverloadResolver();
    auto or_resolver = createInternalFunctionOrOverloadResolver();
    auto and_resolver = createInternalFunctionAndOverloadResolver();

    // Helper to convert atomic formula to node
    auto atom_to_node = [&](const AtomicFormula & atom) -> const ActionsDAG::Node *
    {
        std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> cloned;
        const auto * cloned_node = cloneNodeIntoDAG(atom.node_with_hash.node, dag, cloned);

        if (atom.negative)
        {
            ActionsDAG::NodeRawConstPtrs args = {cloned_node};
            return &dag.addFunction(not_resolver, args, "");
        }

        return cloned_node;
    };

    ActionsDAG::NodeRawConstPtrs and_arguments;
    and_arguments.reserve(statements.size());

    for (const auto & or_group : statements)
    {
        if (or_group.size() == 1)
        {
            and_arguments.push_back(atom_to_node(*or_group.begin()));
        }
        else
        {
            ActionsDAG::NodeRawConstPtrs or_arguments;
            or_arguments.reserve(or_group.size());

            for (const auto & atom : or_group)
                or_arguments.push_back(atom_to_node(atom));

            and_arguments.push_back(&dag.addFunction(or_resolver, or_arguments, ""));
        }
    }

    if (and_arguments.size() == 1)
        return and_arguments[0];

    return &dag.addFunction(and_resolver, and_arguments, "");
}

std::string ActionsDAGCNF::dump() const
{
    WriteBufferFromOwnString res;
    bool first = true;
    for (const auto & group : statements)
    {
        if (!first)
            res << " AND ";
        first = false;
        res << "(";
        bool first_in_group = true;
        for (const auto & atom : group)
        {
            if (!first_in_group)
                res << " OR ";
            first_in_group = false;
            if (atom.negative)
                res << "NOT ";
            if (atom.node_with_hash.node)
                res << atom.node_with_hash.node->result_name;
            else
                res << "<null>";
        }
        res << ")";
    }

    return res.str();
}

ActionsDAGCNF & ActionsDAGCNF::transformGroups(std::function<OrGroup(const OrGroup &)> fn)
{
    AndGroup result;

    for (const auto & group : statements)
    {
        auto new_group = fn(group);
        if (!new_group.empty())
            result.insert(std::move(new_group));
    }

    statements = std::move(result);
    return *this;
}

ActionsDAGCNF & ActionsDAGCNF::transformAtoms(std::function<AtomicFormula(const AtomicFormula &)> fn)
{
    transformGroups([fn](const OrGroup & group)
    {
        OrGroup result;
        for (const auto & atom : group)
        {
            auto new_atom = fn(atom);
            if (new_atom.node_with_hash.node)
                result.insert(std::move(new_atom));
        }
        return result;
    });

    return *this;
}

ActionsDAGCNF & ActionsDAGCNF::pushNotIntoFunctions(ContextPtr context)
{
    ActionsDAG temp_dag;

    transformAtoms([&](const AtomicFormula & atom)
    {
        return pushNotIntoFunction(atom, temp_dag, context);
    });

    return *this;
}

ActionsDAGCNF::AtomicFormula ActionsDAGCNF::pushNotIntoFunction(
    const AtomicFormula & atom,
    ActionsDAG & dag,
    ContextPtr context)
{
    if (!atom.negative)
        return atom;

    static const std::unordered_map<std::string, std::string> inverse_relations = {
        {"equals", "notEquals"},
        {"less", "greaterOrEquals"},
        {"lessOrEquals", "greater"},
        {"in", "notIn"},
        {"like", "notLike"},
        {"empty", "notEmpty"},
        {"notEquals", "equals"},
        {"greaterOrEquals", "less"},
        {"greater", "lessOrEquals"},
        {"notIn", "in"},
        {"notLike", "like"},
        {"notEmpty", "empty"},
    };

    if (auto inverted_atom = tryInvertFunction(atom, dag, context, inverse_relations))
        return std::move(*inverted_atom);

    return atom;
}

ActionsDAGCNF & ActionsDAGCNF::pullNotOutFunctions(ContextPtr context)
{
    /// Ensure we have an owned DAG for new nodes
    if (!owned_dag)
        owned_dag = std::make_shared<ActionsDAG>();

    transformAtoms([&](const AtomicFormula & atom)
    {
        static const std::unordered_map<std::string, std::string> inverse_relations = {
            {"notEquals", "equals"},
            {"greaterOrEquals", "less"},
            {"greater", "lessOrEquals"},
            {"notIn", "in"},
            {"notLike", "like"},
            {"notEmpty", "empty"},
        };

        if (auto inverted_atom = tryInvertFunction(atom, *owned_dag, context, inverse_relations))
            return std::move(*inverted_atom);

        return atom;
    });

    return *this;
}

ActionsDAGCNF & ActionsDAGCNF::filterAlwaysTrueGroups(std::function<bool(const OrGroup &)> predicate)
{
    AndGroup filtered;
    for (const auto & or_group : statements)
    {
        if (predicate(or_group))
            filtered.insert(or_group);
    }

    statements = std::move(filtered);
    return *this;
}

ActionsDAGCNF & ActionsDAGCNF::filterAlwaysFalseAtoms(std::function<bool(const AtomicFormula &)> predicate)
{
    AndGroup filtered;
    for (const auto & or_group : statements)
    {
        OrGroup filtered_group;
        for (const auto & atom : or_group)
        {
            if (predicate(atom))
                filtered_group.insert(atom);
        }

        if (!filtered_group.empty())
        {
            filtered.insert(std::move(filtered_group));
        }
        else
        {
            // All atoms false -> group false -> CNF false
            // Just clear all statements - empty CNF means false
            filtered.clear();
            break;
        }
    }

    statements = std::move(filtered);
    return *this;
}

ActionsDAGCNF & ActionsDAGCNF::reduce()
{
    while (true)
    {
        AndGroup new_statements = reduceOnceCNFStatements(statements);
        if (statements == new_statements)
        {
            statements = filterCNFSubsets(statements);
            return *this;
        }
        statements = new_statements;
    }
}

void ActionsDAGCNF::appendGroup(const AndGroup & and_group)
{
    for (const auto & or_group : and_group)
        statements.emplace(or_group);
}

}
