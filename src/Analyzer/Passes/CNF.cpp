#include <Analyzer/Passes/CNF.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>

#include <Interpreters/TreeCNFConverter.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Functions/FunctionFactory.h>
#include <Functions/logical.h>

#include <Common/checkStackSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_TEMPORARY_COLUMNS;
}

namespace Analyzer
{

namespace
{

bool isLogicalFunction(const FunctionNode & function_node)
{
    const std::string_view name = function_node.getFunctionName();
    return name == "and" || name == "or" || name == "not";
}

template <typename... Args>
QueryTreeNodePtr createFunctionNode(const FunctionOverloadResolverPtr & function_resolver, Args &&... args)
{
    auto function_node = std::make_shared<FunctionNode>(function_resolver->getName());
    auto & new_arguments = function_node->getArguments().getNodes();
    new_arguments.reserve(sizeof...(args));
    (new_arguments.push_back(std::forward<Args>(args)), ...);
    function_node->resolveAsFunction(function_resolver);
    return function_node;
}

size_t countAtoms(const QueryTreeNodePtr & node)
{
    checkStackSize();

    const auto * function_node = node->as<FunctionNode>();
    if (!function_node || !isLogicalFunction(*function_node))
        return 1;

    size_t atom_count = 0;
    const auto & arguments = function_node->getArguments().getNodes();
    for (const auto & argument : arguments)
        atom_count += countAtoms(argument);

    return atom_count;
}

class SplitMultiLogicVisitor
{
public:
    explicit SplitMultiLogicVisitor(ContextPtr context)
        : current_context(std::move(context))
    {}

    void visit(QueryTreeNodePtr & node)
    {
        checkStackSize();

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isLogicalFunction(*function_node))
            return;

        const auto & name = function_node->getFunctionName();

        if (name == "and" || name == "or")
        {
            auto function_resolver = name == "and" ? createInternalFunctionAndOverloadResolver() : createInternalFunctionOrOverloadResolver();

            const auto & arguments = function_node->getArguments().getNodes();
            if (arguments.size() > 2)
            {
                QueryTreeNodePtr current = arguments[0];
                for (size_t i = 1; i < arguments.size(); ++i)
                    current = createFunctionNode(function_resolver, std::move(current), arguments[i]);

                auto & new_function_node = current->as<FunctionNode &>();
                function_node->getArguments().getNodes() = std::move(new_function_node.getArguments().getNodes());
                function_node->resolveAsFunction(function_resolver);
            }
        }
        else
        {
            assert(name == "not");
        }

        auto & arguments = function_node->getArguments().getNodes();
        for (auto & argument : arguments)
            visit(argument);
    }

private:
    ContextPtr current_context;
};

class PushNotVisitor
{
public:
    explicit PushNotVisitor()
        : not_function_resolver(createInternalFunctionNotOverloadResolver())
        , or_function_resolver(createInternalFunctionOrOverloadResolver())
        , and_function_resolver(createInternalFunctionAndOverloadResolver())
    {}

    void visit(QueryTreeNodePtr & node, bool add_negation)
    {
        checkStackSize();

        auto * function_node = node->as<FunctionNode>();

        if (!function_node || !isLogicalFunction(*function_node))
        {
            if (add_negation)
                node = createFunctionNode(not_function_resolver, std::move(node));
            return;
        }

        std::string_view function_name = function_node->getFunctionName();
        if (function_name == "and" || function_name == "or")
        {
            if (add_negation)
            {
                if (function_name == "and")
                    function_node->resolveAsFunction(or_function_resolver);
                else
                    function_node->resolveAsFunction(and_function_resolver);
            }

            auto & arguments = function_node->getArguments().getNodes();
            for (auto & argument : arguments)
                visit(argument, add_negation);
            return;
        }

        assert(function_name == "not");
        auto & arguments = function_node->getArguments().getNodes();
        assert(arguments.size() == 1);
        node = arguments[0];
        visit(node, !add_negation);
    }

private:
    const FunctionOverloadResolverPtr not_function_resolver;
    const FunctionOverloadResolverPtr or_function_resolver;
    const FunctionOverloadResolverPtr and_function_resolver;
};

class PushOrVisitor
{
public:
    explicit PushOrVisitor(size_t max_atoms_)
        : max_atoms(max_atoms_)
        , and_resolver(createInternalFunctionAndOverloadResolver())
        , or_resolver(createInternalFunctionOrOverloadResolver())
    {}

    bool visit(QueryTreeNodePtr & node, size_t num_atoms)
    {
        if (max_atoms && num_atoms > max_atoms)
            return false;

        checkStackSize();

        auto * function_node = node->as<FunctionNode>();

        if (!function_node)
            return true;

        std::string_view name = function_node->getFunctionName();

        if (name == "or" || name == "and")
        {
            auto & arguments = function_node->getArguments().getNodes();
            for (auto & argument : arguments)
            {
                if (!visit(argument, num_atoms))
                    return false;
            }
        }

        if (name == "or")
        {
            auto & arguments = function_node->getArguments().getNodes();
            assert(arguments.size() == 2);

            size_t and_node_id = arguments.size();

            for (size_t i = 0; i < arguments.size(); ++i)
            {
                auto & argument = arguments[i];
                if (auto * argument_function_node = argument->as<FunctionNode>();
                    argument_function_node && argument_function_node->getFunctionName() == "and")
                    and_node_id = i;
            }

            if (and_node_id == arguments.size())
                return true;

            auto & other_node = arguments[1 - and_node_id];
            auto & and_function_arguments = arguments[and_node_id]->as<FunctionNode &>().getArguments().getNodes();

            auto lhs = createFunctionNode(or_resolver, other_node->clone(), std::move(and_function_arguments[0]));
            num_atoms += countAtoms(other_node);

            auto rhs = createFunctionNode(or_resolver, std::move(other_node), std::move(and_function_arguments[1]));
            node = createFunctionNode(and_resolver, std::move(lhs), std::move(rhs));

            return visit(node, num_atoms);
        }

        return true;
    }

private:
    size_t max_atoms;

    const FunctionOverloadResolverPtr and_resolver;
    const FunctionOverloadResolverPtr or_resolver;
};

class CollectGroupsVisitor
{
public:
    void visit(QueryTreeNodePtr & node)
    {
        CNF::OrGroup or_group;
        visitImpl(node, or_group);
        if (!or_group.empty())
            and_group.insert(std::move(or_group));
    }

    CNF::AndGroup and_group;

private:
    void visitImpl(QueryTreeNodePtr & node, CNF::OrGroup & or_group)
    {
        checkStackSize();

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isLogicalFunction(*function_node))
        {
            or_group.insert(CNF::AtomicFormula{false, std::move(node)});
            return;
        }

        std::string_view name = function_node->getFunctionName();

        if (name == "and")
        {
            auto & arguments = function_node->getArguments().getNodes();
            for (auto & argument : arguments)
            {
                CNF::OrGroup argument_or_group;
                visitImpl(argument, argument_or_group);
                if (!argument_or_group.empty())
                    and_group.insert(std::move(argument_or_group));
            }
        }
        else if (name == "or")
        {
            auto & arguments = function_node->getArguments().getNodes();
            for (auto & argument : arguments)
                visitImpl(argument, or_group);
        }
        else
        {
            assert(name == "not");
            auto & arguments = function_node->getArguments().getNodes();
            or_group.insert(CNF::AtomicFormula{true, std::move(arguments[0])});
        }
    }
};

std::optional<CNF::AtomicFormula> tryInvertFunction(
    const CNF::AtomicFormula & atom, const ContextPtr & context, const std::unordered_map<std::string, std::string> & inverse_relations)
{
    auto * function_node = atom.node_with_hash.node->as<FunctionNode>();
    if (!function_node)
        return std::nullopt;

    if (auto it = inverse_relations.find(function_node->getFunctionName()); it != inverse_relations.end())
    {
        auto inverse_function_resolver = FunctionFactory::instance().get(it->second, context);
        function_node->resolveAsFunction(inverse_function_resolver);
        return CNF::AtomicFormula{!atom.negative, atom.node_with_hash.node};
    }

    return std::nullopt;
}
}

bool CNF::AtomicFormula::operator==(const AtomicFormula & rhs) const
{
    return negative == rhs.negative && node_with_hash == rhs.node_with_hash;
}

bool CNF::AtomicFormula::operator<(const AtomicFormula & rhs) const
{
    if (node_with_hash.hash > rhs.node_with_hash.hash)
        return false;

    return node_with_hash.hash < rhs.node_with_hash.hash || negative < rhs.negative;
}

std::string CNF::dump() const
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
                res << " NOT ";
            res << atom.node_with_hash.node->formatASTForErrorMessage();
        }
        res << ")";
    }

    return res.str();
}

CNF & CNF::transformGroups(std::function<OrGroup(const OrGroup &)> fn)
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

CNF & CNF::transformAtoms(std::function<AtomicFormula(const AtomicFormula &)> fn)
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

CNF & CNF::pushNotIntoFunctions(const ContextPtr & context)
{
    transformAtoms([&](const AtomicFormula & atom)
    {
        return pushNotIntoFunction(atom, context);
    });

    return *this;
}

CNF::AtomicFormula CNF::pushNotIntoFunction(const AtomicFormula & atom, const ContextPtr & context)
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

    if (auto inverted_atom = tryInvertFunction(atom, context, inverse_relations);
        inverted_atom.has_value())
        return std::move(*inverted_atom);

    return atom;
}

CNF & CNF::pullNotOutFunctions(const ContextPtr & context)
{
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

        if (auto inverted_atom = tryInvertFunction(atom, context, inverse_relations);
            inverted_atom.has_value())
            return std::move(*inverted_atom);

        return atom;
    });

    return *this;
}

CNF & CNF::filterAlwaysTrueGroups(std::function<bool(const OrGroup &)> predicate)
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

CNF & CNF::filterAlwaysFalseAtoms(std::function<bool(const AtomicFormula &)> predicate)
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
            filtered.insert(std::move(filtered_group));
        else
        {
            filtered.clear();
            filtered_group.insert(AtomicFormula{false, QueryTreeNodePtrWithHash{std::make_shared<ConstantNode>(static_cast<UInt8>(0))}});
            filtered.insert(std::move(filtered_group));
            break;
        }
    }

    statements = std::move(filtered);
    return *this;
}

CNF & CNF::reduce()
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

void CNF::appendGroup(const AndGroup & and_group)
{
    for (const auto & or_group : and_group)
        statements.emplace(or_group);
}

CNF::CNF(AndGroup statements_)
    : statements(std::move(statements_))
{}

std::optional<CNF> CNF::tryBuildCNF(const QueryTreeNodePtr & node, ContextPtr context, size_t max_growth_multiplier)
{
    auto node_cloned = node->clone();

    size_t atom_count = countAtoms(node_cloned);
    size_t max_atoms = max_growth_multiplier ? std::max(MAX_ATOMS_WITHOUT_CHECK, atom_count * max_growth_multiplier) : 0;

    {
        SplitMultiLogicVisitor visitor(context);
        visitor.visit(node_cloned);
    }

    {
        PushNotVisitor visitor;
        visitor.visit(node_cloned, false);
    }

    if (PushOrVisitor visitor(max_atoms);
        !visitor.visit(node_cloned, atom_count))
            return std::nullopt;

    CollectGroupsVisitor collect_visitor;
    collect_visitor.visit(node_cloned);

    if (collect_visitor.and_group.empty())
        return std::nullopt;

    return CNF{std::move(collect_visitor.and_group)};
}

CNF CNF::toCNF(const QueryTreeNodePtr & node, ContextPtr context, size_t max_growth_multiplier)
{
    auto cnf = tryBuildCNF(node, context, max_growth_multiplier);
    if (!cnf)
        throw Exception(ErrorCodes::TOO_MANY_TEMPORARY_COLUMNS,
            "Cannot convert expression '{}' to CNF, because it produces to many clauses."
            "Size of boolean formula in CNF can be exponential of size of source formula.",
            node->formatConvertedASTForErrorMessage());

    return *cnf;
}

QueryTreeNodePtr CNF::toQueryTree() const
{
    if (statements.empty())
        return nullptr;

    QueryTreeNodes and_arguments;
    and_arguments.reserve(statements.size());

    auto not_resolver = createInternalFunctionNotOverloadResolver();
    auto or_resolver = createInternalFunctionOrOverloadResolver();
    auto and_resolver = createInternalFunctionAndOverloadResolver();

    const auto function_node_from_atom = [&](const auto & atom) -> QueryTreeNodePtr
    {
        auto cloned_node = atom.node_with_hash.node->clone();
        if (atom.negative)
            return createFunctionNode(not_resolver, std::move(cloned_node));

        return std::move(cloned_node);
    };

    for (const auto & or_group : statements)
    {
        if (or_group.size() == 1)
        {
            const auto & atom = *or_group.begin();
            and_arguments.push_back(function_node_from_atom(atom));
        }
        else
        {
            QueryTreeNodes or_arguments;
            or_arguments.reserve(or_group.size());

            for (const auto & atom : or_group)
                or_arguments.push_back(function_node_from_atom(atom));

            auto or_function = std::make_shared<FunctionNode>("or");
            or_function->getArguments().getNodes() = std::move(or_arguments);
            or_function->resolveAsFunction(or_resolver);

            and_arguments.push_back(std::move(or_function));
        }
    }

    if (and_arguments.size() == 1)
        return std::move(and_arguments[0]);

    auto and_function = std::make_shared<FunctionNode>("and");
    and_function->getArguments().getNodes() = std::move(and_arguments);
    and_function->resolveAsFunction(and_resolver);

    return and_function;
}

}

}
