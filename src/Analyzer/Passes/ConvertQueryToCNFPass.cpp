#include <Analyzer/Passes/ConvertQueryToCNFPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/Passes/CNF.h>
#include <Analyzer/Utils.h>

#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

std::optional<Analyzer::CNF> tryConvertQueryToCNF(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    auto cnf_form = Analyzer::CNF::tryBuildCNF(node, context);
    if (!cnf_form)
        return std::nullopt;

    cnf_form->pushNotIntoFunctions(context);
    return cnf_form;
}

enum class MatchState : uint8_t
{
    FULL_MATCH, /// a = b
    PARTIAL_MATCH, /// a = not b
    NONE,
};

MatchState match(const Analyzer::CNF::AtomicFormula & a, const Analyzer::CNF::AtomicFormula & b)
{
    using enum MatchState;
    if (a.node_with_hash.hash != b.node_with_hash.hash)
        return NONE;

    return a.negative == b.negative ? FULL_MATCH : PARTIAL_MATCH;
}

bool checkIfGroupAlwaysTrueFullMatch(const Analyzer::CNF::OrGroup & group, const ConstraintsDescription::QueryTreeData & query_tree_constraints)
{
    /// We have constraints in CNF.
    /// CNF is always true => Each OR group in CNF is always true.
    /// So, we try to check whether we have al least one OR group from CNF as subset in our group.
    /// If we've found one then our group is always true too.

    const auto & constraints_data = query_tree_constraints.getConstraintData();
    std::vector<size_t> found(constraints_data.size());
    for (size_t i = 0; i < constraints_data.size(); ++i)
        found[i] = constraints_data[i].size();

    for (const auto & atom : group)
    {
        const auto constraint_atom_ids = query_tree_constraints.getAtomIds(atom.node_with_hash);
        if (constraint_atom_ids)
        {
            const auto constraint_atoms = query_tree_constraints.getAtomsById(*constraint_atom_ids);
            for (size_t i = 0; i < constraint_atoms.size(); ++i)
            {
                if (match(constraint_atoms[i], atom) == MatchState::FULL_MATCH)
                {
                    if ((--found[(*constraint_atom_ids)[i].group_id]) == 0)
                        return true;
                }
            }
        }
    }
    return false;
}

bool checkIfGroupAlwaysTrueGraph(const Analyzer::CNF::OrGroup & group, const ComparisonGraph<QueryTreeNodePtr> & graph)
{
    /// We try to find at least one atom that is always true by using comparison graph.
    for (const auto & atom : group)
    {
        const auto * function_node = atom.node_with_hash.node->as<FunctionNode>();
        if (function_node)
        {
            const auto & arguments = function_node->getArguments().getNodes();
            if (arguments.size() == 2)
            {
                const auto expected = ComparisonGraph<QueryTreeNodePtr>::atomToCompareResult(atom);
                if (graph.isAlwaysCompare(expected, arguments[0], arguments[1]))
                    return true;
            }
        }
    }

    return false;
}

bool checkIfAtomAlwaysFalseFullMatch(const Analyzer::CNF::AtomicFormula & atom, const ConstraintsDescription::QueryTreeData & query_tree_constraints)
{
    const auto constraint_atom_ids = query_tree_constraints.getAtomIds(atom.node_with_hash);
    if (constraint_atom_ids)
    {
        for (const auto & constraint_atom : query_tree_constraints.getAtomsById(*constraint_atom_ids))
        {
            const auto match_result = match(constraint_atom, atom);
            if (match_result == MatchState::PARTIAL_MATCH)
                return true;
        }
    }

    return false;
}

bool checkIfAtomAlwaysFalseGraph(const Analyzer::CNF::AtomicFormula & atom, const ComparisonGraph<QueryTreeNodePtr> & graph)
{
    const auto * function_node = atom.node_with_hash.node->as<FunctionNode>();
    if (!function_node)
        return false;

    const auto & arguments = function_node->getArguments().getNodes();
    if (arguments.size() != 2)
        return false;

    /// TODO: special support for !=
    const auto expected = ComparisonGraph<QueryTreeNodePtr>::atomToCompareResult(atom);
    return !graph.isPossibleCompare(expected, arguments[0], arguments[1]);
}

void replaceToConstants(QueryTreeNodePtr & term, const ComparisonGraph<QueryTreeNodePtr> & graph)
{
    const auto equal_constant = graph.getEqualConst(term);
    if (equal_constant)
    {
        term = (*equal_constant)->clone();
        return;
    }

    for (auto & child : term->getChildren())
    {
        if (child)
            replaceToConstants(child, graph);
    }
}

Analyzer::CNF::AtomicFormula replaceTermsToConstants(const Analyzer::CNF::AtomicFormula & atom, const ComparisonGraph<QueryTreeNodePtr> & graph)
{
    auto node = atom.node_with_hash.node->clone();
    replaceToConstants(node, graph);
    return {atom.negative, std::move(node)};
}

StorageSnapshotPtr getStorageSnapshot(const QueryTreeNodePtr & node)
{
    StorageSnapshotPtr storage_snapshot{nullptr};
    if (auto * table_node = node->as<TableNode>())
        return table_node->getStorageSnapshot();
    else if (auto * table_function_node = node->as<TableFunctionNode>())
        return table_function_node->getStorageSnapshot();

    return nullptr;
}

bool onlyIndexColumns(const QueryTreeNodePtr & node, const std::unordered_set<std::string_view> & primary_key_set)
{
    const auto * column_node = node->as<ColumnNode>();
    /// TODO: verify that full name is correct here
    if (column_node && !primary_key_set.contains(column_node->getColumnName()))
        return false;

    for (const auto & child : node->getChildren())
    {
        if (child && !onlyIndexColumns(child, primary_key_set))
            return false;
    }

    return true;
}

bool onlyConstants(const QueryTreeNodePtr & node)
{
    /// if it's only constant it will be already calculated
    return node->as<ConstantNode>() != nullptr;
}

const std::unordered_map<std::string_view, ComparisonGraphCompareResult> & getRelationMap()
{
    using enum ComparisonGraphCompareResult;
    static const std::unordered_map<std::string_view, ComparisonGraphCompareResult> relations =
    {
        {"equals", EQUAL},
        {"less", LESS},
        {"lessOrEquals", LESS_OR_EQUAL},
        {"greaterOrEquals", GREATER_OR_EQUAL},
        {"greater", GREATER},
    };
    return relations;
}

const std::unordered_map<ComparisonGraphCompareResult, std::string> & getReverseRelationMap()
{
    using enum ComparisonGraphCompareResult;
    static const std::unordered_map<ComparisonGraphCompareResult, std::string> relations =
    {
        {EQUAL, "equals"},
        {LESS, "less"},
        {LESS_OR_EQUAL, "lessOrEquals"},
        {GREATER_OR_EQUAL, "greaterOrEquals"},
        {GREATER, "greater"},
    };
    return relations;
}

bool canBeSequence(const ComparisonGraphCompareResult left, const ComparisonGraphCompareResult right)
{
    using enum ComparisonGraphCompareResult;
    if (left == UNKNOWN || right == UNKNOWN || left == NOT_EQUAL || right == NOT_EQUAL)
        return false;
    if ((left == GREATER || left == GREATER_OR_EQUAL) && (right == LESS || right == LESS_OR_EQUAL))
        return false;
    if ((right == GREATER || right == GREATER_OR_EQUAL) && (left == LESS || left == LESS_OR_EQUAL))
        return false;
    return true;
}

ComparisonGraphCompareResult mostStrict(const ComparisonGraphCompareResult left, const ComparisonGraphCompareResult right)
{
    using enum ComparisonGraphCompareResult;
    if (left == LESS || left == GREATER)
        return left;
    if (right == LESS || right == GREATER)
        return right;
    if (left == LESS_OR_EQUAL || left == GREATER_OR_EQUAL)
        return left;
    if (right == LESS_OR_EQUAL || right == GREATER_OR_EQUAL)
        return right;
    if (left == EQUAL)
        return left;
    if (right == EQUAL)
        return right;
    return UNKNOWN;
}

/// Create OR-group for 'indexHint'.
/// Consider we have expression like A <op1> C, where C is constant.
/// Consider we have a constraint I <op2> A, where I depends only on columns from primary key.
/// Then if op1 and op2 forms a sequence of comparisons (e.g. A < C and I < A),
/// we can add to expression 'indexHint(I < A)' condition.
Analyzer::CNF::OrGroup createIndexHintGroup(
    const Analyzer::CNF::OrGroup & group,
    const ComparisonGraph<QueryTreeNodePtr> & graph,
    const QueryTreeNodes & primary_key_only_nodes,
    const ContextPtr & context)
{
    Analyzer::CNF::OrGroup result;
    for (const auto & atom : group)
    {
        const auto * function_node = atom.node_with_hash.node->as<FunctionNode>();
        if (!function_node || getRelationMap().contains(function_node->getFunctionName()))
            continue;

        const auto & arguments =  function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            continue;

        auto check_and_insert = [&](const size_t index, const ComparisonGraphCompareResult expected_result)
        {
            if (!onlyConstants(arguments[1 - index]))
                return false;

            for (const auto & primary_key_node : primary_key_only_nodes)
            {
                ComparisonGraphCompareResult actual_result;
                if (index == 0)
                    actual_result = graph.compare(primary_key_node, arguments[index]);
                else
                    actual_result = graph.compare(arguments[index], primary_key_node);

                if (canBeSequence(expected_result, actual_result))
                {
                    auto helper_node = function_node->clone();
                    auto & helper_function_node = helper_node->as<FunctionNode &>();
                    auto reverse_function_name = getReverseRelationMap().at(mostStrict(expected_result, actual_result));
                    helper_function_node.resolveAsFunction(FunctionFactory::instance().get(reverse_function_name, context));
                    result.insert(Analyzer::CNF::AtomicFormula{atom.negative, std::move(helper_node)});
                    return true;
                }
            }

            return false;
        };

        auto expected = getRelationMap().at(function_node->getFunctionName());
        if (!check_and_insert(0, expected) && !check_and_insert(1, expected))
            return {};
    }

    return result;
}

void addIndexConstraint(Analyzer::CNF & cnf, const QueryTreeNodes & table_expressions, const ContextPtr & context)
{
    for (const auto & table_expression : table_expressions)
    {
        auto snapshot = getStorageSnapshot(table_expression);
        if (!snapshot || !snapshot->metadata)
            continue;

        const auto primary_key = snapshot->metadata->getColumnsRequiredForPrimaryKey();
        const std::unordered_set<std::string_view> primary_key_set(primary_key.begin(), primary_key.end());
        const auto & query_tree_constraint = snapshot->metadata->getConstraints().getQueryTreeData(context, table_expression);
        const auto & graph = query_tree_constraint.getGraph();

        QueryTreeNodes primary_key_only_nodes;
        for (const auto & vertex : graph.getVertices())
        {
            for (const auto & node : vertex)
            {
                if (onlyIndexColumns(node, primary_key_set))
                    primary_key_only_nodes.push_back(node);
            }
        }

        Analyzer::CNF::AndGroup and_group;
        const auto & statements = cnf.getStatements();
        for (const auto & group : statements)
        {
            auto new_group = createIndexHintGroup(group, graph, primary_key_only_nodes, context);
            if (!new_group.empty())
                and_group.emplace(std::move(new_group));
        }

        if (!and_group.empty())
        {
            Analyzer::CNF::OrGroup new_group;
            auto index_hint_node = std::make_shared<FunctionNode>("indexHint");
            index_hint_node->getArguments().getNodes().push_back(Analyzer::CNF{std::move(and_group)}.toQueryTree(context));
            index_hint_node->resolveAsFunction(FunctionFactory::instance().get("indexHint", context));
            new_group.insert({false, QueryTreeNodePtrWithHash{std::move(index_hint_node)}});

            cnf.appendGroup({new_group});
        }
    }
}

void optimizeWithConstraints(Analyzer::CNF & cnf, const QueryTreeNodes & table_expressions, const ContextPtr & context)
{
    cnf.pullNotOutFunctions(context);

    for (const auto & table_expression : table_expressions)
    {
        auto snapshot = getStorageSnapshot(table_expression);
        if (!snapshot || !snapshot->metadata)
            continue;

        const auto & constraints = snapshot->metadata->getConstraints();
        const auto & query_tree_constraints = constraints.getQueryTreeData(context, table_expression);
        const auto & compare_graph = query_tree_constraints.getGraph();
        cnf.filterAlwaysTrueGroups([&](const auto & group)
           {
               /// remove always true groups from CNF
               return !checkIfGroupAlwaysTrueFullMatch(group, query_tree_constraints) && !checkIfGroupAlwaysTrueGraph(group, compare_graph);
           })
           .filterAlwaysFalseAtoms([&](const Analyzer::CNF::AtomicFormula & atom)
           {
               /// remove always false atoms from CNF
               return !checkIfAtomAlwaysFalseFullMatch(atom, query_tree_constraints) && !checkIfAtomAlwaysFalseGraph(atom, compare_graph);
           })
           .transformAtoms([&](const auto & atom)
           {
               return replaceTermsToConstants(atom, compare_graph);
           })
           .reduce();
    }

    cnf.pushNotIntoFunctions(context);

    const auto & settings = context->getSettingsRef();
    if (settings.optimize_append_index)
        addIndexConstraint(cnf, table_expressions, context);
}

void optimizeNode(QueryTreeNodePtr & node, const QueryTreeNodes & table_expressions, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    auto cnf = tryConvertQueryToCNF(node, context);
    if (!cnf)
        return;

    if (settings.optimize_using_constraints)
        optimizeWithConstraints(*cnf, table_expressions, context);

    auto new_node = cnf->toQueryTree(context);
    if (!new_node)
        return;

    node = std::move(new_node);
}

class ConvertQueryToCNFVisitor : public InDepthQueryTreeVisitorWithContext<ConvertQueryToCNFVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertQueryToCNFVisitor>;
    using Base::Base;

    static bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType &)
    {
        return parent->as<QueryNode>() == nullptr;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto table_expressions = extractTableExpressions(query_node->getJoinTree());

        if (query_node->hasWhere())
            optimizeNode(query_node->getWhere(), table_expressions, getContext());

        if (query_node->hasPrewhere())
            optimizeNode(query_node->getPrewhere(), table_expressions, getContext());
    }
};

}

void ConvertQueryToCNFPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings.convert_query_to_cnf)
        return;

    ConvertQueryToCNFVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
