#include <Analyzer/Passes/ConvertQueryToCNFPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/Passes/CNF.h>
#include <Analyzer/Utils.h>
#include <Analyzer/HashUtils.h>

#include <Core/Settings.h>

#include <Storages/IStorage.h>

#include <Functions/FunctionFactory.h>
#include <Interpreters/ComparisonGraph.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool convert_query_to_cnf;
    extern const SettingsBool optimize_append_index;
    extern const SettingsBool optimize_substitute_columns;
    extern const SettingsBool optimize_using_constraints;
}

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
    if (a.node_with_hash != b.node_with_hash)
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

bool checkIfGroupAlwaysTrueAtoms(const Analyzer::CNF::OrGroup & group)
{
    /// Filters out groups containing mutually exclusive atoms,
    /// since these groups are always True

    for (const auto & atom : group)
    {
        auto negated(atom);
        negated.negative = !atom.negative;
        if (group.contains(negated))
        {
            return true;
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
    if (auto * table_function_node = node->as<TableFunctionNode>())
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
        if (!function_node || !getRelationMap().contains(function_node->getFunctionName()))
            continue;

        const auto & arguments = function_node->getArguments().getNodes();
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
                    helper_function_node.getArguments().getNodes()[index] = primary_key_node->clone();
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
            index_hint_node->getArguments().getNodes().push_back(Analyzer::CNF{std::move(and_group)}.toQueryTree());
            index_hint_node->resolveAsFunction(FunctionFactory::instance().get("indexHint", context));
            new_group.insert({false, QueryTreeNodePtrWithHash{std::move(index_hint_node)}});

            cnf.appendGroup({new_group});
        }
    }
}

struct ColumnPrice
{
    Int64 compressed_size{0};
    Int64 uncompressed_size{0};

    ColumnPrice(const Int64 compressed_size_, const Int64 uncompressed_size_)
        : compressed_size(compressed_size_)
        , uncompressed_size(uncompressed_size_)
    {
    }

    bool operator<(const ColumnPrice & that) const
    {
        return std::tie(compressed_size, uncompressed_size) < std::tie(that.compressed_size, that.uncompressed_size);
    }

    ColumnPrice & operator+=(const ColumnPrice & that)
    {
        compressed_size += that.compressed_size;
        uncompressed_size += that.uncompressed_size;
        return *this;
    }

    ColumnPrice & operator-=(const ColumnPrice & that)
    {
        compressed_size -= that.compressed_size;
        uncompressed_size -= that.uncompressed_size;
        return *this;
    }
};

using ColumnPriceByName = std::unordered_map<String, ColumnPrice>;
using ColumnPriceByQueryNode = QueryTreeNodePtrWithHashMap<ColumnPrice>;

class ComponentCollectorVisitor : public ConstInDepthQueryTreeVisitor<ComponentCollectorVisitor>
{
public:
    ComponentCollectorVisitor(
        std::set<UInt64> & components_,
        QueryTreeNodePtrWithHashMap<UInt64> & query_node_to_component_,
        const ComparisonGraph<QueryTreeNodePtr> & graph_)
        : components(components_), query_node_to_component(query_node_to_component_), graph(graph_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (auto id = graph.getComponentId(node))
        {
            query_node_to_component.emplace(node, *id);
            components.insert(*id);
        }
    }

private:
    std::set<UInt64> & components;
    QueryTreeNodePtrWithHashMap<UInt64> & query_node_to_component;

    const ComparisonGraph<QueryTreeNodePtr> & graph;
};

class ColumnNameCollectorVisitor : public ConstInDepthQueryTreeVisitor<ColumnNameCollectorVisitor>
{
public:
    ColumnNameCollectorVisitor(
        std::unordered_set<std::string> & column_names_,
        const QueryTreeNodePtrWithHashMap<UInt64> * query_node_to_component_)
        : column_names(column_names_), query_node_to_component(query_node_to_component_)
    {}

    bool needChildVisit(const VisitQueryTreeNodeType & parent, const VisitQueryTreeNodeType &)
    {
        return !query_node_to_component || !query_node_to_component->contains(parent);
    }

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (query_node_to_component && query_node_to_component->contains(node))
            return;

        if (const auto * column_node = node->as<ColumnNode>())
            column_names.insert(column_node->getColumnName());
    }

private:
    std::unordered_set<std::string> & column_names;
    const QueryTreeNodePtrWithHashMap<UInt64> * query_node_to_component;
};

class SubstituteColumnVisitor : public InDepthQueryTreeVisitor<SubstituteColumnVisitor>
{
public:
    SubstituteColumnVisitor(
        const QueryTreeNodePtrWithHashMap<UInt64> & query_node_to_component_,
        const std::unordered_map<UInt64, QueryTreeNodePtr> & id_to_query_node_map_,
        ContextPtr context_)
        : query_node_to_component(query_node_to_component_), id_to_query_node_map(id_to_query_node_map_), context(std::move(context_))
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto component_id_it = query_node_to_component.find(node);
        if (component_id_it == query_node_to_component.end())
            return;

        const auto component_id = component_id_it->second;
        auto new_node = id_to_query_node_map.at(component_id)->clone();

        if (!node->getResultType()->equals(*new_node->getResultType()))
        {
            node = buildCastFunction(new_node, node->getResultType(), context);
            return;
        }

        node = std::move(new_node);
    }

private:
    const QueryTreeNodePtrWithHashMap<UInt64> & query_node_to_component;
    const std::unordered_map<UInt64, QueryTreeNodePtr> & id_to_query_node_map;
    ContextPtr context;
};

ColumnPrice calculatePrice(
        const ColumnPriceByName & column_prices,
        const std::unordered_set<std::string> & column_names)
{
    ColumnPrice result(0, 0);

    for (const auto & column : column_names)
    {
        if (auto it = column_prices.find(column); it != column_prices.end())
            result += it->second;
    }

    return result;
}


void bruteForce(
        const ComparisonGraph<QueryTreeNodePtr> & graph,
        const std::vector<UInt64> & components,
        size_t current_component,
        const ColumnPriceByName & column_prices,
        ColumnPrice current_price,
        std::vector<QueryTreeNodePtr> & expressions_stack,
        ColumnPrice & min_price,
        std::vector<QueryTreeNodePtr> & min_expressions)
{
    if (current_component == components.size())
    {
        if (current_price < min_price)
        {
            min_price = current_price;
            min_expressions = expressions_stack;
        }
        return;
    }

    for (const auto & node : graph.getComponent(components[current_component]))
    {
        std::unordered_set<std::string> column_names;
        ColumnNameCollectorVisitor column_name_collector{column_names, nullptr};
        column_name_collector.visit(node);

        ColumnPrice expression_price = calculatePrice(column_prices, column_names);

        expressions_stack.push_back(node);
        current_price += expression_price;

        ColumnPriceByName new_prices(column_prices);
        for (const auto & column : column_names)
            new_prices.insert_or_assign(column, ColumnPrice(0, 0));

        bruteForce(graph,
                   components,
                   current_component + 1,
                   new_prices,
                   current_price,
                   expressions_stack,
                   min_price,
                   min_expressions);

        current_price -= expression_price;
        expressions_stack.pop_back();
    }
}

void substituteColumns(QueryNode & query_node, const QueryTreeNodes & table_expressions, const ContextPtr & context)
{
    static constexpr UInt64 COLUMN_PENALTY = 10 * 1024 * 1024;
    static constexpr Int64 INDEX_PRICE = -1'000'000'000'000'000'000;

    for (const auto & table_expression : table_expressions)
    {
        auto snapshot = getStorageSnapshot(table_expression);
        if (!snapshot || !snapshot->metadata)
            continue;

        const auto column_sizes = snapshot->storage.getColumnSizes();
        if (column_sizes.empty())
            return;

        auto query_tree_constraint = snapshot->metadata->getConstraints().getQueryTreeData(context, table_expression);
        const auto & graph = query_tree_constraint.getGraph();

        auto run_for_all = [&](const auto function)
        {
            function(query_node.getProjectionNode());

            if (query_node.hasWhere())
                function(query_node.getWhere());

            if (query_node.hasPrewhere())
                function(query_node.getPrewhere());

            if (query_node.hasHaving())
                function(query_node.getHaving());
        };

        std::set<UInt64> components;
        QueryTreeNodePtrWithHashMap<UInt64> query_node_to_component;
        std::unordered_set<std::string> column_names;

        run_for_all([&](QueryTreeNodePtr & node)
        {
            ComponentCollectorVisitor component_collector{components, query_node_to_component, graph};
            component_collector.visit(node);
            ColumnNameCollectorVisitor column_name_collector{column_names, &query_node_to_component};
            column_name_collector.visit(node);
        });

        ColumnPriceByName column_prices;
        const auto primary_key = snapshot->metadata->getColumnsRequiredForPrimaryKey();

        for (const auto & [column_name, column_size] : column_sizes)
            column_prices.insert_or_assign(column_name, ColumnPrice(column_size.data_compressed + COLUMN_PENALTY, column_size.data_uncompressed));

        for (const auto & column_name : primary_key)
            column_prices.insert_or_assign(column_name, ColumnPrice(INDEX_PRICE, INDEX_PRICE));

        for (const auto & column_name : column_names)
            column_prices.insert_or_assign(column_name, ColumnPrice(0, 0));

        std::unordered_map<UInt64, QueryTreeNodePtr> id_to_query_node_map;
        std::vector<UInt64> components_list;

        for (const auto component_id : components)
        {
            auto component = graph.getComponent(component_id);
            if (component.size() == 1)
                id_to_query_node_map[component_id] = component.front();
            else
                components_list.push_back(component_id);
        }

        std::vector<QueryTreeNodePtr> expressions_stack;
        ColumnPrice min_price(std::numeric_limits<Int64>::max(), std::numeric_limits<Int64>::max());
        std::vector<QueryTreeNodePtr> min_expressions;

        bruteForce(graph,
                   components_list,
                   0,
                   column_prices,
                   ColumnPrice(0, 0),
                   expressions_stack,
                   min_price,
                   min_expressions);

        for (size_t i = 0; i < components_list.size(); ++i)
            id_to_query_node_map[components_list[i]] = min_expressions[i];

        SubstituteColumnVisitor substitute_column{query_node_to_component, id_to_query_node_map, context};

        run_for_all([&](QueryTreeNodePtr & node)
        {
            substitute_column.visit(node);
        });
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
               return !checkIfGroupAlwaysTrueFullMatch(group, query_tree_constraints)
                   && !checkIfGroupAlwaysTrueGraph(group, compare_graph) && !checkIfGroupAlwaysTrueAtoms(group);
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
    if (settings[Setting::optimize_append_index])
        addIndexConstraint(cnf, table_expressions, context);
}

void optimizeNode(QueryTreeNodePtr & node, const QueryTreeNodes & table_expressions, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    auto cnf = tryConvertQueryToCNF(node, context);
    if (!cnf)
        return;

    if (settings[Setting::optimize_using_constraints])
        optimizeWithConstraints(*cnf, table_expressions, context);

    auto new_node = cnf->toQueryTree();
    node = std::move(new_node);
}

class ConvertQueryToCNFVisitor : public InDepthQueryTreeVisitorWithContext<ConvertQueryToCNFVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertQueryToCNFVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto table_expressions = extractTableExpressions(query_node->getJoinTree());

        const auto & context = getContext();
        const auto & settings = context->getSettingsRef();

        bool has_filter = false;
        const auto optimize_filter = [&](QueryTreeNodePtr & filter_node)
        {
            if (filter_node == nullptr)
                return;

            optimizeNode(filter_node, table_expressions, context);
            has_filter = true;
        };

        optimize_filter(query_node->getWhere());
        optimize_filter(query_node->getPrewhere());
        optimize_filter(query_node->getHaving());

        if (has_filter && settings[Setting::optimize_substitute_columns])
            substituteColumns(*query_node, table_expressions, context);
    }
};

}

void ConvertLogicalExpressionToCNFPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::convert_query_to_cnf])
        return;

    ConvertQueryToCNFVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
