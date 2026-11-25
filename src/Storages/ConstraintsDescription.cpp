#include <Storages/ConstraintsDescription.h>

#include <algorithm>

#include <Common/quoteString.h>
#include <Core/Block.h>
#include <Interpreters/ComparisonGraph.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>

#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/ExpressionContainsArrayJoin.h>

#include <Core/Defines.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/Passes/CNF.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/QueryTreePassManager.h>

#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/StorageID.h>

#include <Planner/CollectTableExpressionData.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerSteps.h>
#include <Planner/Utils.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/StorageDummy.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_IDENTIFIER;
}

String ConstraintsDescription::toString() const
{
    if (constraints.empty())
        return {};

    ASTExpressionList list;
    for (const auto & constraint : constraints)
        list.children.push_back(constraint);

    return list.formatIgnoringRedundantParentheses();
}

ConstraintsDescription ConstraintsDescription::parse(const String & str)
{
    if (str.empty())
        return {};

    ConstraintsDescription res;
    ParserConstraintDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (const auto & constraint : list->children)
        res.constraints.push_back(constraint);

    return res;
}

ASTs ConstraintsDescription::filterConstraints(ConstraintType selection) const
{
    const auto ast_to_decr_constraint_type = [](ASTConstraintDeclaration::Type constraint_type) -> UInt8
    {
        switch (constraint_type)
        {
            case ASTConstraintDeclaration::Type::CHECK:
                return static_cast<UInt8>(ConstraintType::CHECK);
            case ASTConstraintDeclaration::Type::ASSUME:
                return static_cast<UInt8>(ConstraintType::ASSUME);
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown constraint type.");
    };

    ASTs res;
    res.reserve(constraints.size());
    for (const auto & constraint : constraints)
    {
        if ((ast_to_decr_constraint_type(constraint->as<ASTConstraintDeclaration>()->type) & static_cast<UInt8>(selection)) != 0)
        {
            res.push_back(constraint);
        }
    }
    return res;
}

ASTs ConstraintsDescription::filterConstraintsForOptimization() const
{
    ASTs res;
    for (const auto & constraint : filterConstraints(ConstraintsDescription::ConstraintType::ALWAYS_TRUE))
    {
        const auto & declaration = constraint->as<const ASTConstraintDeclaration &>();
        if (declaration.expr && expressionContainsArrayJoin(*declaration.expr))
            continue;
        res.push_back(constraint);
    }
    return res;
}

std::vector<std::vector<CNFQueryAtomicFormula>> ConstraintsDescription::buildConstraintData() const
{
    std::vector<std::vector<CNFQueryAtomicFormula>> constraint_data;
    for (const auto & constraint : filterConstraintsForOptimization())
    {
        const auto cnf = TreeCNFConverter::toCNF(constraint->as<ASTConstraintDeclaration>()->expr)
            .pullNotOutFunctions(); /// TODO: move prepare stage to ConstraintsDescription
        for (const auto & group : cnf.getStatements())
            constraint_data.emplace_back(std::begin(group), std::end(group));
    }

    return constraint_data;
}

std::vector<CNFQueryAtomicFormula> ConstraintsDescription::getAtomicConstraintData() const
{
    std::vector<CNFQueryAtomicFormula> constraint_data;
    for (const auto & constraint : filterConstraintsForOptimization())
    {
        const auto cnf = TreeCNFConverter::toCNF(constraint->as<ASTConstraintDeclaration>()->expr)
             .pullNotOutFunctions();
        for (const auto & group : cnf.getStatements())
        {
            if (group.size() == 1)
                constraint_data.push_back(*group.begin());
        }
    }

    return constraint_data;
}

std::unique_ptr<ComparisonGraph<ASTPtr>> ConstraintsDescription::buildGraph() const
{
    static const NameSet relations = { "equals", "less", "lessOrEquals", "greaterOrEquals", "greater" };

    ASTs constraints_for_graph;
    auto atomic_formulas = getAtomicConstraintData();
    for (const auto & atomic_formula : atomic_formulas)
    {
        CNFQueryAtomicFormula atom{atomic_formula.negative, atomic_formula.ast->clone()};
        pushNotIn(atom);

        /// `pushNotIn` does not always manage to remove the negation: `NOT (x < c)` is not `x >= c` when
        /// `x` can be a `NaN`, so an ordered comparison keeps its `NOT`. The comparison graph stores plain
        /// relations and has nowhere to put the negation, so such an atom is left out of it - taking it in
        /// as if it were positive would state the opposite of what the constraint says.
        if (atom.negative)
            continue;

        auto * func = atom.ast->as<ASTFunction>();
        if (func && relations.contains(func->name))
            constraints_for_graph.push_back(atom.ast);
    }

    return std::make_unique<ComparisonGraph<ASTPtr>>(constraints_for_graph);
}

ConstraintsExpressions ConstraintsDescription::getExpressions(
    ContextMutablePtr context, const NamesAndTypesList & source_columns_, const StorageID & table_id) const
{
    auto is_check = [](const ASTPtr & constraint)
    {
        return constraint->as<const ASTConstraintDeclaration &>().type == ASTConstraintDeclaration::Type::CHECK;
    };
    if (std::ranges::none_of(constraints, is_check))
        return {};

    /// Only top-level columns are in the checked block; subcolumns are extracted from them.
    Block available_columns;
    ColumnsDescription fake_columns;
    for (const auto & column : source_columns_)
    {
        available_columns.insert({column.type->createColumn(), column.type, column.name});
        fake_columns.add(ColumnDescription(column.name, column.type), /*after_column=*/ "", /*first=*/ false, /*add_subcolumns=*/ false);
    }

    /// Named as the real table, so that error messages and qualified names refer to it.
    auto fake_storage = std::make_shared<StorageDummy>(table_id, fake_columns);
    auto fake_table_expression = std::make_shared<TableNode>(fake_storage, context);

    ConstraintsExpressions res;
    res.reserve(constraints.size());
    for (const auto & constraint : constraints)
    {
        auto * constraint_ptr = constraint->as<ASTConstraintDeclaration>();
        if (constraint_ptr->type != ASTConstraintDeclaration::Type::CHECK)
            continue;

        try
        {
            /// The list wrapper lets a bare subquery be resolved as an expression.
            auto expression_list = make_intrusive<ASTExpressionList>();
            expression_list->children.push_back(constraint_ptr->expr->clone());
            auto expression = buildQueryTree(expression_list, context);

            QueryAnalyzer analyzer(/*only_analyze_=*/ false);
            analyzer.resolve(expression, fake_table_expression, context);

            /// A guard: no SQL path is known to give several expressions here.
            if (expression->as<ListNode &>().getNodes().size() != 1)
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Constraint {} for table {} must be a single expression. Expression: ({})",
                    backQuote(constraint_ptr->name), table_id.getNameForLogs(), constraint_ptr->expr->formatForErrorMessage());

            /// The planner would turn these into inputs of the actions.
            assertNoAggregateFunctionNodes(expression, "in CHECK constraint");
            assertNoGroupingFunctionNodes(expression, "in CHECK constraint");
            assertNoWindowFunctionNodes(expression, "in CHECK constraint");

            auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
            /// Not a copy: functions of the returned actions may refer to the context while they are executed.
            auto planner_context = std::make_shared<PlannerContext>(context, global_planner_context, SelectQueryOptions{});
            collectSetsAndSourceColumns(expression, planner_context, /*keep_alias_columns=*/ false);

            auto [constraint_dag, correlated_subtrees] = buildActionsDAGFromExpressionNode(
                expression,
                /*input_columns=*/ {},
                planner_context,
                /*correlated_columns_set=*/ {},
                /*use_column_identifier_as_action_node_name=*/ false);
            correlated_subtrees.assertEmpty("in a CHECK constraint");

            /// `CheckConstraintsTransform` builds the sets for `IN (SELECT ...)` from these plans.
            for (auto & subquery : planner_context->getPreparedSets().getSubqueries())
            {
                auto query_tree = subquery->detachQueryTree();
                /// As for scalar subqueries; `QueryAnalysisPass` also creates the unique aliases.
                QueryTreePassManager query_tree_pass_manager(context);
                addQueryTreePasses(query_tree_pass_manager);
                query_tree_pass_manager.run(query_tree);
                /// The `Planner` mutates the options it is given.
                auto subquery_options = SelectQueryOptions{}.subquery();
                subquery_options.forceMaterializeCTE();
                /// Planned as in `addBuildSubqueriesForSetsStepIfNeeded`; the filters let e.g. `Distributed` skip shards.
                Planner subquery_planner(
                    query_tree,
                    subquery_options,
                    std::make_shared<GlobalPlannerContext>(
                        nullptr, nullptr, nullptr, collectFiltersForAnalysis(query_tree, subquery_options, nullptr)));
                subquery_planner.buildQueryPlanIfNeeded();
                subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan()));
            }

            /// `CheckConstraintsTransform` reads the result by the name of the AST.
            auto & outputs = constraint_dag.getOutputs();
            const auto * result_node = outputs.at(0);
            String result_name = constraint_ptr->expr->getColumnName();
            if (result_node->result_name != result_name)
                result_node = &constraint_dag.addAlias(*result_node, result_name);
            outputs = {result_node};

            /// The checked block is shared by all constraints, so the used columns must stay in it.
            for (const auto * input : constraint_dag.getInputs())
                if (input != result_node)
                    outputs.push_back(input);

            auto extract_subcolumns_dag = createSubcolumnsExtractionActions(available_columns, constraint_dag.getRequiredColumnsNames(), context);
            constraint_dag = ActionsDAG::merge(std::move(extract_subcolumns_dag), std::move(constraint_dag));

            /// A guard: every input must be a header column, not e.g. a virtual column of the `StorageDummy`.
            NameSet required_columns;
            for (const auto * input : constraint_dag.getInputs())
            {
                if (!available_columns.has(input->result_name))
                    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                        "Unknown identifier {} in constraint {} for table {}. Expression: ({})",
                        backQuote(input->result_name), backQuote(constraint_ptr->name), table_id.getNameForLogs(),
                        constraint_ptr->expr->formatForErrorMessage());
                required_columns.insert(input->result_name);
            }

            /// The violation message lists the required columns, keep them in the order of the header.
            ColumnsWithTypeAndName ordered_inputs;
            for (const auto & column : available_columns)
                if (required_columns.contains(column.name))
                    ordered_inputs.push_back(column);
            constraint_dag = ActionsDAG::merge(ActionsDAG(ordered_inputs), std::move(constraint_dag));

            res.push_back(std::make_shared<ExpressionActions>(
                std::move(constraint_dag), ExpressionActionsSettings(context, CompileExpressions::yes)));
        }
        catch (Exception & e)
        {
            e.addMessage("While building constraint {} for table {}. Expression: ({})",
                backQuote(constraint_ptr->name), table_id.getNameForLogs(), constraint_ptr->expr->formatForErrorMessage());
            throw;
        }
    }
    return res;
}

void ConstraintsDescription::checkExpressionsPreserveRowCount() const
{
    for (const auto & constraint : constraints)
    {
        const auto & declaration = constraint->as<const ASTConstraintDeclaration &>();
        if (declaration.expr && expressionContainsArrayJoin(*declaration.expr))
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Constraint {} cannot contain arrayJoin, because it changes the number of rows",
                backQuote(declaration.name));
    }
}

void ConstraintsDescription::checkNamesAreUnique() const
{
    NameSet names;
    for (const auto & constraint : constraints)
    {
        const auto & declaration = constraint->as<const ASTConstraintDeclaration &>();
        if (!names.insert(declaration.name).second)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Constraint {} is declared more than once", backQuote(declaration.name));
    }
}

const ComparisonGraph<ASTPtr> & ConstraintsDescription::getGraph() const
{
    return *graph;
}

const std::vector<std::vector<CNFQueryAtomicFormula>> & ConstraintsDescription::getConstraintData() const
{
    return cnf_constraints;
}

const ASTs & ConstraintsDescription::getConstraints() const
{
    return constraints;
}

std::optional<ConstraintsDescription::AtomIds> ConstraintsDescription::getAtomIds(const ASTPtr & ast) const
{
    const auto hash = ast->getTreeHash(/*ignore_aliases=*/ true);
    auto it = ast_to_atom_ids.find(hash);
    if (it != ast_to_atom_ids.end())
        return it->second;
    return std::nullopt;
}

std::vector<CNFQueryAtomicFormula> ConstraintsDescription::getAtomsById(const ConstraintsDescription::AtomIds & ids) const
{
    std::vector<CNFQueryAtomicFormula> result;
    for (const auto & id : ids)
        result.push_back(cnf_constraints[id.group_id][id.atom_id]);
    return result;
}

ConstraintsDescription::QueryTreeData ConstraintsDescription::getQueryTreeData(const ContextPtr & context, const TableExpressionNodePtr & table_node) const
{
    QueryTreeData data;
    std::vector<Analyzer::CNFAtomicFormula> atomic_constraints_data;

    QueryAnalysisPass pass(table_node);

    for (const auto & constraint : filterConstraintsForOptimization())
    {
        auto expr = constraint->as<ASTConstraintDeclaration>()->expr->ptr();
        // Wrap the scalar expression with a function call "equals(SELECT..., 1)".
        if (dynamic_cast<ASTSubquery *>(expr.get()))
        {
            auto func = make_intrusive<ASTFunction>();
            func ->name = "equals";
            func->children.push_back(make_intrusive<ASTExpressionList>());
            auto args = make_intrusive<ASTExpressionList>();
            args->children.push_back(expr);
            args->children.push_back(make_intrusive<ASTLiteral>(Field{static_cast<UInt8>(1)}));
            func->arguments = args;
            expr = func;
        }
        auto query_tree = buildQueryTree(expr, context);
        pass.run(query_tree, context);

        const auto cnf = Analyzer::CNF::toCNF(query_tree, context)
            .pullNotOutFunctions(context);
        for (const auto & group : cnf.getStatements())
        {
            data.cnf_constraints.emplace_back(group.begin(), group.end());

            if (group.size() == 1)
                atomic_constraints_data.emplace_back(*group.begin());
        }

        data.constraints.push_back(std::move(query_tree));
    }

    for (size_t i = 0; i < data.cnf_constraints.size(); ++i)
        for (size_t j = 0; j < data.cnf_constraints[i].size(); ++j)
            data.query_node_to_atom_ids[data.cnf_constraints[i][j].node_with_hash].push_back({i, j});

    /// build graph
    if (constraints.empty())
    {
        data.graph = std::make_unique<ComparisonGraph<QueryTreeNodePtr>>(QueryTreeNodes(), context);
    }
    else
    {
        static const NameSet relations = { "equals", "less", "lessOrEquals", "greaterOrEquals", "greater" };

        QueryTreeNodes constraints_for_graph;
        for (const auto & atomic_formula : atomic_constraints_data)
        {
            Analyzer::CNFAtomicFormula atom{atomic_formula.negative, atomic_formula.node_with_hash.node->clone()};
            atom = Analyzer::CNF::pushNotIntoFunction(atom, context);

            /// See the same check in `buildGraph`: an ordered comparison over an argument that can be a
            /// `NaN` keeps its negation, which the comparison graph cannot represent.
            if (atom.negative)
                continue;

            auto * function_node = atom.node_with_hash.node->as<FunctionNode>();
            if (function_node && relations.contains(function_node->getFunctionName()))
                constraints_for_graph.push_back(atom.node_with_hash.node);
        }
        data.graph = std::make_unique<ComparisonGraph<QueryTreeNodePtr>>(constraints_for_graph, context);
    }

    return data;
}

const QueryTreeNodes & ConstraintsDescription::QueryTreeData::getConstraints() const
{
    return constraints;
}

const std::vector<std::vector<Analyzer::CNFAtomicFormula>> & ConstraintsDescription::QueryTreeData::getConstraintData() const
{
    return cnf_constraints;
}

const ComparisonGraph<QueryTreeNodePtr> & ConstraintsDescription::QueryTreeData::getGraph() const
{
    return *graph;
}

std::optional<ConstraintsDescription::AtomIds> ConstraintsDescription::QueryTreeData::getAtomIds(const QueryTreeNodePtrWithHash & node_with_hash) const
{
    auto it = query_node_to_atom_ids.find(node_with_hash);
    if (it != query_node_to_atom_ids.end())
        return it->second;
    return std::nullopt;
}

std::vector<Analyzer::CNFAtomicFormula> ConstraintsDescription::QueryTreeData::getAtomsById(const AtomIds & ids) const
{
    std::vector<Analyzer::CNFAtomicFormula> result;
    for (const auto & id : ids)
        result.push_back(cnf_constraints[id.group_id][id.atom_id]);
    return result;
}

ConstraintsDescription::ConstraintsDescription(const ASTs & constraints_)
    : constraints(constraints_)
{
    update();
}

ConstraintsDescription::ConstraintsDescription(const ConstraintsDescription & other)
{
    constraints.reserve(other.constraints.size());
    for (const auto & constraint : other.constraints)
        constraints.emplace_back(constraint->clone());
    update();
}

ConstraintsDescription & ConstraintsDescription::operator=(const ConstraintsDescription & other)
{
    if (&other == this)
        return *this;
    constraints.resize(other.constraints.size());
    for (size_t i = 0; i < constraints.size(); ++i)
        constraints[i] = other.constraints[i]->clone();
    update();
    return *this;
}

ConstraintsDescription::ConstraintsDescription(ConstraintsDescription && other) /// NOLINT(hicpp-noexcept-move,performance-noexcept-move-constructor)
    : constraints(std::move(other.constraints))
{
    update();
}

ConstraintsDescription & ConstraintsDescription::operator=(ConstraintsDescription && other) /// NOLINT(hicpp-noexcept-move,performance-noexcept-move-constructor)
{
    constraints = std::move(other.constraints);
    update();

    return *this;
}

void ConstraintsDescription::update()
{
    if (constraints.empty())
    {
        cnf_constraints.clear();
        ast_to_atom_ids.clear();
        graph = std::make_unique<ComparisonGraph<ASTPtr>>(ASTs());
        return;
    }

    cnf_constraints = buildConstraintData();
    ast_to_atom_ids.clear();
    for (size_t i = 0; i < cnf_constraints.size(); ++i)
        for (size_t j = 0; j < cnf_constraints[i].size(); ++j)
            ast_to_atom_ids[cnf_constraints[i][j].ast->getTreeHash(/*ignore_aliases=*/ true)].push_back({i, j});

    graph = buildGraph();
}

}
