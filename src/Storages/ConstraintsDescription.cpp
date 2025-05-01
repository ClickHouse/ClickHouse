#include <Storages/ConstraintsDescription.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

#include <Core/Defines.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>

#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String ConstraintsDescription::toString() const
{
    if (constraints.empty())
        return {};

    ASTExpressionList list;
    for (const auto & constraint : constraints)
        list.children.push_back(constraint);

    return serializeAST(list);
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

std::vector<std::vector<CNFQuery::AtomicFormula>> ConstraintsDescription::buildConstraintData() const
{
    std::vector<std::vector<CNFQuery::AtomicFormula>> constraint_data;
    for (const auto & constraint : filterConstraints(ConstraintsDescription::ConstraintType::ALWAYS_TRUE))
    {
        const auto cnf = TreeCNFConverter::toCNF(constraint->as<ASTConstraintDeclaration>()->expr->ptr())
            .pullNotOutFunctions(); /// TODO: move prepare stage to ConstraintsDescription
        for (const auto & group : cnf.getStatements())
            constraint_data.emplace_back(std::begin(group), std::end(group));
    }

    return constraint_data;
}

std::vector<CNFQuery::AtomicFormula> ConstraintsDescription::getAtomicConstraintData() const
{
    std::vector<CNFQuery::AtomicFormula> constraint_data;
    for (const auto & constraint : filterConstraints(ConstraintsDescription::ConstraintType::ALWAYS_TRUE))
    {
        const auto cnf = TreeCNFConverter::toCNF(constraint->as<ASTConstraintDeclaration>()->expr->ptr())
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
        CNFQuery::AtomicFormula atom{atomic_formula.negative, atomic_formula.ast->clone()};
        pushNotIn(atom);
        auto * func = atom.ast->as<ASTFunction>();
        if (func && relations.contains(func->name))
        {
            assert(!atom.negative);
            constraints_for_graph.push_back(atom.ast);
        }
    }

    return std::make_unique<ComparisonGraph<ASTPtr>>(constraints_for_graph);
}

ConstraintsExpressions ConstraintsDescription::getExpressions(const DB::ContextPtr context,
                                                              const DB::NamesAndTypesList & source_columns_) const
{
    ConstraintsExpressions res;
    res.reserve(constraints.size());
    for (const auto & constraint : constraints)
    {
        auto * constraint_ptr = constraint->as<ASTConstraintDeclaration>();
        if (constraint_ptr->type == ASTConstraintDeclaration::Type::CHECK)
        {
            // TreeRewriter::analyze has query as non-const argument so to avoid accidental query changes we clone it
            ASTPtr expr = constraint_ptr->expr->clone();
            auto syntax_result = TreeRewriter(context).analyze(expr, source_columns_);
            res.push_back(ExpressionAnalyzer(constraint_ptr->expr->clone(), syntax_result, context).getActions(false, true, CompileExpressions::yes));
        }
    }
    return res;
}

const ComparisonGraph<ASTPtr> & ConstraintsDescription::getGraph() const
{
    return *graph;
}

const std::vector<std::vector<CNFQuery::AtomicFormula>> & ConstraintsDescription::getConstraintData() const
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

std::vector<CNFQuery::AtomicFormula> ConstraintsDescription::getAtomsById(const ConstraintsDescription::AtomIds & ids) const
{
    std::vector<CNFQuery::AtomicFormula> result;
    for (const auto & id : ids)
        result.push_back(cnf_constraints[id.group_id][id.atom_id]);
    return result;
}

ConstraintsDescription::QueryTreeData ConstraintsDescription::getQueryTreeData(const ContextPtr & context, const QueryTreeNodePtr & table_node) const
{
    QueryTreeData data;
    std::vector<Analyzer::CNF::AtomicFormula> atomic_constraints_data;

    QueryAnalysisPass pass(table_node);

    for (const auto & constraint : filterConstraints(ConstraintsDescription::ConstraintType::ALWAYS_TRUE))
    {
        auto query_tree = buildQueryTree(constraint->as<ASTConstraintDeclaration>()->expr->ptr(), context);
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
            Analyzer::CNF::AtomicFormula atom{atomic_formula.negative, atomic_formula.node_with_hash.node->clone()};
            atom = Analyzer::CNF::pushNotIntoFunction(atom, context);

            auto * function_node = atom.node_with_hash.node->as<FunctionNode>();
            if (function_node && relations.contains(function_node->getFunctionName()))
            {
                assert(!atom.negative);
                constraints_for_graph.push_back(atom.node_with_hash.node);
            }
        }
        data.graph = std::make_unique<ComparisonGraph<QueryTreeNodePtr>>(constraints_for_graph, context);
    }

    return data;
}

const QueryTreeNodes & ConstraintsDescription::QueryTreeData::getConstraints() const
{
    return constraints;
}

const std::vector<std::vector<Analyzer::CNF::AtomicFormula>> & ConstraintsDescription::QueryTreeData::getConstraintData() const
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

std::vector<Analyzer::CNF::AtomicFormula> ConstraintsDescription::QueryTreeData::getAtomsById(const AtomIds & ids) const
{
    std::vector<Analyzer::CNF::AtomicFormula> result;
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
    constraints.resize(other.constraints.size());
    for (size_t i = 0; i < constraints.size(); ++i)
        constraints[i] = other.constraints[i]->clone();
    update();
    return *this;
}

ConstraintsDescription::ConstraintsDescription(ConstraintsDescription && other) noexcept
    : constraints(std::move(other.constraints))
{
    update();
}

ConstraintsDescription & ConstraintsDescription::operator=(ConstraintsDescription && other) noexcept
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
