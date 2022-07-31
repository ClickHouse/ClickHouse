#include <Storages/ConstraintsDescription.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

#include <Core/Defines.h>


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

    return serializeAST(list, true);
}

ConstraintsDescription ConstraintsDescription::parse(const String & str)
{
    if (str.empty())
        return {};

    ConstraintsDescription res;
    ParserConstraintDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

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
        throw Exception("Unknown constraint type.", ErrorCodes::LOGICAL_ERROR);
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

std::unique_ptr<ComparisonGraph> ConstraintsDescription::buildGraph() const
{
    static const NameSet relations = { "equals", "less", "lessOrEquals", "greaterOrEquals", "greater" };

    std::vector<ASTPtr> constraints_for_graph;
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

    return std::make_unique<ComparisonGraph>(constraints_for_graph);
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

const ComparisonGraph & ConstraintsDescription::getGraph() const
{
    return *graph;
}

const std::vector<std::vector<CNFQuery::AtomicFormula>> & ConstraintsDescription::getConstraintData() const
{
    return cnf_constraints;
}

const ASTList & ConstraintsDescription::getConstraints() const
{
    return constraints;
}

std::optional<ConstraintsDescription::AtomIds> ConstraintsDescription::getAtomIds(const ASTPtr & ast) const
{
    const auto hash = ast->getTreeHash();
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

ConstraintsDescription::ConstraintsDescription(const ASTList & constraints_)
    : constraints(constraints_)
{
    update();
}

ConstraintsDescription::ConstraintsDescription(const ConstraintsDescription & other)
{
    for (const auto & constraint : other.constraints)
        constraints.emplace_back(constraint->clone());
    update();
}

ConstraintsDescription & ConstraintsDescription::operator=(const ConstraintsDescription & other)
{
    for (const auto & expr : other.constraints)
        constraints.emplace_back(expr->clone());
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
        graph = std::make_unique<ComparisonGraph>(std::vector<ASTPtr>());
        return;
    }

    cnf_constraints = buildConstraintData();
    ast_to_atom_ids.clear();
    for (size_t i = 0; i < cnf_constraints.size(); ++i)
        for (size_t j = 0; j < cnf_constraints[i].size(); ++j)
            ast_to_atom_ids[cnf_constraints[i][j].ast->getTreeHash()].push_back({i, j});

    graph = buildGraph();
}

}
