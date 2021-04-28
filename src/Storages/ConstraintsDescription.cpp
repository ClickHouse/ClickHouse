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
    const auto ast_to_decr_constraint_type = [](ASTConstraintDeclaration::Type constraint_type) -> UInt32
    {
        switch (constraint_type)
        {
            case ASTConstraintDeclaration::Type::CHECK:
                return static_cast<UInt32>(ConstraintType::CHECK);
            case ASTConstraintDeclaration::Type::ASSUME:
                return static_cast<UInt32>(ConstraintType::ASSUME);
        }
    };

    ASTs res;
    res.reserve(constraints.size());
    for (const auto & constraint : constraints)
    {
        if ((ast_to_decr_constraint_type(constraint->as<ASTConstraintDeclaration>()->type) & static_cast<UInt32>(selection)) != 0) {
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
        Poco::Logger::get("atomic_formula: initial:").information(constraint->as<ASTConstraintDeclaration>()->expr->ptr()->dumpTree());
        const auto cnf = TreeCNFConverter::toCNF(constraint->as<ASTConstraintDeclaration>()->expr->ptr())
            .pullNotOutFunctions();
        for (const auto & group : cnf.getStatements()) {
            if (group.size() == 1)
                constraint_data.push_back(*group.begin());
        }
    }

    return constraint_data;
}

std::unique_ptr<ComparisonGraph> ConstraintsDescription::buildGraph() const
{
    static const std::set<std::string> relations = {
        "equals", "less", "lessOrEquals", "greaterOrEquals", "greater"};

    std::vector<ASTPtr> constraints_for_graph;
    auto atomic_formulas = getAtomicConstraintData();
    for (auto & atomic_formula : atomic_formulas)
    {
        Poco::Logger::get("atomic_formula: before:").information(atomic_formula.ast->dumpTree() + " " + std::to_string(atomic_formula.negative));
        pushNotIn(atomic_formula);
        auto * func = atomic_formula.ast->as<ASTFunction>();
        if (func && relations.count(func->name))
        {
            if (atomic_formula.negative)
                throw Exception(": ", ErrorCodes::LOGICAL_ERROR);
            Poco::Logger::get("atomic_formula: after:").information(atomic_formula.ast->dumpTree() + " " + std::to_string(atomic_formula.negative));
            constraints_for_graph.push_back(atomic_formula.ast);
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
            res.push_back(ExpressionAnalyzer(constraint_ptr->expr->clone(), syntax_result, context).getActions(false));
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

const std::vector<ASTPtr> & ConstraintsDescription::getConstraints() const
{
    return constraints;
}

void ConstraintsDescription::updateConstraints(const std::vector<ASTPtr> & constraints_)
{
    constraints = constraints_;
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

void ConstraintsDescription::update()
{
    cnf_constraints = buildConstraintData();
    graph = buildGraph();
}

}
