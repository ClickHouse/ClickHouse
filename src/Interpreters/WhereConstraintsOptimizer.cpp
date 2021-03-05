#include <Interpreters/WhereConstraintsOptimizer.h>

#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


std::vector<std::vector<CNFQuery::AtomicFormula>> getConstraintData(const StorageMetadataPtr & metadata_snapshot)
{
    std::vector<std::vector<CNFQuery::AtomicFormula>> constraint_data;
    for (const auto & constraint :
         metadata_snapshot->getConstraints().filterConstraints(ConstraintsDescription::ConstraintType::ALWAYS_TRUE))
    {
        const auto cnf = TreeCNFConverter::toCNF(constraint->as<ASTConstraintDeclaration>()->expr->ptr())
                             .pullNotOutFunctions(); /// TODO: move prepare stage to ConstraintsDescription
        for (const auto & group : cnf.getStatements())
            constraint_data.emplace_back(std::begin(group), std::end(group));
    }

    return constraint_data;
}

std::vector<CNFQuery::AtomicFormula> getAtomicConstraintData(const StorageMetadataPtr & metadata_snapshot)
{
    std::vector<CNFQuery::AtomicFormula> constraint_data;
    for (const auto & constraint :
        metadata_snapshot->getConstraints().filterConstraints(ConstraintsDescription::ConstraintType::ALWAYS_TRUE))
    {
        const auto cnf = TreeCNFConverter::toCNF(constraint->as<ASTConstraintDeclaration>()->expr->ptr())
            .pullNotOutFunctions();
        for (const auto & group : cnf.getStatements()) {
            if (group.size() == 1)
                constraint_data.push_back(*group.begin());
        }
    }

    return constraint_data;
}

ComparisonGraph getComparisonGraph(const StorageMetadataPtr & metadata_snapshot)
{
    static const std::set<std::string> relations = {
        "equals", "less", "lessOrEquals", "greaterOrEquals", "greater"};

    std::vector<ASTPtr> constraints_for_graph;
    auto atomic_formulas = getAtomicConstraintData(metadata_snapshot);
    const std::vector<CNFQuery::AtomicFormula> atomic_constraints = getAtomicConstraintData(metadata_snapshot);
    for (auto & atomic_formula : atomic_formulas)
    {
        pushNotIn(atomic_formula);
        auto * func = atomic_formula.ast->as<ASTFunction>();
        if (func && relations.count(func->name))
        {
            if (atomic_formula.negative)
                throw Exception(": ", ErrorCodes::LOGICAL_ERROR);
            constraints_for_graph.push_back(atomic_formula.ast);
        }
    }

    return ComparisonGraph(constraints_for_graph);
}

WhereConstraintsOptimizer::WhereConstraintsOptimizer(
    ASTSelectQuery * select_query_,
    Aliases & /*aliases_*/,
    const NameSet & /*source_columns_set_*/,
    const std::vector<TableWithColumnNamesAndTypes> & /*tables_with_columns_*/,
    const StorageMetadataPtr & metadata_snapshot_)
    : select_query(select_query_)
    /* , aliases(aliases_)
    , source_columns_set(source_columns_set_)
    , tables_with_columns(tables_with_columns_)*/
    , metadata_snapshot(metadata_snapshot_)
{
}

namespace
{
    enum class MatchState
    {
        FULL_MATCH, /// a = b
        NOT_MATCH, /// a = not b
        NONE, /// other
    };
}

MatchState match(CNFQuery::AtomicFormula a, CNFQuery::AtomicFormula b)
{
    bool match_means_ok = true ^ a.negative ^ b.negative;

    if (a.ast->getTreeHash() == b.ast->getTreeHash() &&
        a.ast->getColumnName() == b.ast->getColumnName())
    {
        return match_means_ok ? MatchState::FULL_MATCH : MatchState::NOT_MATCH;
    }
    return MatchState::NONE;
}

bool checkIfGroupAlwaysTrueFullMatch(const CNFQuery::OrGroup & group, const std::vector<std::vector<CNFQuery::AtomicFormula>> & constraints)
{
    /// TODO: constraints graph

    /// TODO: this is temporary; need to write more effective search
    /// TODO: go deeper into asts (a < b, a = b,...) with z3 or some visitor
    for (const auto & constraint : constraints) /// one constraint in group is enough,
                                                /// otherwise it's difficult to make judgements without using constraint solving (z3..)
    {
        bool group_always_true = true;
        for (const auto & constraint_ast : constraint)
        {
            bool found_match = false;
            for (const auto & atom_ast : group)
            {
                const auto match_result = match(constraint_ast, atom_ast);

                if (match_result == MatchState::FULL_MATCH)
                {
                    found_match = true;
                    break;
                }
            }
            if (!found_match)
            {
                group_always_true = false;
                break;
            }
        }

        if (group_always_true)
            return true;
    }
    return false;
}

ComparisonGraph::CompareResult getExpectedCompare(const CNFQuery::AtomicFormula & atom)
{
    static const std::map<std::string, std::string> inverse_relations = {
        {"equals", "notEquals"},
        {"less", "greaterOrEquals"},
        {"lessOrEquals", "greater"},
        {"notEquals", "equals"},
        {"greaterOrEquals", "less"},
        {"greater", "lessOrEquals"},
    };

    static const std::map<std::string, ComparisonGraph::CompareResult> relation_to_compare = {
        {"equals", ComparisonGraph::CompareResult::EQUAL},
        {"less", ComparisonGraph::CompareResult::LESS},
        {"lessOrEquals", ComparisonGraph::CompareResult::LESS_OR_EQUAL},
        {"notEquals", ComparisonGraph::CompareResult::UNKNOWN},
        {"greaterOrEquals", ComparisonGraph::CompareResult::GREATER_OR_EQUAL},
        {"greater", ComparisonGraph::CompareResult::GREATER},
    };


    const auto * func = atom.ast->as<ASTFunction>();
    if (func && inverse_relations.count(func->name))
    {
        std::string function_name = func->name;
        if (atom.negative)
        {
            function_name = inverse_relations.at(func->name);
        }
        return relation_to_compare.at(function_name);
    }
    return ComparisonGraph::CompareResult::UNKNOWN;
}


bool checkIfGroupAlwaysTrueGraph(const CNFQuery::OrGroup & group, const ComparisonGraph & graph)
{
    for (const auto & atom : group)
    {
        const auto * func = atom.ast->as<ASTFunction>();
        if (func && func->arguments->children.size() == 2)
        {
            const auto expected = getExpectedCompare(atom);
            const auto result = graph.compare(func->arguments->children[0], func->arguments->children[1]);
            Poco::Logger::get("GRAPH REASON").information("neg: " + std::to_string(atom.negative));
            Poco::Logger::get("GRAPH REASON").information(atom.ast->dumpTree());
            Poco::Logger::get("GRAPH REASON").information(std::to_string(static_cast<int>(expected)) + " " + std::to_string(static_cast<int>(result)));

            if (expected == ComparisonGraph::CompareResult::UNKNOWN || result == ComparisonGraph::CompareResult::UNKNOWN)
                return false;

            if (expected == result)
                return true;
            if (result == ComparisonGraph::CompareResult::EQUAL &&
                (expected == ComparisonGraph::CompareResult::LESS_OR_EQUAL || expected == ComparisonGraph::CompareResult::GREATER_OR_EQUAL))
                return true;
            if (result == ComparisonGraph::CompareResult::LESS && expected == ComparisonGraph::CompareResult::LESS_OR_EQUAL)
                return true;
            if (result == ComparisonGraph::CompareResult::GREATER && expected == ComparisonGraph::CompareResult::GREATER_OR_EQUAL)
                return true;
        }
    }
    return false;
}


bool checkIfAtomAlwaysFalseFullMatch(const CNFQuery::AtomicFormula & atom, const std::vector<std::vector<CNFQuery::AtomicFormula>> & constraints)
{
    /// TODO: more efficient matching

    for (const auto & constraint : constraints)
    {
        if (constraint.size() > 1)
            continue; /// TMP

        for (const auto & constraint_atoms : constraint)
        {
            const auto match_result = match(constraint_atoms, atom);

            if (match_result != MatchState::NONE)
                return match_result == MatchState::NOT_MATCH;
        }
    }

    return false;
}

bool checkIfAtomAlwaysFalseGraph(const CNFQuery::AtomicFormula & atom, const ComparisonGraph & graph)
{
    const auto * func = atom.ast->as<ASTFunction>();
    if (func && func->arguments->children.size() == 2)
    {
        /// TODO: special support for !=
        const auto expected = getExpectedCompare(atom);
        const auto result = graph.compare(func->arguments->children[0], func->arguments->children[1]);
        Poco::Logger::get("GRAPH REASON F").information("neg: " + std::to_string(atom.negative));
        Poco::Logger::get("GRAPH REASON F").information(atom.ast->dumpTree());
        Poco::Logger::get("GRAPH REASON F").information(std::to_string(static_cast<int>(expected)) + " " + std::to_string(static_cast<int>(result)));

        if (expected == ComparisonGraph::CompareResult::UNKNOWN || result == ComparisonGraph::CompareResult::UNKNOWN)
            return false;

        if (expected == result)
            return false;
        else if (result == ComparisonGraph::CompareResult::EQUAL &&
            (expected == ComparisonGraph::CompareResult::LESS_OR_EQUAL || expected == ComparisonGraph::CompareResult::GREATER_OR_EQUAL))
            return false;
        else if (result == ComparisonGraph::CompareResult::LESS && expected == ComparisonGraph::CompareResult::LESS_OR_EQUAL)
            return false;
        else if (result == ComparisonGraph::CompareResult::GREATER && expected == ComparisonGraph::CompareResult::GREATER_OR_EQUAL)
            return false;
        else
            return true;
    }

    return false;
}


void WhereConstraintsOptimizer::perform()
{
    if (select_query->where() && metadata_snapshot)
    {
        const auto constraint_data = getConstraintData(metadata_snapshot);
        const auto compare_graph = getComparisonGraph(metadata_snapshot);
        Poco::Logger::get("BEFORE CNF ").information(select_query->where()->dumpTree());
        auto cnf = TreeCNFConverter::toCNF(select_query->where());
        Poco::Logger::get("BEFORE OPT").information(cnf.dump());
        cnf.pullNotOutFunctions()
            .filterAlwaysTrueGroups([&constraint_data, &compare_graph](const auto & group) { /// remove always true groups from CNF
                return !checkIfGroupAlwaysTrueFullMatch(group, constraint_data) && !checkIfGroupAlwaysTrueGraph(group, compare_graph);
            })
            .filterAlwaysFalseAtoms([&constraint_data, &compare_graph](const auto & atom) { /// remove always false atoms from CNF
                return !checkIfAtomAlwaysFalseFullMatch(atom, constraint_data) && !checkIfAtomAlwaysFalseGraph(atom, compare_graph);
            })
            .pushNotInFuntions();

        Poco::Logger::get("AFTER OPT").information(cnf.dump());
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, TreeCNFConverter::fromCNF(cnf));
    }
}

}
