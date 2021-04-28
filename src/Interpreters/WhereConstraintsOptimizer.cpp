#include <Interpreters/WhereConstraintsOptimizer.h>

#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/AddIndexConstraintsOptimizer.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
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
    for (const auto & constraint : constraints)
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

void replaceToConstants(ASTPtr & term, const ComparisonGraph & graph)
{
    const auto equal_constant = graph.getEqualConst(term);
    if (equal_constant)
    {
        term = (*equal_constant)->clone();
    }
    else
    {
        for (auto & child : term->children)
            replaceToConstants(child, graph);
    }
}

CNFQuery::AtomicFormula replaceTermsToConstants(const CNFQuery::AtomicFormula & atom, const ComparisonGraph & graph)
{
    CNFQuery::AtomicFormula result;
    result.negative = atom.negative;
    result.ast = atom.ast->clone();

    replaceToConstants(result.ast, graph);

    return result;
}

void WhereConstraintsOptimizer::perform()
{
    if (select_query->where() && metadata_snapshot)
    {
        const auto & constraint_data = metadata_snapshot->getConstraints().getConstraintData();
        const auto & compare_graph = metadata_snapshot->getConstraints().getGraph();
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
            .transformAtoms([&compare_graph](const auto & atom) {
                return replaceTermsToConstants(atom, compare_graph);
            })
            .pushNotInFuntions();
        AddIndexConstraintsOptimizer(metadata_snapshot).perform(cnf);

        Poco::Logger::get("AFTER OPT").information(cnf.dump());
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, TreeCNFConverter::fromCNF(cnf));
    }
}

}
