#include <Interpreters/WhereConstraintsOptimizer.h>

#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/AddIndexConstraintsOptimizer.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>

namespace DB
{

WhereConstraintsOptimizer::WhereConstraintsOptimizer(
    ASTSelectQuery * select_query_,
    const StorageMetadataPtr & metadata_snapshot_,
    const bool optimize_append_index_)
    : select_query(select_query_)
    , metadata_snapshot(metadata_snapshot_)
    , optimize_append_index(optimize_append_index_)
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

    if (a.ast->getTreeHash() == b.ast->getTreeHash())
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
    const auto * func = atom.ast->as<ASTFunction>();
    if (func)
    {
        auto expected = ComparisonGraph::getCompareResult(func->name);
        if (atom.negative)
            expected = ComparisonGraph::inverseCompareResult(expected);
        return expected;
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
            return graph.isAlwaysCompare(expected, func->arguments->children[0], func->arguments->children[1]);
        }
    }
    return false;
}


bool checkIfAtomAlwaysFalseFullMatch(const CNFQuery::AtomicFormula & atom, const std::vector<std::vector<CNFQuery::AtomicFormula>> & constraints)
{
    for (const auto & constraint : constraints)
    {
        if (constraint.size() > 1)
            continue;

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
        return !graph.isPossibleCompare(expected, func->arguments->children[0], func->arguments->children[1]);
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
            .filterAlwaysTrueGroups([&constraint_data, &compare_graph](const auto & group)
            {
                /// remove always true groups from CNF
                return !checkIfGroupAlwaysTrueFullMatch(group, constraint_data) && !checkIfGroupAlwaysTrueGraph(group, compare_graph);
            })
            .filterAlwaysFalseAtoms([&constraint_data, &compare_graph](const auto & atom)
            {
                /// remove always false atoms from CNF
                return !checkIfAtomAlwaysFalseFullMatch(atom, constraint_data) && !checkIfAtomAlwaysFalseGraph(atom, compare_graph);
            })
            .transformAtoms([&compare_graph](const auto & atom)
            {
                return replaceTermsToConstants(atom, compare_graph);
            })
            .reduce()
            .pushNotInFuntions();

        if (optimize_append_index)
            AddIndexConstraintsOptimizer(metadata_snapshot).perform(cnf);

        Poco::Logger::get("AFTER OPT").information(cnf.dump());
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, TreeCNFConverter::fromCNF(cnf));
    }
}

}
