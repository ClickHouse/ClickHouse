#include <Interpreters/WhereConstraintsOptimizer.h>

#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/AddIndexConstraintsOptimizer.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>

#include <Parsers/queryToString.h>

namespace DB
{

WhereConstraintsOptimizer::WhereConstraintsOptimizer(
    ASTSelectQuery * select_query_,
    const StorageMetadataPtr & metadata_snapshot_,
    bool optimize_append_index_)
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

MatchState match(CNFQuery::AtomicFormula a, CNFQuery::AtomicFormula b)
{
    bool match_means_ok = (a.negative == b.negative);
    if (a.ast->getTreeHash() == b.ast->getTreeHash())
        return match_means_ok ? MatchState::FULL_MATCH : MatchState::NOT_MATCH;

    return MatchState::NONE;
}

bool checkIfGroupAlwaysTrueFullMatch(const CNFQuery::OrGroup & group, const ConstraintsDescription & constraints_description)
{
    /// We have constraints in CNF.
    /// CNF is always true => Each OR group in CNF is always true.
    /// So, we try to check whether we have al least one OR group from CNF as subset in our group.
    /// If we've found one then our group is always true too.

    const auto & constraints_data = constraints_description.getConstraintData();
    std::vector<size_t> found(constraints_data.size());
    for (size_t i = 0; i < constraints_data.size(); ++i)
        found[i] = constraints_data[i].size();

    for (const auto & atom : group)
    {
        const auto constraint_atom_ids = constraints_description.getAtomIds(atom.ast);
        if (constraint_atom_ids)
        {
            const auto constraint_atoms = constraints_description.getAtomsById(*constraint_atom_ids);
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

bool checkIfGroupAlwaysTrueGraph(const CNFQuery::OrGroup & group, const ComparisonGraph & graph)
{
    /// We try to find at least one atom that is always true by using comparison graph.
    for (const auto & atom : group)
    {
        const auto * func = atom.ast->as<ASTFunction>();
        if (func && func->arguments->children.size() == 2)
        {
            const auto expected = ComparisonGraph::atomToCompareResult(atom);
            if (graph.isAlwaysCompare(expected, func->arguments->children.front(), func->arguments->children.back()))
                return true;
        }
    }

    return false;
}


bool checkIfAtomAlwaysFalseFullMatch(const CNFQuery::AtomicFormula & atom, const ConstraintsDescription & constraints_description)
{
    const auto constraint_atom_ids = constraints_description.getAtomIds(atom.ast);
    if (constraint_atom_ids)
    {
        for (const auto & constraint_atom : constraints_description.getAtomsById(*constraint_atom_ids))
        {
            const auto match_result = match(constraint_atom, atom);
            if (match_result == MatchState::NOT_MATCH)
                return true;
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
        const auto expected = ComparisonGraph::atomToCompareResult(atom);
        return !graph.isPossibleCompare(expected, func->arguments->children.front(), func->arguments->children.back());
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

}

void WhereConstraintsOptimizer::perform()
{
    if (select_query->where() && metadata_snapshot)
    {
        const auto & compare_graph = metadata_snapshot->getConstraints().getGraph();
        auto cnf = TreeCNFConverter::toCNF(select_query->where());
        cnf.pullNotOutFunctions()
            .filterAlwaysTrueGroups([&compare_graph, this](const auto & group)
            {
                /// remove always true groups from CNF
                return !checkIfGroupAlwaysTrueFullMatch(group, metadata_snapshot->getConstraints()) && !checkIfGroupAlwaysTrueGraph(group, compare_graph);
            })
            .filterAlwaysFalseAtoms([&compare_graph, this](const auto & atom)
            {
                /// remove always false atoms from CNF
                return !checkIfAtomAlwaysFalseFullMatch(atom, metadata_snapshot->getConstraints()) && !checkIfAtomAlwaysFalseGraph(atom, compare_graph);
            })
            .transformAtoms([&compare_graph](const auto & atom)
            {
                return replaceTermsToConstants(atom, compare_graph);
            })
            .reduce()
            .pushNotInFuntions();

        if (optimize_append_index)
            AddIndexConstraintsOptimizer(metadata_snapshot).perform(cnf);

        select_query->setExpression(ASTSelectQuery::Expression::WHERE, TreeCNFConverter::fromCNF(cnf));
    }
}

}
