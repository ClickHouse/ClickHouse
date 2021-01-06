#include <Interpreters/WhereConstraintsOptimizer.h>

#include <Interpreters/TreeCNFConverter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>

namespace DB
{
std::vector<std::vector<ASTPtr>> getConstraintData(const StorageMetadataPtr & metadata_snapshot)
{
    std::vector<std::vector<ASTPtr>> constraint_data;
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

MatchState match(ASTPtr a, ASTPtr b)
{
    bool match_means_ok = true;

    {
        auto * func_a = a->as<ASTFunction>();
        if (func_a && func_a->name == "not")
        {
            a = func_a->arguments->children.front();
            match_means_ok ^= true;
        }
    }
    {
        auto * func_b = b->as<ASTFunction>();
        if (func_b && func_b->name == "not")
        {
            b = func_b->arguments->children.front();
            match_means_ok ^= true;
        }
    }

    if (a->getTreeHash() == b->getTreeHash() &&
        a->getColumnName() == b->getColumnName())
    {
        return match_means_ok ? MatchState::FULL_MATCH : MatchState::NOT_MATCH;
    }
    return MatchState::NONE;
}

bool checkIfGroupAlwaysTrue(const CNFQuery::OrGroup & group, const std::vector<std::vector<ASTPtr>> & constraints)
{
    /// TODO: this is temporary; need to write more effective search
    /// TODO: go deeper into asts (a < b, a = b,...) with z3 or some visitor
    for (const auto & constraint : constraints) /// one constraint in group is enough,
                                                /// otherwise it's difficult to make judgements without using constraint solving (z3..)
    {
        bool group_always_true = true;
        for (const auto & constraint_ast : constraint)
        {
            bool found_match = false;
            for (const auto & group_ast : group)
            {
                const auto match_result = match(constraint_ast, group_ast);

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

bool checkIfAtomAlwaysFalse(const ASTPtr & atom, const std::vector<std::vector<ASTPtr>> & constraints)
{
    /// TODO: more efficient matching

    for (const auto & constraint : constraints)
    {
        if (constraint.size() > 1)
            continue; /// TMP

        for (const auto & constraint_ast : constraint)
        {
            const auto match_result = match(constraint_ast, atom);

            if (match_result != MatchState::NONE)
                return match_result == MatchState::NOT_MATCH;
        }
    }

    return false;
}

void WhereConstraintsOptimizer::perform()
{
    if (select_query->where() && metadata_snapshot)
    {
        const auto constraint_data = getConstraintData(metadata_snapshot);
        auto cnf = TreeCNFConverter::toCNF(select_query->where());
        Poco::Logger::get("BEFORE OPT").information(cnf.dump());
        cnf.pullNotOutFunctions()
            .filterAlwaysTrueGroups([&constraint_data](const auto & group) { /// remove always true groups from CNF
                return !checkIfGroupAlwaysTrue(group, constraint_data);
            })
            .filterAlwaysFalseAtoms([&constraint_data](const auto & ast) { /// remove always false atoms from CNF
                return !checkIfAtomAlwaysFalse(ast, constraint_data);
            })
            .pushNotInFuntions();

        Poco::Logger::get("AFTER OPT").information(cnf.dump());
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, TreeCNFConverter::fromCNF(cnf));
    }
}

}
