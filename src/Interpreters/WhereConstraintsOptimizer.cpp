#include <Interpreters/WhereConstraintsOptimizer.h>

#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ConstraintMatcherVisitor.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{
std::vector<std::vector<ASTPtr>> getConstraintData(const StorageMetadataPtr & metadata_snapshot)
{
    std::vector<std::vector<ASTPtr>> constraint_data;

    for (const auto & constraint :
         metadata_snapshot->getConstraints().filterConstraints(ConstraintsDescription::ConstraintType::ALWAYS_TRUE))
    {
        const auto cnf = TreeCNFConverter::toCNF(constraint->as<ASTConstraintDeclaration>()->expr->ptr())
                             .pullNotOutFunctions();
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
                bool match_means_ok = true;
                ASTPtr a = constraint_ast;
                ASTPtr b = group_ast;

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
                    found_match = match_means_ok;
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
            continue; /// TMP; Too hard to do something at current time (without more powerful instruments)

        for (const auto & constraint_ast : constraint)
        {
            bool match_means_ok = true;
            ASTPtr a = constraint_ast;
            ASTPtr b = atom;

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

            Poco::Logger::get("MATCHER a").information(a->dumpTree());
            Poco::Logger::get("MATCHER b").information(b->dumpTree());
            Poco::Logger::get("MATCHER a>>").information(a->getColumnName());
            Poco::Logger::get("MATCHER b>>" ).information(b->getColumnName());
            if (a->getTreeHash() == b->getTreeHash() &&
                a->getColumnName() == b->getColumnName())
            {
                Poco::Logger::get("MATCH").information(std::to_string(static_cast<int>(match_means_ok)));
                return !match_means_ok;
            }
        }
    }

    return false;
}

void WhereConstraintsOptimizer::perform()
{
    auto constraint_data = getConstraintData(metadata_snapshot);

    if (select_query->where())
    {
        auto cnf = TreeCNFConverter::toCNF(select_query->where());

        cnf.pullNotOutFunctions()
            .filterGroups([&constraint_data](const auto & group)
                          { return !checkIfGroupAlwaysTrue(group, constraint_data); }) /// remove always true functions in CNF
            .filterAtoms([&constraint_data](const auto & ast)
                         { return !checkIfAtomAlwaysFalse(ast, constraint_data); }) /// TODO: remove always false atoms in CNF
            .pushNotInFuntions();

        //ConstraintMatcherVisitor(constraint_data).visit(select_query->refWhere());
        Poco::Logger::get("AFTER OPT").information(cnf.dump());
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, TreeCNFConverter::fromCNF(cnf));
    }
}

}
