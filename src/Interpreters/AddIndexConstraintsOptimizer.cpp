#include <Interpreters/AddIndexConstraintsOptimizer.h>

#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>

namespace DB
{

AddIndexConstraintsOptimizer::AddIndexConstraintsOptimizer(
    const StorageMetadataPtr & metadata_snapshot_)
    : metadata_snapshot(metadata_snapshot_)
{
}

namespace
{

    bool hasIndexColumns(const ASTPtr & ast, const std::unordered_set<std::string_view> & primary_key_set)
    {
        const auto * identifier = ast->as<ASTIdentifier>();
        if (identifier && primary_key_set.contains(identifier->name()))
            return true;
        for (auto & child : ast->children)
            if (hasIndexColumns(child, primary_key_set))
                return true;
        return false;
    }

    bool onlyIndexColumns(const ASTPtr & ast, const std::unordered_set<std::string_view> & primary_key_set)
    {
        const auto * identifier = ast->as<ASTIdentifier>();
        if (identifier && !primary_key_set.contains(identifier->name()))
            return false;
        for (auto & child : ast->children)
            if (!onlyIndexColumns(child, primary_key_set))
                return false;
        return true;
    }

    bool onlyConstants(const ASTPtr & ast)
    {
        const auto * identifier = ast->as<ASTIdentifier>();
        if (identifier)
            return false;
        for (auto & child : ast->children)
            if (!onlyConstants(child))
                return false;
        return true;
    }

    const std::unordered_map<std::string, ComparisonGraph::CompareResult> & getRelationMap()
    {
        const static std::unordered_map<std::string, ComparisonGraph::CompareResult> relations = {
            {"equals", ComparisonGraph::CompareResult::EQUAL},
            {"less", ComparisonGraph::CompareResult::LESS},
            {"lessOrEquals", ComparisonGraph::CompareResult::LESS_OR_EQUAL},
            {"greaterOrEquals", ComparisonGraph::CompareResult::GREATER_OR_EQUAL},
            {"greater", ComparisonGraph::CompareResult::GREATER},
        };
        return relations;
    }

    const std::unordered_map<ComparisonGraph::CompareResult, std::string> & getReverseRelationMap()
    {
        const static std::unordered_map<ComparisonGraph::CompareResult, std::string> relations = {
            {ComparisonGraph::CompareResult::EQUAL, "equals"},
            {ComparisonGraph::CompareResult::LESS, "less"},
            {ComparisonGraph::CompareResult::LESS_OR_EQUAL, "lessOrEquals"},
            {ComparisonGraph::CompareResult::GREATER_OR_EQUAL, "greaterOrEquals"},
            {ComparisonGraph::CompareResult::GREATER, "greater"},
        };
        return relations;
    }

    bool canBeSequence(const ComparisonGraph::CompareResult left, const ComparisonGraph::CompareResult right)
    {
        using CR = ComparisonGraph::CompareResult;
        if (left == CR::UNKNOWN || right == CR::UNKNOWN || left == CR::NOT_EQUAL || right == CR::NOT_EQUAL)
            return false;
        if ((left == CR::GREATER || left == CR::GREATER_OR_EQUAL) && (right == CR::LESS || right == CR::LESS_OR_EQUAL))
            return false;
        if ((right == CR::GREATER || right == CR::GREATER_OR_EQUAL) && (left == CR::LESS || left == CR::LESS_OR_EQUAL))
            return false;
        return true;
    }

    ComparisonGraph::CompareResult mostStrict(const ComparisonGraph::CompareResult left, const ComparisonGraph::CompareResult right)
    {
        using CR = ComparisonGraph::CompareResult;
        if (left == CR::LESS || left == CR::GREATER)
            return left;
        if (right == CR::LESS || right == CR::GREATER)
            return right;
        if (left == CR::LESS_OR_EQUAL || left == CR::GREATER_OR_EQUAL)
            return left;
        if (right == CR::LESS_OR_EQUAL || right == CR::GREATER_OR_EQUAL)
            return right;
        if (left == CR::EQUAL)
            return left;
        if (right == CR::EQUAL)
            return right;
        return CR::UNKNOWN;
    }

    /// Create OR-group for index_hint
    CNFQuery::OrGroup createIndexHintGroup(
        const CNFQuery::OrGroup & group,
        const ComparisonGraph & graph,
        const ASTs & primary_key_only_asts)
    {
        CNFQuery::OrGroup result;
        for (const auto & atom : group)
        {
            const auto * func = atom.ast->as<ASTFunction>();
            if (func && func->arguments->children.size() == 2 && getRelationMap().contains(func->name))
            {
                auto check_and_insert = [&](const size_t index, const ComparisonGraph::CompareResult need_result) -> bool
                {
                    if (!onlyConstants(func->arguments->children[1 - index]))
                        return false;

                    for (const auto & primary_key_ast : primary_key_only_asts)
                    {
                        ComparisonGraph::CompareResult actual_result;
                        if (index == 0)
                            actual_result = graph.compare(primary_key_ast, func->arguments->children[index]);
                        else
                            actual_result = graph.compare(func->arguments->children[index], primary_key_ast);

                        if (canBeSequence(need_result, actual_result))
                        {
                            ASTPtr helper_ast = func->clone();
                            auto * helper_func = helper_ast->as<ASTFunction>();
                            helper_func->name = getReverseRelationMap().at(mostStrict(need_result, actual_result));
                            helper_func->arguments->children[index] = primary_key_ast->clone();
                            result.insert(CNFQuery::AtomicFormula{atom.negative, helper_ast});
                            return true;
                        }
                    }
                    return false;
                };

                if (!check_and_insert(1, getRelationMap().at(func->name)) && !check_and_insert(0, getRelationMap().at(func->name)))
                    return {};
            }
        }

        return result;
    }
}

void AddIndexConstraintsOptimizer::perform(CNFQuery & cnf_query)
{
    const auto primary_key = metadata_snapshot->getColumnsRequiredForPrimaryKey();
    const auto & graph = metadata_snapshot->getConstraints().getGraph();
    const std::unordered_set<std::string_view> primary_key_set(std::begin(primary_key), std::end(primary_key));

    ASTs primary_key_only_asts;
    for (const auto & vertex : graph.getVertices())
        for (const auto & ast : vertex)
            if (hasIndexColumns(ast, primary_key_set) && onlyIndexColumns(ast, primary_key_set))
                primary_key_only_asts.push_back(ast);

    CNFQuery::AndGroup and_group;
    cnf_query.iterateGroups([&and_group, &graph, &primary_key_only_asts](const auto & or_group)
    {
        auto add_group = createIndexHintGroup(or_group, graph, primary_key_only_asts);
        if (!add_group.empty())
            and_group.emplace(std::move(add_group));
    });
    if (!and_group.empty())
    {
        CNFQuery::OrGroup new_or_group;
        new_or_group.insert(CNFQuery::AtomicFormula{false, makeASTFunction("indexHint", TreeCNFConverter::fromCNF(CNFQuery(std::move(and_group))))});
        cnf_query.appendGroup(CNFQuery::AndGroup{new_or_group});
    }
}

}
