#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <numeric>

#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_EXCEPTION;
}

void MergeTreeIndexFactory::registerIndex(const std::string &name, Creator creator)
{
    if (!indexes.emplace(name, std::move(creator)).second)
        throw Exception("MergeTreeIndexFactory: the Index creator name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

std::unique_ptr<IMergeTreeIndex> MergeTreeIndexFactory::get(
    const NamesAndTypesList & columns,
    std::shared_ptr<ASTIndexDeclaration> node,
    const Context & context) const
{
    if (!node->type)
        throw Exception(
                "for index TYPE is required", ErrorCodes::INCORRECT_QUERY);
    if (node->type->parameters && !node->type->parameters->children.empty())
        throw Exception(
                "Index type can not have parameters", ErrorCodes::INCORRECT_QUERY);

    boost::algorithm::to_lower(node->type->name);
    auto it = indexes.find(node->type->name);
    if (it == indexes.end())
        throw Exception(
                "Unknown Index type '" + node->type->name + "'. Available index types: " +
                std::accumulate(indexes.cbegin(), indexes.cend(), std::string{},
                        [] (auto && lft, const auto & rht) -> std::string {
                            if (lft == "")
                                return rht.first;
                            else
                                return lft + ", " + rht.first;
                        }),
                ErrorCodes::INCORRECT_QUERY);
    return it->second(columns, node, context);
}


std::unique_ptr<IMergeTreeIndex> minmaxIndexCreator(
        const NamesAndTypesList & columns,
        std::shared_ptr<ASTIndexDeclaration> node,
        const Context & context);

std::unique_ptr<IMergeTreeIndex> setIndexCreator(
        const NamesAndTypesList & columns,
        std::shared_ptr<ASTIndexDeclaration> node,
        const Context & context);

std::unique_ptr<IMergeTreeIndex> bloomFilterIndexCreator(
        const NamesAndTypesList & columns,
        std::shared_ptr<ASTIndexDeclaration> node,
        const Context & context);


MergeTreeIndexFactory::MergeTreeIndexFactory()
{
    registerIndex("minmax", minmaxIndexCreator);
    registerIndex("set", setIndexCreator);
    registerIndex("ngrambf_v1", bloomFilterIndexCreator);
    registerIndex("tokenbf_v1", bloomFilterIndexCreator);
}

}
