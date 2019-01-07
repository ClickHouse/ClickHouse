#include <Storages/MergeTree/MergeTreeIndexes.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <numeric>


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

std::unique_ptr<MergeTreeIndex> MergeTreeIndexFactory::get(
        const MergeTreeData & data,
        std::shared_ptr<ASTIndexDeclaration> node,
        const Context & context) const
{
    if (!node->type)
        throw Exception(
                "for INDEX TYPE is required",
                ErrorCodes::INCORRECT_QUERY);
    auto it = indexes.find(node->type->name);
    if (it == indexes.end())
        throw Exception(
                "Unknown Index type '" + node->type->name + "'. Available index types: " +
                std::accumulate(indexes.cbegin(), indexes.cend(), std::string{},
                        [] (auto && lft, const auto & rht) -> std::string {
                            if (lft == "") {
                                return rht.first;
                            } else {
                                return lft + ", " + rht.first;
                            }
                        }),
                ErrorCodes::INCORRECT_QUERY);
    return it->second(data, node, context);
}

}