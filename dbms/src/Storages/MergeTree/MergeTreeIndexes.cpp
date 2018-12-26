#include <Storages/MergeTree/MergeTreeIndexes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

void MergeTreeIndexFactory::registerIndex(const std::string &name, Creator creator) {
    if (!indexes.emplace(name, std::move(creator)).second)
        throw Exception("MergeTreeIndexFactory: the Index creator name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

IMergeTreeIndex MergeTreeIndexFactory::get(const ASTIndexDeclaration & node) const {
    if (!node.type)
        throw Exception(
                "for INDEX TYPE is required",
                ErrorCodes::INCORRECT_QUERY);
    auto it = indexes.find(node.type->name);
    if (it == indexes.end())
        throw Exception(
                "Unknown Index type '" + node.type->name + "'",
                ErrorCodes::INCORRECT_QUERY);
    return it->second(node);
}

}