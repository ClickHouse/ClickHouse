#include <Storages/MergeTree/MergeTreeIndexes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_EXCEPTION;
}

void MergeTreeIndexPart::update(const Block & block, const Names & column_names) {
    updateImpl(block, column_names);
}

void MergeTreeIndexPart::merge(const MergeTreeIndexPart & other) {
    if (other.indexType() != indexType()) {
        throw Exception("MergeTreeIndexPart: Merging index part with another index type.",
                        ErrorCodes::LOGICAL_ERROR);
    }
    mergeImpl(other);
}

INDEX_TYPE MergeTreeIndexPart::indexType() const {
    return owner->indexType();
}

INDEX_TYPE IndexCondition::indexType() const {
    return owner->indexType();
}


void MergeTreeIndexFactory::registerIndex(const std::string &name, Creator creator) {
    if (!indexes.emplace(name, std::move(creator)).second)
        throw Exception("MergeTreeIndexFactory: the Index creator name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

std::unique_ptr<MergeTreeIndex> MergeTreeIndexFactory::get(std::shared_ptr<ASTIndexDeclaration> node) const {
    if (!node->type)
        throw Exception(
                "for INDEX TYPE is required",
                ErrorCodes::INCORRECT_QUERY);
    auto it = indexes.find(node->type->name);
    if (it == indexes.end())
        throw Exception(
                "Unknown Index type '" + node->type->name + "'",
                ErrorCodes::INCORRECT_QUERY);
    return it->second(node);
}

}