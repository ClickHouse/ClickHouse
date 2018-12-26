#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Parsers/ASTIndexDeclaration.h>

namespace DB
{

class IMergeTreeIndex;
using MergeTreeIndexes = std::vector<IMergeTreeIndex>;


/// Interface for secondary MergeTree indexes
class IMergeTreeIndex
{
public:
    virtual void load(const MergeTreeData & storage, const String & part_path) = 0;
    virtual void store(const MergeTreeData & storage, const String & part_path,
                       MergeTreeDataPartChecksums & checksums) const = 0;

    virtual void update(const Block & block, const Names & column_names) = 0;
    virtual void merge(const IMergeTreeIndex & other) = 0;

    virtual bool alwaysUnknownOrTrue() const = 0;
    virtual bool maybeTrue() const = 0;

    String name;
    ExpressionActionsPtr expr;
    Block header;
};


class MergeTreeIndexFactory : public ext::singleton<MergeTreeIndexFactory>
{
    friend class ext::singleton<MergeTreeIndexFactory>;

public:
    using Creator = std::function<IMergeTreeIndex(const ASTIndexDeclaration & node)>;

protected:
    MergeTreeIndexFactory() {};

    IMergeTreeIndex get(const ASTIndexDeclaration & node) const;

    void registerIndex(const std::string & name, Creator creator);

    const auto & getAllIndexes() const {
        return indexes;
    }

private:
    using Indexes = std::unordered_map<std::string, Creator>;
    Indexes indexes;
};

}