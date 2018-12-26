#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <Core/Block.h>
#include <ext/singleton.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Parsers/ASTIndexDeclaration.h>

namespace DB
{


/// Interface for secondary MergeTree indexes
class MergeTreeIndex
{
public:
    virtual ~MergeTreeIndex() {};

    virtual void load(const String & part_path) = 0;
    virtual void store(const String & part_path, MergeTreeDataPartChecksums & checksums) const = 0;

    virtual void update(const Block & block, const Names & column_names) = 0;
    virtual void merge(const MergeTreeIndex & other) = 0;

    virtual bool alwaysUnknownOrTrue() const = 0;
    virtual bool maybeTrue() const = 0;

    String name;
    ExpressionActionsPtr expr;
    Block header;
};

using MergeTreeIndexPtr = std::unique_ptr<MergeTreeIndex>;
using MergeTreeIndexes = std::vector<MergeTreeIndexPtr>;


class MergeTreeIndexFactory : public ext::singleton<MergeTreeIndexFactory>
{
    friend class ext::singleton<MergeTreeIndexFactory>;

public:
    using Creator = std::function<std::unique_ptr<MergeTreeIndex>(std::shared_ptr<ASTIndexDeclaration> node)>;

    std::unique_ptr<MergeTreeIndex> get(std::shared_ptr<ASTIndexDeclaration> node) const;

    void registerIndex(const std::string & name, Creator creator);

    const auto & getAllIndexes() const {
        return indexes;
    }

protected:
    MergeTreeIndexFactory() {};

private:
    using Indexes = std::unordered_map<std::string, Creator>;
    Indexes indexes;
};

}