#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <Core/Block.h>
#include <ext/singleton.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTIndexDeclaration.h>

namespace DB
{

class MergeTreeIndexPart
{
public:
    virtual ~MergeTreeIndexPart() {};

    virtual void update(const Block & block, const Names & column_names) = 0;
    virtual void merge(const MergeTreeIndexPart & other) = 0;
};

using MergeTreeIndexPartPtr = std::unique_ptr<MergeTreeIndexPart>;
using MergeTreeIndexParts = std::vector<MergeTreeIndexPartPtr>;


class MergeTreeIndex
{
public:
    MergeTreeIndex(String name, ExpressionActionsPtr expr, Block key)
            : name(name), expr(expr), sample(key) {}

    virtual ~MergeTreeIndex() {};

    virtual bool alwaysUnknownOrTrue() const = 0;
    virtual bool maybeTrue(/* args */) const = 0;

    virtual MergeTreeIndexPartPtr createEmptyIndexPart() const = 0;

    String name;
    ExpressionActionsPtr expr;
    Block sample;
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