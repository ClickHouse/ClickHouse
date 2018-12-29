#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <Core/Block.h>
#include <ext/singleton.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTIndexDeclaration.h>

namespace DB
{

enum class INDEX_TYPE {
    NONE = 0
};


class MergeTreeIndex;

using MergeTreeIndexPtr = std::shared_ptr<MergeTreeIndex>;
using MergeTreeIndexes = std::vector<MergeTreeIndexPtr>;


/// Data structure storing some data for each MergeTreeDataPart
struct MergeTreeIndexPart
{
    friend MergeTreeIndex;

public:
    virtual ~MergeTreeIndexPart() = default;

    virtual INDEX_TYPE indexType() const;

    void update(const Block & block, const Names & column_names);
    void merge(const MergeTreeIndexPart & other);

protected:
    MergeTreeIndexPart() = default;

    virtual void updateImpl(const Block & block, const Names & column_names) = 0;
    virtual void mergeImpl(const MergeTreeIndexPart & other) = 0;

    MergeTreeIndexPtr owner;
};

using MergeTreeIndexPartPtr = std::shared_ptr<MergeTreeIndexPart>;
using MergeTreeIndexParts = std::vector<MergeTreeIndexPartPtr>;


/// Condition on the index.
class IndexCondition {
    friend MergeTreeIndex;

public:
    virtual ~IndexCondition() = default;

    virtual INDEX_TYPE indexType() const;

    // methods like KeyCondition
    virtual bool alwaysUnknownOrTrue() const = 0;
    virtual bool maybeTrueInRange(const MarkRange & range) const = 0;

protected:
    IndexCondition() = default;

    MergeTreeIndexPtr owner;
};

using IndexConditionPtr = std::shared_ptr<IndexCondition>;


/// Structure for storing index info like columns, expression, arguments, ...
class MergeTreeIndex
{
public:
    MergeTreeIndex(String name, ExpressionActionsPtr expr, Block key)
            : name(name), expr(expr), sample(key) {}

    virtual ~MergeTreeIndex() {};

    virtual INDEX_TYPE indexType() const = 0;

    virtual MergeTreeIndexPartPtr createEmptyIndexPart() const = 0;
    virtual IndexConditionPtr createIndexCondition(const SelectQueryInfo & query_info,
                                                const Context & context,
                                                const Names & key_column_names,
                                                const ExpressionActionsPtr & key_expr) const = 0;

    String name;
    ExpressionActionsPtr expr;
    Block sample;
};


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
    MergeTreeIndexFactory() = default;

private:
    using Indexes = std::unordered_map<std::string, Creator>;
    Indexes indexes;
};

}