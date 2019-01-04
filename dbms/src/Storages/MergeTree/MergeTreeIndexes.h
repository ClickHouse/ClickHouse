#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <Core/Block.h>
#include <ext/singleton.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTIndexDeclaration.h>

namespace DB
{

enum class IndexType {
    NONE = 0
};


class MergeTreeIndex;
struct MergeTreeIndexPart;

using MergeTreeIndexPtr = std::shared_ptr<const MergeTreeIndex>;
using MutableMergeTreeIndexPtr = std::shared_ptr<MergeTreeIndex>;
using MergeTreeIndexes = std::vector<MutableMergeTreeIndexPtr>;

using MergeTreeIndexPartPtr = std::shared_ptr<MergeTreeIndexPart>;
using MergeTreeIndexParts = std::vector<MergeTreeIndexPartPtr>;


/// Condition on the index.
/// It works only with one indexPart (MergeTreeDataPart).
class IndexCondition {
    friend MergeTreeIndexPart;

public:
    virtual ~IndexCondition() = default;

    IndexType indexType() const;

    /// Checks if this index is useful for query.
    virtual bool alwaysUnknownOrTrue() const = 0;

    /// Drops out ranges where query is false
    virtual MarkRanges filterRanges(const MarkRanges & ranges) const = 0;

protected:
    IndexCondition() = default;

public:
    MergeTreeIndexPartPtr part;
};

using IndexConditionPtr = std::shared_ptr<IndexCondition>;


struct MergeTreeIndexGranule
{
    friend MergeTreeIndexPart;

public:
    virtual ~MergeTreeIndexGranule();

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) const = 0;

    virtual bool empty() const = 0;

    virtual void update(const Block & block, size_t * pos, size_t limit) = 0;
};

using MergeTreeIndexGranulePtr = std::shared_ptr<MergeTreeIndexGranule>;
using MergeTreeIndexGranules = std::vector<MergeTreeIndexGranulePtr>;


/// Data structure for operations with index data for each MergeTreeDataPart.
/// Stores information specific for DataPart.
/// Возможно будет убран.
struct MergeTreeIndexPart
{
    friend MergeTreeIndex;

public:
    MergeTreeIndexPart() = default;
    virtual ~MergeTreeIndexPart() = default;

    IndexType indexType() const;

    virtual MergeTreeIndexPartPtr cloneEmpty() const = 0;

    virtual IndexConditionPtr createIndexConditionOnPart(
            const SelectQueryInfo & query_info, const Context & context) const = 0;

    MergeTreeIndexPtr index;
};


/// Structure for storing basic index info like columns, expression, arguments, ...
class MergeTreeIndex
{
public:
    MergeTreeIndex(String name, ExpressionActionsPtr expr, size_t granularity, Block key)
            : name(name), expr(expr), granularity(granularity), sample(key) {}

    virtual ~MergeTreeIndex() {};

    virtual IndexType indexType() const = 0;

    virtual MergeTreeIndexGranulePtr createIndexGranule() const = 0;

    String name;
    ExpressionActionsPtr expr;
    size_t granularity;
    Names columns;
    DataTypes data_types;
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