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

constexpr auto INDEX_FILE_PREFIX = "skp_idx_";

namespace DB
{

class MergeTreeData;
class MergeTreeIndex;

using MergeTreeIndexPtr = std::shared_ptr<const MergeTreeIndex>;
using MutableMergeTreeIndexPtr = std::shared_ptr<MergeTreeIndex>;


struct MergeTreeIndexGranule
{
    virtual ~MergeTreeIndexGranule() = default;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual bool empty() const = 0;

    virtual void update(const Block & block, size_t * pos, size_t limit) = 0;
};


using MergeTreeIndexGranulePtr = std::shared_ptr<MergeTreeIndexGranule>;
using MergeTreeIndexGranules = std::vector<MergeTreeIndexGranulePtr>;

/// Condition on the index.
class IndexCondition {
public:
    IndexCondition() = default;
    virtual ~IndexCondition() = default;

    /// Checks if this index is useful for query.
    virtual bool alwaysUnknownOrTrue() const = 0;

    virtual bool mayBeTrueOnGranule(const MergeTreeIndexGranule & granule) const = 0;

    MergeTreeIndexPtr index;
};

using IndexConditionPtr = std::shared_ptr<IndexCondition>;


/// Structure for storing basic index info like columns, expression, arguments, ...
class MergeTreeIndex
{
public:
    MergeTreeIndex(String name, ExpressionActionsPtr expr, size_t granularity, Block key)
            : name(name), expr(expr), granularity(granularity), sample(key) {}

    virtual ~MergeTreeIndex() {};

    virtual String indexType() const { return "UNKNOWN"; };

    /// gets filename without extension
    String getFileName() const { return INDEX_FILE_PREFIX + name; };

    virtual MergeTreeIndexGranulePtr createIndexGranule() const = 0;

    virtual IndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query_info, const Context & context) const = 0;

    virtual void writeText(WriteBuffer & ostr) const = 0;

    String name;
    ExpressionActionsPtr expr;
    size_t granularity;
    Names columns;
    DataTypes data_types;
    Block sample;
};


using MergeTreeIndexes = std::vector<MutableMergeTreeIndexPtr>;


class MergeTreeIndexFactory : public ext::singleton<MergeTreeIndexFactory>
{
    friend class ext::singleton<MergeTreeIndexFactory>;

public:
    using Creator = std::function<
            std::unique_ptr<MergeTreeIndex>(
                    const MergeTreeData & data,
                    std::shared_ptr<ASTIndexDeclaration> node,
                    const Context & context)>;

    std::unique_ptr<MergeTreeIndex> get(
            const MergeTreeData & data,
            std::shared_ptr<ASTIndexDeclaration> node,
            const Context & context) const;

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