#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <Core/Block.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <DataTypes/DataTypeLowCardinality.h>

constexpr auto INDEX_FILE_PREFIX = "skp_idx_";

namespace DB
{

class MergeTreeData;
class IMergeTreeIndex;

using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;
using MutableMergeTreeIndexPtr = std::shared_ptr<IMergeTreeIndex>;


/// Stores some info about a single block of data.
struct IMergeTreeIndexGranule
{
    virtual ~IMergeTreeIndexGranule() = default;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;

    virtual bool empty() const = 0;
};

using MergeTreeIndexGranulePtr = std::shared_ptr<IMergeTreeIndexGranule>;
using MergeTreeIndexGranules = std::vector<MergeTreeIndexGranulePtr>;


/// Aggregates info about a single block of data.
struct IMergeTreeIndexAggregator
{
    virtual ~IMergeTreeIndexAggregator() = default;

    virtual bool empty() const = 0;
    virtual MergeTreeIndexGranulePtr getGranuleAndReset() = 0;

    /// Updates the stored info using rows of the specified block.
    /// Reads no more than `limit` rows.
    /// After finishing updating `pos` will store the position of the first row which was not read.
    virtual void update(const Block & block, size_t * pos, size_t limit) = 0;
};

using MergeTreeIndexAggregatorPtr = std::shared_ptr<IMergeTreeIndexAggregator>;
using MergeTreeIndexAggregators = std::vector<MergeTreeIndexAggregatorPtr>;


/// Condition on the index.
class IMergeTreeIndexCondition
{
public:
    virtual ~IMergeTreeIndexCondition() = default;
    /// Checks if this index is useful for query.
    virtual bool alwaysUnknownOrTrue() const = 0;

    virtual bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const = 0;
};

using MergeTreeIndexConditionPtr = std::shared_ptr<IMergeTreeIndexCondition>;


/// Structure for storing basic index info like columns, expression, arguments, ...
class IMergeTreeIndex
{
public:
    IMergeTreeIndex(
        String name_,
        ExpressionActionsPtr expr_,
        const Names & columns_,
        const DataTypes & data_types_,
        const Block & header_,
        size_t granularity_)
        : name(name_)
        , expr(expr_)
        , columns(columns_)
        , data_types(data_types_)
        , header(header_)
        , granularity(granularity_) {}

    virtual ~IMergeTreeIndex() = default;

    /// gets filename without extension
    String getFileName() const { return INDEX_FILE_PREFIX + name; }

    /// Checks whether the column is in data skipping index.
    virtual bool mayBenefitFromIndexForIn(const ASTPtr & node) const = 0;

    virtual MergeTreeIndexGranulePtr createIndexGranule() const = 0;
    virtual MergeTreeIndexAggregatorPtr createIndexAggregator() const = 0;

    virtual MergeTreeIndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query_info, const Context & context) const = 0;

    Names getColumnsRequiredForIndexCalc() const { return expr->getRequiredColumns(); }

    /// Index name
    String name;

    /// Index expression (x * y)
    /// with columns arguments
    ExpressionActionsPtr expr;

    /// Names of columns for index
    Names columns;

    /// Data types of columns
    DataTypes data_types;

    /// Block with columns and data_types
    Block header;

    /// Skip index granularity
    size_t granularity;
};

using MergeTreeIndices = std::vector<MergeTreeIndexPtr>;


class MergeTreeIndexFactory : private boost::noncopyable
{
public:
    static MergeTreeIndexFactory & instance();

    using Creator = std::function<
            std::unique_ptr<IMergeTreeIndex>(
                    const NamesAndTypesList & columns,
                    std::shared_ptr<ASTIndexDeclaration> node,
                    const Context & context,
                    bool attach)>;

    std::unique_ptr<IMergeTreeIndex> get(
        const NamesAndTypesList & columns,
        std::shared_ptr<ASTIndexDeclaration> node,
        const Context & context,
        bool attach) const;

    void registerIndex(const std::string & name, Creator creator);

    const auto & getAllIndexes() const { return indexes; }

protected:
    MergeTreeIndexFactory();

private:
    using Indexes = std::unordered_map<std::string, Creator>;
    Indexes indexes;
};

std::unique_ptr<IMergeTreeIndex> minmaxIndexCreator(
    const NamesAndTypesList & columns,
    std::shared_ptr<ASTIndexDeclaration> node,
    const Context & context,
    bool attach);

std::unique_ptr<IMergeTreeIndex> setIndexCreator(
    const NamesAndTypesList & columns,
    std::shared_ptr<ASTIndexDeclaration> node,
    const Context & context,
    bool attach);

std::unique_ptr<IMergeTreeIndex> bloomFilterIndexCreator(
    const NamesAndTypesList & columns,
    std::shared_ptr<ASTIndexDeclaration> node,
    const Context & context,
    bool attach);

std::unique_ptr<IMergeTreeIndex> bloomFilterIndexCreatorNew(
    const NamesAndTypesList & columns,
    std::shared_ptr<ASTIndexDeclaration> node,
    const Context & context,
    bool attach);

}
