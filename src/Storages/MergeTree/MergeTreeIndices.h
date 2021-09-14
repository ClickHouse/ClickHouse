#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <utility>
#include <Core/Block.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeLowCardinality.h>

constexpr auto INDEX_FILE_PREFIX = "skp_idx_";

namespace DB
{

using MergeTreeIndexVersion = uint8_t;
struct MergeTreeIndexFormat
{
    MergeTreeIndexVersion version;
    const char* extension;

    operator bool() const { return version != 0; }
};

/// Stores some info about a single block of data.
struct IMergeTreeIndexGranule
{
    virtual ~IMergeTreeIndexGranule() = default;

    /// Serialize always last version.
    virtual void serializeBinary(WriteBuffer & ostr) const = 0;

    /// Version of the index to deserialize:
    ///
    /// - 2 -- minmax index for proper Nullable support,
    /// - 1 -- everything else.
    ///
    /// Implementation is responsible for version check,
    /// and throw LOGICAL_ERROR in case of unsupported version.
    ///
    /// See also:
    /// - IMergeTreeIndex::getSerializedFileExtension()
    /// - IMergeTreeIndex::getDeserializedFormat()
    /// - MergeTreeDataMergerMutator::collectFilesToSkip()
    /// - MergeTreeDataMergerMutator::collectFilesForRenames()
    virtual void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) = 0;

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


struct IMergeTreeIndex
{
    IMergeTreeIndex(const IndexDescription & index_)
        : index(index_)
    {
    }

    virtual ~IMergeTreeIndex() = default;

    /// Returns filename without extension.
    String getFileName() const { return INDEX_FILE_PREFIX + index.name; }

    /// Returns extension for serialization.
    /// Reimplement if you want new index format.
    ///
    /// NOTE: In case getSerializedFileExtension() is reimplemented,
    /// getDeserializedFormat() should be reimplemented too,
    /// and check all previous extensions too
    /// (to avoid breaking backward compatibility).
    virtual const char* getSerializedFileExtension() const { return ".idx"; }

    /// Returns extension for deserialization.
    ///
    /// Return pair<extension, version>.
    virtual MergeTreeIndexFormat getDeserializedFormat(const DiskPtr, const std::string & /* relative_path_prefix */) const
    {
        return {1, ".idx"};
    }

    /// Checks whether the column is in data skipping index.
    virtual bool mayBenefitFromIndexForIn(const ASTPtr & node) const = 0;

    virtual MergeTreeIndexGranulePtr createIndexGranule() const = 0;

    virtual MergeTreeIndexAggregatorPtr createIndexAggregator() const = 0;

    virtual MergeTreeIndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query_info, ContextPtr context) const = 0;

    Names getColumnsRequiredForIndexCalc() const { return index.expression->getRequiredColumns(); }

    const IndexDescription & index;
};

using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;
using MergeTreeIndices = std::vector<MergeTreeIndexPtr>;


class MergeTreeIndexFactory : private boost::noncopyable
{
public:
    static MergeTreeIndexFactory & instance();

    using Creator = std::function<MergeTreeIndexPtr(const IndexDescription & index)>;

    using Validator = std::function<void(const IndexDescription & index, bool attach)>;

    void validate(const IndexDescription & index, bool attach) const;

    MergeTreeIndexPtr get(const IndexDescription & index) const;

    MergeTreeIndices getMany(const std::vector<IndexDescription> & indices) const;

    void registerCreator(const std::string & index_type, Creator creator);
    void registerValidator(const std::string & index_type, Validator validator);

protected:
    MergeTreeIndexFactory();

private:
    using Creators = std::unordered_map<std::string, Creator>;
    using Validators = std::unordered_map<std::string, Validator>;
    Creators creators;
    Validators validators;
};

MergeTreeIndexPtr minmaxIndexCreator(const IndexDescription & index);
void minmaxIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr setIndexCreator(const IndexDescription & index);
void setIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr bloomFilterIndexCreator(const IndexDescription & index);
void bloomFilterIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr bloomFilterIndexCreatorNew(const IndexDescription & index);
void bloomFilterIndexValidatorNew(const IndexDescription & index, bool attach);

}
