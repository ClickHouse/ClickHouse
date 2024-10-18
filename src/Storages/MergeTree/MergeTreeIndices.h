#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <Core/Block.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include "config.h"

constexpr auto INDEX_FILE_PREFIX = "skp_idx_";

namespace DB
{

struct MergeTreeWriterSettings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

using MergeTreeIndexVersion = uint8_t;
struct MergeTreeIndexFormat
{
    MergeTreeIndexVersion version;
    const char* extension;

    explicit operator bool() const { return version != 0; }
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

    /// Special method for vector similarity indexes:
    /// Returns the row positions of the N nearest neighbors in the index granule
    /// The returned row numbers are guaranteed to be sorted and unique.
    virtual std::vector<UInt64> calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "calculateApproximateNearestNeighbors is not implemented for non-vector-similarity indexes");
    }
};

using MergeTreeIndexConditionPtr = std::shared_ptr<IMergeTreeIndexCondition>;
using MergeTreeIndexConditions = std::vector<MergeTreeIndexConditionPtr>;

struct IMergeTreeIndex;
using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;

/// IndexCondition that checks several indexes at the same time.
class IMergeTreeIndexMergedCondition
{
public:
    explicit IMergeTreeIndexMergedCondition(size_t granularity_)
        : granularity(granularity_)
    {
    }

    virtual ~IMergeTreeIndexMergedCondition() = default;

    virtual void addIndex(const MergeTreeIndexPtr & index) = 0;
    virtual bool alwaysUnknownOrTrue() const = 0;
    virtual bool mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const = 0;

protected:
    const size_t granularity;
};

using MergeTreeIndexMergedConditionPtr = std::shared_ptr<IMergeTreeIndexMergedCondition>;
using MergeTreeIndexMergedConditions = std::vector<IMergeTreeIndexMergedCondition>;


struct IMergeTreeIndex
{
    explicit IMergeTreeIndex(const IndexDescription & index_)
        : index(index_)
    {
    }

    virtual ~IMergeTreeIndex() = default;

    /// Returns filename without extension.
    String getFileName() const { return INDEX_FILE_PREFIX + index.name; }
    size_t getGranularity() const { return index.granularity; }

    virtual bool isMergeable() const { return false; }

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
    virtual MergeTreeIndexFormat getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & relative_path_prefix) const
    {
        if (data_part_storage.existsFile(relative_path_prefix + ".idx"))
            return {1, ".idx"};
        return {0 /*unknown*/, ""};
    }

    virtual MergeTreeIndexGranulePtr createIndexGranule() const = 0;

    virtual MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const = 0;

    virtual MergeTreeIndexAggregatorPtr createIndexAggregatorForPart(const GinIndexStorePtr & /*store*/, const MergeTreeWriterSettings & settings) const
    {
        return createIndexAggregator(settings);
    }

    virtual MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG * filter_actions_dag, ContextPtr context) const = 0;

    virtual bool isVectorSimilarityIndex() const { return false; }

    virtual MergeTreeIndexMergedConditionPtr createIndexMergedCondition(
        const SelectQueryInfo & /*query_info*/, StorageMetadataPtr /*storage_metadata*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "MergedCondition is not implemented for index of type {}", index.type);
    }

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

MergeTreeIndexPtr bloomFilterIndexTextCreator(const IndexDescription & index);
void bloomFilterIndexTextValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr bloomFilterIndexCreator(const IndexDescription & index);
void bloomFilterIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr hypothesisIndexCreator(const IndexDescription & index);
void hypothesisIndexValidator(const IndexDescription & index, bool attach);

#if USE_USEARCH
MergeTreeIndexPtr vectorSimilarityIndexCreator(const IndexDescription & index);
void vectorSimilarityIndexValidator(const IndexDescription & index, bool attach);
#endif

MergeTreeIndexPtr legacyVectorSimilarityIndexCreator(const IndexDescription & index);
void legacyVectorSimilarityIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr fullTextIndexCreator(const IndexDescription & index);
void fullTextIndexValidator(const IndexDescription & index, bool attach);

}
