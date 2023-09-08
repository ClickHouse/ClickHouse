#pragma once

#include <string>
#include <map>
#include <unordered_map>
#include <vector>
#include <memory>
#include <utility>
#include <mutex>
#include <Core/Block.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeLowCardinality.h>


constexpr auto INDEX_FILE_PREFIX = "skp_idx_";

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

class MMappedFile;

using MergeTreeIndexVersion = uint8_t;
struct MergeTreeIndexFormat
{
    MergeTreeIndexVersion version;
    const char* extension;

    /// Enabling either of these two flags disables compression and checksumming on the index file.
    /// By convention, such indexes should use .uidx file extension (u for uncompressed).
    /// Use viewFromSeekableFile() when possible.
    bool supports_view_from_seekable_file = false;
    /// Use viewFromMMappedFile() when possible.
    bool supports_view_from_mmapped_file = false;

    /// Disable compression and checksumming in the index file. The file contents will be exactly
    /// what IMergeTreeIndexGranule::serializeBinary() produced (concatenated for all index granules).
    bool plain_file() const
    {
        return supports_view_from_seekable_file || supports_view_from_mmapped_file;
    }

    explicit operator bool() const { return version != 0; }
};

/// Stores some info about a single block of data.
struct IMergeTreeIndexGranule
{
    virtual ~IMergeTreeIndexGranule() = default;

    /// Serialize always last version.
    virtual void serializeBinary(WriteBuffer & ostr) const = 0;

    /// Read the whole index into memory.
    /// Alternatively, viewFromSeekableFile() or viewFromMMappedFile() can be used for big indexes
    /// that don't require loading the entire index into memory.
    ///
    /// Version of the index is determined by getDeserializedFormat() based on file extension.
    /// Implementation is responsible for version check,
    /// and throw LOGICAL_ERROR in case of unsupported version.
    ///
    /// See also:
    /// - IMergeTreeIndex::getSerializedFormat()
    /// - IMergeTreeIndex::getDeserializedFormat()
    /// - MergeTreeDataMergerMutator::collectFilesForRenames()
    /// - IMergeTreeDataPart::hasSecondaryIndex()
    virtual void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) = 0;

    virtual bool empty() const = 0;

    virtual size_t memoryUsageBytes() const = 0;

    /// If supports_view_from_seekable_file is true, this method will be called instead
    /// of deserializeBinary(). Allows the index to read chunks index file on demand instead of loading
    /// the entire index into memory. The index granule will take ownership of the provided buffer and
    /// may hold it for a very long time while sitting in secondary index cache.
    virtual void viewFromSeekableFile(std::shared_ptr<SeekableReadBuffer>, size_t /* offset */, size_t /* length */, MergeTreeIndexVersion)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected viewFromSeekableFile() call");
    }

    /// If supports_view_from_mmapped_file is true, this method will be called instead
    /// of deserializeBinary(). Takes precedence over viewFromSeekableFile().
    /// The index can use this to mmap() the file (using MMappedFileCache).
    /// Useful if the index does lots of small random reads. In particular for usearch index, where
    /// search does many sequential steps through a graph, touching ~1K nodes.
    virtual void viewFromMMappedFile(std::shared_ptr<MMappedFile>, size_t /* offset */, size_t /* length */, MergeTreeIndexVersion)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected viewFromMMappedFile() call");
    }
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
    /// NOTE: In case getSerializedFormat() is reimplemented,
    /// getDeserializedFormat() should be reimplemented too,
    /// and check all previous extensions too
    /// (to avoid breaking backward compatibility).
    virtual MergeTreeIndexFormat getSerializedFormat() const { return {1, ".idx"}; }

    /// Returns extension for deserialization.
    ///
    /// Return pair<extension, version>.
    virtual MergeTreeIndexFormat getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & relative_path_prefix) const
    {
        if (data_part_storage.exists(relative_path_prefix + ".idx"))
            return {1, ".idx"};
        return {0 /*unknown*/, ""};
    }

    /// Checks whether the column is in data skipping index.
    virtual bool mayBenefitFromIndexForIn(const ASTPtr & node) const = 0;

    virtual MergeTreeIndexGranulePtr createIndexGranule() const = 0;

    virtual MergeTreeIndexAggregatorPtr createIndexAggregator() const = 0;

    virtual MergeTreeIndexAggregatorPtr createIndexAggregatorForPart([[maybe_unused]]const GinIndexStorePtr &store) const
    {
        return createIndexAggregator();
    }

    virtual MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query_info, ContextPtr context) const = 0;

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

MergeTreeIndexPtr bloomFilterIndexCreator(const IndexDescription & index);
void bloomFilterIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr bloomFilterIndexCreatorNew(const IndexDescription & index);
void bloomFilterIndexValidatorNew(const IndexDescription & index, bool attach);

MergeTreeIndexPtr hypothesisIndexCreator(const IndexDescription & index);
void hypothesisIndexValidator(const IndexDescription & index, bool attach);

#ifdef ENABLE_ANNOY
MergeTreeIndexPtr annoyIndexCreator(const IndexDescription & index);
void annoyIndexValidator(const IndexDescription & index, bool attach);
#endif

#ifdef ENABLE_USEARCH
MergeTreeIndexPtr usearchIndexCreator(const IndexDescription& index);
void usearchIndexValidator(const IndexDescription& index, bool attach);
#endif

MergeTreeIndexPtr invertedIndexCreator(const IndexDescription& index);
void invertedIndexValidator(const IndexDescription& index, bool attach);

}
