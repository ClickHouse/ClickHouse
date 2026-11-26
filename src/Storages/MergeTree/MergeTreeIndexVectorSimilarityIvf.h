#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IvfTree.h>
#include <Storages/MergeTree/IvfTreeStore.h>
#include <Common/Logger.h>

#include <pcg_random.hpp>

#include <memory>
#include <mutex>
#include <vector>

namespace DB
{
/// vector_similarity('ivf', 'L2Distance' | 'cosineDistance', dimensions[, lists]): lists of 4-bit codes of rotated residuals,
/// reached through a tree of centroids (see `IvfTree`), and the exact distances of the best candidates. The tree is one per
/// table, kept in a table of its own (see `IvfTreeStore`): the first build trains it, every part encodes its rows by it.
enum class IvfMetric : uint8_t
{
    L2 = 0,
    Cosine = 1,
};

struct IvfParams
{
    size_t lists = 0;
};

class TemporaryDataBuffer;

struct IvfListBucket
{
    size_t first_list = 0;
    size_t end_list = 0;
    std::shared_ptr<TemporaryDataBuffer> records;
};

struct MergeTreeIndexGranuleIvf final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleIvf(const String & index_name_, IvfMetric metric_, size_t dimensions_);
    ~MergeTreeIndexGranuleIvf() override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;

    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return rows == 0; }
    size_t memoryUsageBytes() const override;

    NearestNeighbours search(const std::vector<float> & query, size_t limit, size_t probes,
        const MergeTreeReaderSettings & reader_settings, bool return_distances) const;

    const String index_name;
    const IvfMetric metric;
    size_t dimensions;
    size_t rows = 0;
    size_t lists = 0;
    /// The lists are the leaves of the tree of the table, which this version of it is.
    std::shared_ptr<const IvfTree> tree;
    UInt64 tree_version = 0;
    std::vector<UInt64> list_offsets;

    std::vector<float> rotation;
    std::vector<float> rotated_centroids;

    std::vector<IvfListBucket> buckets;

private:
    static constexpr UInt64 FILE_FORMAT_VERSION = 6;

    size_t entryBytes() const;
    size_t entries() const { return list_offsets.empty() ? 0 : list_offsets.back(); }

    UInt64 lists_byte_begin = 0;

    DataPartStoragePtr data_part_storage;
    String index_file_name;

    LoggerPtr logger = getLogger("VectorSimilarityIndexIvf");
};

struct MergeTreeIndexAggregatorIvf final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorIvf(const String & index_name_, const Block & index_sample_block_, size_t dimensions_, IvfMetric metric_,
        IvfParams params_, String table_);
    ~MergeTreeIndexAggregatorIvf() override;

    bool empty() const override { return rows == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const String index_name;
    const Block index_sample_block;
    const size_t dimensions;
    const IvfMetric metric;
    const IvfParams params;
    /// The table of the part, whose tree the part encodes its rows by.
    const String table;

private:
    void add(const float * vector);
    void reset();
    /// From now on, the rows are encoded by `tree` as they come.
    void startEncoding(IvfTreeStore::VersionedTree tree_);
    void encode(const float * block, size_t n);

    /// The rows that came, and those encoded so far, which the next encoded row follows.
    size_t rows = 0;
    size_t encoded_rows = 0;
    /// Whether the store was asked for the tree of the table yet.
    bool tree_looked_up = false;
    /// Until the table has a tree, the rows wait in `vectors` and a sample of them in `sample`, for this part to train it.
    std::optional<IvfTreeStore::VersionedTree> tree;
    std::vector<float> rotated_centroids;
    std::vector<IvfListBucket> buckets;
    std::vector<UInt32> bucket_of_list;
    std::vector<UInt64> counts;
    std::vector<char> records;
    /// The rows not encoded yet, encoded on all cores once they take `assign_block_bytes`.
    std::vector<float> pending;
    std::shared_ptr<TemporaryDataBuffer> vectors;
    std::vector<float> sample;
    size_t sample_capacity = 0;
    pcg64 rng;
    std::vector<float> converted;

    LoggerPtr logger = getLogger("VectorSimilarityIndexIvf");
};

class MergeTreeIndexConditionIvf final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionIvf(const std::optional<VectorSearchParameters> & parameters_, const String & index_column_, IvfMetric metric_, ContextPtr context);

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    NearestNeighbours calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule) const override;
    std::string getDescription() const override { return ""; }

private:
    std::optional<VectorSearchParameters> parameters;
    const String index_column;
    const IvfMetric metric;
    const size_t probes;
    const float index_fetch_multiplier;
    const size_t max_limit;
    const bool is_rescoring;
    const MergeTreeReaderSettings reader_settings;
};

class MergeTreeIndexIvf final : public IMergeTreeIndex
{
public:
    MergeTreeIndexIvf(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_, size_t dimensions_, IvfMetric metric_, IvfParams params_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregatorForPart(const IDataPartStorage & part_storage) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const override;
    bool isVectorSimilarityIndex() const override { return true; }

    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat getPhysicalFormat(
        const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const override;

private:
    const size_t dimensions;
    const IvfMetric metric;
    const IvfParams params;
};

void normalizeIvfVector(float * vector, size_t dimensions);

MergeTreeIndexPtr ivfIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const FieldVector & args);
void ivfIndexValidator(const IndexDescription & index, const FieldVector & args);

}
