#pragma once

#include "config.h"

#if USE_GPU

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Common/Logger.h>

#include <pcg_random.hpp>

#include <memory>
#include <mutex>
#include <vector>

namespace DB
{

/// The `ivf` method of the `vector_similarity` index: an inverted file of `Float32` vectors, built and searched on the GPU.
///
///     INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', dimensions[, lists])
///
/// The vectors of a granule are clustered by the balanced k-means of cuVS into `lists` lists (0 or omitted: `4 * sqrt(rows)`).
/// A search scores the query against the centroids, takes the lists of the `vector_search_ivf_probes` nearest ones, and ranks
/// their vectors by exact distances.
///
/// Only the centroids and the offsets of the lists stay in memory (in the cache of vector similarity indexes) and, for
/// probing, on the device. The lists are in a substream of their own, uncompressed, and a search reads only the probed ones
/// at their byte offsets, in chunks, and streams them to the device. A batch of queries (`vectorSearch`) reads every list it
/// probes once.

enum class IvfMetric : uint8_t
{
    L2 = 0,     /// squared, as in `hnsw`
    Cosine = 1, /// normalized vectors, `1 - dot`
};

struct IvfParams
{
    /// 0: `4 * sqrt(rows)`.
    size_t lists = 0;
};

namespace GPU
{
class IvfCentroids;
}

class TemporaryDataBuffer;

/// The entries of a range of lists, built and not yet written: records of the list, the row and the vector, in no order.
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

    /// Writes the header, centroids and offsets to the regular substream and the lists to `VectorIndexLists`.
    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;

    /// Reads the regular substream; the lists are read by the searches.
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return rows == 0; }
    size_t memoryUsageBytes() const override;

    /// The `limit` nearest rows to `query` in the lists of the `probes` nearest centroids. For `Cosine`, queries must be normalized.
    NearestNeighbours search(const std::vector<float> & query, size_t limit, size_t probes,
        const MergeTreeReaderSettings & reader_settings, bool return_distances) const;

    /// The same for `num_queries` queries at once. Writes `num_queries * limit` rows and distances; a missing row is
    /// `std::numeric_limits<UInt32>::max()`.
    void searchBatch(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t probes,
        const MergeTreeReaderSettings & reader_settings, std::vector<UInt32> & neighbours, std::vector<float> & distances) const;

    const String index_name;
    const IvfMetric metric;
    size_t dimensions;
    size_t rows = 0;
    size_t lists = 0;
    std::vector<float> centroids;       /// lists x dimensions
    std::vector<UInt64> list_offsets;   /// lists + 1, in entries

    /// Built, before they are written: the entries in temporary files, by ranges of lists.
    std::vector<IvfListBucket> buckets;

private:
    static constexpr UInt64 FILE_FORMAT_VERSION = 2;

    /// A list in the substream: the rows of its entries, then their vectors.
    size_t entryBytes() const { return sizeof(UInt32) + dimensions * sizeof(float); }
    size_t entries() const { return list_offsets.empty() ? 0 : list_offsets.back(); }

    std::shared_ptr<GPU::IvfCentroids> getDeviceCentroids() const;

    /// Where the lists of this granule start in their substream.
    UInt64 lists_byte_begin = 0;

    /// Where the lists are read from, set by `deserializeBinaryWithMultipleStreams`.
    DataPartStoragePtr data_part_storage;
    String index_file_name;

    mutable std::mutex device_mutex;
    mutable std::shared_ptr<GPU::IvfCentroids> device_centroids;

    LoggerPtr logger = getLogger("VectorSimilarityIndexIvf");
};


/// Builds the index without holding the vectors in memory: they go to a temporary file as they come, with a reservoir sample
/// for k-means. At the end the file is read back in blocks, assigned to lists on the device, and the entries go to temporary
/// files by ranges of lists, each small enough to be sorted by list in memory when the granule is written.
struct MergeTreeIndexAggregatorIvf final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorIvf(const String & index_name_, const Block & index_sample_block_, size_t dimensions_, IvfMetric metric_, IvfParams params_);
    ~MergeTreeIndexAggregatorIvf() override;

    bool empty() const override { return rows == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const String index_name;
    const Block index_sample_block;
    const size_t dimensions;
    const IvfMetric metric;
    const IvfParams params;

private:
    void add(const float * vector);
    void reset();

    size_t rows = 0;
    std::unique_ptr<TemporaryDataBuffer> vectors;
    /// A uniform sample of the rows so far, at most `sample_capacity` of them.
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
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const override;
    bool isVectorSimilarityIndex() const override { return true; }

    /// The regular substream and the lists.
    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat getPhysicalFormat(
        const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const override;

private:
    const size_t dimensions;
    const IvfMetric metric;
    const IvfParams params;
};

/// Normalizes a vector in place; a zero vector stays zero.
void normalizeIvfVector(float * vector, size_t dimensions);

MergeTreeIndexPtr ivfIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const FieldVector & args);
void ivfIndexValidator(const IndexDescription & index, const FieldVector & args);

}

#endif
