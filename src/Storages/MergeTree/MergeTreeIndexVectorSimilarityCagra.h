#pragma once

#include "config.h"

#if USE_GPU

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/Logger.h>

#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/// The `cagra` method of the `vector_similarity` index: a CAGRA graph of cuVS, built and searched on the GPU.
///
///     INDEX idx vec TYPE vector_similarity('cagra', 'L2Distance', dimensions[, quantization[, graph_degree, intermediate_graph_degree]])
///
/// The granule keeps the vectors and the graph, which are what the file of the index holds as well. The first search of a
/// granule uploads both to the device, and they stay there for as long as the granule is in the cache of vector
/// similarity indexes. Granules of fewer than `min_rows_for_graph` rows have no graph and are searched exactly on the CPU,
/// as CAGRA cannot build a graph of fewer rows than its degree, and a scan of so few rows is faster than a round trip to
/// the device anyway.
///
/// The quantization is `f32` (the default: the vectors are kept as `Float32`) or `pqN` with N of 2, 4 or 8 (CAGRA-Q: the
/// vectors are compressed by VPQ, N components to a byte, see `GPU::CagraCompressedDataset`). The graph is built from the
/// vectors as they are and searched over the compressed ones, by the distances to the decompressed vectors, which are
/// approximate: `vector_search_with_rescoring` makes the query compute the exact distances of the rows it found.
/// CAGRA-Q supports only the L2 distance, which orders normalized vectors as the cosine distance does, so it is for
/// `L2Distance` and `cosineDistance`. Granules without a graph keep their vectors as `Float32`.

enum class CagraMetric : uint8_t
{
    L2 = 0,     /// `L2Distance`, the distance is squared as in the `hnsw` method
    Cosine = 1, /// `cosineDistance`, vectors are normalized and searched by inner product
    Dot = 2,    /// `dotProduct`, the distance is `1 - dot` as in the `hnsw` method
};

struct CagraParams
{
    size_t graph_degree = 64;
    size_t intermediate_graph_degree = 128;
    /// The components of a vector per byte of its VPQ codes, or 0 for `Float32` vectors.
    size_t pq_len = 0;
};

/// The vectors of a granule compressed by VPQ, as `GPU::CagraCompressedDataset` copies them to the host.
struct CagraCompressedVectors
{
    size_t pq_len = 0;
    size_t vq_centers = 0;
    size_t row_bytes = 0;
    /// `vq_centers * dimensions` floats.
    std::vector<float> vq_codebook;
    /// `GPU::cagra_pq_codebook_rows * pq_len` floats.
    std::vector<float> pq_codebook;
    /// `rows * row_bytes` bytes.
    std::vector<uint8_t> codes;

    /// The approximation of the vector `row` of `dimensions` components that it encodes.
    void decompress(size_t row, size_t dimensions, float * vector) const;
    size_t memoryUsageBytes() const;
};

class CagraDeviceIndex;

struct MergeTreeIndexGranuleCagra final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleCagra(const String & index_name_, CagraMetric metric_, size_t dimensions_);
    /// `dataset` is empty if the vectors are `compressed`.
    MergeTreeIndexGranuleCagra(
        const String & index_name_, CagraMetric metric_, size_t dimensions_, size_t rows_,
        std::vector<float> && dataset_, std::optional<CagraCompressedVectors> && compressed_,
        std::vector<UInt32> && graph_, size_t graph_degree_);
    ~MergeTreeIndexGranuleCagra() override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return rows == 0; }
    size_t memoryUsageBytes() const override;

    /// The `limit` nearest rows to `query` (normalized already for `CagraMetric::Cosine`). `candidates` is the size of the
    /// internal top-k list of CAGRA, i.e. the trade-off between speed and recall.
    NearestNeighbours search(const std::vector<float> & query, size_t limit, size_t candidates, bool return_distances) const;

    /// The `limit` nearest rows to each of `num_queries` queries, which are `num_queries * dimensions` floats (normalized
    /// already for `CagraMetric::Cosine`), in one search on the device. Writes `num_queries * limit` row numbers and
    /// distances; a missing neighbour has the row `std::numeric_limits<UInt32>::max()`.
    void searchBatch(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t candidates,
        std::vector<UInt32> & neighbours, std::vector<float> & distances) const;

    /// The same as `search`, on the CPU: a beam search over the graph, as CAGRA does on the device, without cuVS.
    NearestNeighbours searchOnCpu(const std::vector<float> & query, size_t limit, size_t candidates, bool return_distances) const;

    /// The same as `searchBatch`, on the CPU, with the queries spread over all cores.
    void searchBatchOnCpu(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t candidates,
        std::vector<UInt32> & neighbours, std::vector<float> & distances) const;

    /// Normalizes a vector for `CagraMetric::Cosine`.
    static void normalize(float * vector, size_t dimensions);

    const String index_name;
    const CagraMetric metric;
    size_t dimensions;
    size_t rows = 0;
    size_t graph_degree = 0;
    std::vector<float> dataset;
    std::optional<CagraCompressedVectors> compressed;
    std::vector<UInt32> graph;

    /// The distance that the `hnsw` method would return for the distance that CAGRA returned, or, for compressed vectors,
    /// for the squared L2 distance to the decompressed vector.
    float toDistance(float cuvs_distance) const;

private:
    static constexpr UInt64 FILE_FORMAT_VERSION = 2;

    NearestNeighbours searchExact(const std::vector<float> & query, size_t limit, bool return_distances) const;
    std::shared_ptr<CagraDeviceIndex> getDeviceIndex() const;

    mutable std::mutex device_mutex;
    mutable std::shared_ptr<CagraDeviceIndex> device_index;

    LoggerPtr logger = getLogger("VectorSimilarityIndexCagra");
};


struct MergeTreeIndexAggregatorCagra final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorCagra(const String & index_name_, const Block & index_sample_block_, size_t dimensions_, CagraMetric metric_, CagraParams params_);

    bool empty() const override { return dataset.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const String index_name;
    const Block index_sample_block;
    const size_t dimensions;
    const CagraMetric metric;
    const CagraParams params;
    std::vector<float> dataset;

    LoggerPtr logger = getLogger("VectorSimilarityIndexCagra");
};


class MergeTreeIndexConditionCagra final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionCagra(const std::optional<VectorSearchParameters> & parameters_, const String & index_column_, CagraMetric metric_, ContextPtr context);

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    NearestNeighbours calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule) const override;
    std::string getDescription() const override { return ""; }

private:
    std::optional<VectorSearchParameters> parameters;
    const String index_column;
    const CagraMetric metric;
    const size_t candidates;
    const float index_fetch_multiplier;
    const size_t max_limit;
    const bool is_rescoring;
    const bool on_cpu;
};


class MergeTreeIndexCagra final : public IMergeTreeIndex
{
public:
    MergeTreeIndexCagra(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_, size_t dimensions_, CagraMetric metric_, CagraParams params_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const override;
    bool isVectorSimilarityIndex() const override { return true; }

private:
    const size_t dimensions;
    const CagraMetric metric;
    const CagraParams params;
};

/// For the creator and the validator of `vector_similarity`, which hand the `cagra` method over to these.
MergeTreeIndexPtr cagraIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const FieldVector & args);
void cagraIndexValidator(const IndexDescription & index, const FieldVector & args);

}

#endif
