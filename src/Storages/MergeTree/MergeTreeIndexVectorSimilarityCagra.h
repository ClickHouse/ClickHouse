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
/// A granule holds the vectors and the graph; the first search uploads them to the device, where they stay while the
/// granule is cached. Granules of fewer than `min_rows_for_graph` rows have no graph and are searched exactly on the CPU.
///
/// Quantization `pq2`, `pq4`, `pq8` (CAGRA-Q) keeps the vectors compressed by VPQ, so the distances are approximate.
/// CAGRA-Q supports only L2, hence only `L2Distance` and `cosineDistance` (normalized vectors).

enum class CagraMetric : uint8_t
{
    L2 = 0,     /// squared, as in `hnsw`
    Cosine = 1, /// normalized vectors, inner product
    Dot = 2,    /// `1 - dot`, as in `hnsw`
};

struct CagraParams
{
    size_t graph_degree = 64;
    size_t intermediate_graph_degree = 128;
    /// Components per byte of VPQ code; 0 for `Float32`.
    size_t pq_len = 0;
};

/// Vectors compressed by VPQ, see `GPU::CagraCompressedDataset`.
struct CagraCompressedVectors
{
    size_t pq_len = 0;
    size_t vq_centers = 0;
    size_t row_bytes = 0;
    std::vector<float> vq_codebook; /// vq_centers x dimensions
    std::vector<float> pq_codebook; /// cagra_pq_codebook_rows x pq_len
    std::vector<uint8_t> codes;     /// rows x row_bytes

    void decompress(size_t row, size_t dimensions, float * vector) const;
    size_t memoryUsageBytes() const;
};

class CagraDeviceIndex;

struct MergeTreeIndexGranuleCagra final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleCagra(const String & index_name_, CagraMetric metric_, size_t dimensions_);
    /// `dataset` is empty if `compressed` is set.
    MergeTreeIndexGranuleCagra(
        const String & index_name_, CagraMetric metric_, size_t dimensions_, size_t rows_,
        std::vector<float> && dataset_, std::optional<CagraCompressedVectors> && compressed_,
        std::vector<UInt32> && graph_, size_t graph_degree_);
    ~MergeTreeIndexGranuleCagra() override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return rows == 0; }
    size_t memoryUsageBytes() const override;

    /// The `limit` nearest rows to `query`, searched on the device. `candidates` is `itopk_size` of CAGRA.
    /// For `Cosine`, queries must be normalized.
    NearestNeighbours search(const std::vector<float> & query, size_t limit, size_t candidates, bool return_distances) const;

    /// The same for `num_queries` queries at once, on the device. Writes `num_queries * limit` rows and distances;
    /// a missing row is `std::numeric_limits<UInt32>::max()`.
    void searchBatch(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t candidates,
        std::vector<UInt32> & neighbours, std::vector<float> & distances) const;

    /// The same as `search`, as a beam search over the graph on the CPU.
    NearestNeighbours searchOnCpu(const std::vector<float> & query, size_t limit, size_t candidates, bool return_distances) const;

    /// The same as `searchBatch`, on all CPU cores.
    void searchBatchOnCpu(const std::vector<float> & queries, size_t num_queries, size_t limit, size_t candidates,
        std::vector<UInt32> & neighbours, std::vector<float> & distances) const;

    static void normalize(float * vector, size_t dimensions);

    const String index_name;
    const CagraMetric metric;
    size_t dimensions;
    size_t rows = 0;
    size_t graph_degree = 0;
    std::vector<float> dataset;
    std::optional<CagraCompressedVectors> compressed;
    std::vector<UInt32> graph;

    /// The distance of CAGRA as `hnsw` returns it.
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

MergeTreeIndexPtr cagraIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const FieldVector & args);
void cagraIndexValidator(const IndexDescription & index, const FieldVector & args);

}

#endif
