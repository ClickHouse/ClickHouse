#pragma once
#include "config.h"

#if USE_SCANN

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>

namespace research_scann
{
template <typename T>
class SingleMachineSearcherBase;
}

namespace DB
{

struct ScannIndexParams
{
    String distance_name; /// "L2Distance", "cosineDistance", "dotProduct"
    UInt64 dimensions = 0;
    /// On-disk precision of the exact-reordering vectors: "f32" (default), "bf16", or "i8".
    /// Lower precision shrinks the index at a small recall cost (recoverable with a larger pool).
    String precision = "f32";
    /// Build-time number of IVF leaves. 0 means auto (sqrt(number of vectors in the granule)).
    UInt64 num_leaves = 0;
};

/// Opaque wrapper so ScaNN heavy headers stay out of this header.
struct ScannSearcherWrapper;

class MergeTreeIndexGranuleVectorSimilarityScann final : public IMergeTreeIndexGranule
{
public:
    explicit MergeTreeIndexGranuleVectorSimilarityScann(const ScannIndexParams & params_);
    ~MergeTreeIndexGranuleVectorSimilarityScann() override;

    bool empty() const override { return num_vectors == 0; }
    size_t memoryUsageBytes() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    /// Build the ScaNN index from scratch using the stored raw vectors.
    /// After a successful build, pre-trained artifacts are stored in the
    /// serialized_* / hashed_* / datapoints_by_token fields so that
    /// serializeBinary can persist them.
    void buildIndex();

    /// Reconstruct the ScaNN index from pre-trained artifacts read by
    /// deserializeBinary.  Falls back to buildIndex if any artifact is missing
    /// or cannot be parsed.
    void buildIndexFromSerialized();

    ScannIndexParams params;
    std::vector<float> vectors;       /// flat: num_vectors × padded_dim (normalized for cosine)
    size_t num_vectors = 0;
    size_t padded_dim = 0;            /// ceil(params.dimensions / 8) * 8
    std::unique_ptr<ScannSearcherWrapper> searcher; /// non-null when index is built
    /// Data owned by ScaNN but not exposed through its searcher API.
    size_t searcher_owned_memory_bytes = 0;

    /// Exact-reordering vectors at the configured params.precision. Exactly one of these holds
    /// the persisted reorder data (the other(s) stay empty); for "f32" the float `vectors` above
    /// are used. ScaNN derives these at build time and we serialize them in place of float32.
    std::vector<int16_t> bf16_data;   /// "bf16": num_vectors × padded_dim bfloat16 (as int16)
    std::vector<int8_t> int8_data;    /// "i8": num_vectors × padded_dim scalar-quantized int8
    std::vector<float> int8_multipliers; /// "i8": per-dimension inverse multiplier (padded_dim)
    std::vector<float> int8_norms;    /// "i8": per-datapoint squared L2 norm (num_vectors), may be empty

    /// Pre-trained artifacts extracted after buildIndex() and persisted by
    /// serializeBinary.  All empty when the index was not built (too few
    /// vectors) or when artifacts could not be extracted.
    std::string serialized_partitioner_proto;   /// SerializedPartitioner binary proto
    std::string serialized_codebook_proto;      /// CentersForAllSubspaces binary proto
    std::vector<uint8_t> hashed_data;           /// flat AH codes: num_vectors × hashed_dim
    std::vector<uint8_t> soar_hashed_data;      /// SOAR: flat AH codes for the secondary partition, num_vectors × hashed_dim (empty if no SOAR)
    size_t hashed_dim = 0;                      /// AH code length per vector
    std::vector<std::vector<uint32_t>> datapoints_by_token; /// IVF inverted lists

private:
    LoggerPtr log;
};

using MergeTreeIndexGranuleVectorSimilarityScannPtr = std::shared_ptr<MergeTreeIndexGranuleVectorSimilarityScann>;

class MergeTreeIndexAggregatorVectorSimilarityScann final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorVectorSimilarityScann(const ScannIndexParams & params_, const String & column_name_);

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    ScannIndexParams params;
    String column_name;
    MergeTreeIndexGranuleVectorSimilarityScannPtr granule;
};

class MergeTreeIndexConditionVectorSimilarityScann final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionVectorSimilarityScann(
        const std::optional<VectorSearchParameters> & parameters_,
        const String & index_column_,
        const ScannIndexParams & index_params_,
        ContextPtr context);

    std::string getDescription() const override;
    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn &) const override;
    NearestNeighbours calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule) const override;

private:
    std::optional<VectorSearchParameters> parameters;
    String index_column;
    ScannIndexParams index_params;
    double index_fetch_multiplier;
    size_t max_limit;
    bool is_rescoring;
    size_t scann_num_leaves_to_search; /// 0 = balanced automatic value
    size_t scann_candidate_pool_size;  /// 0 = balanced automatic value
};

class MergeTreeIndexVectorSimilarityScann final : public IMergeTreeIndex
{
public:
    MergeTreeIndexVectorSimilarityScann(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_, const ScannIndexParams & params_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context,
        const std::optional<VectorSearchParameters> & parameters) const override;

    bool isVectorSimilarityIndex() const override { return true; }

private:
    ScannIndexParams params;
};

}

#endif /// USE_SCANN
