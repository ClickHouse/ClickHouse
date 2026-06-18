#pragma once

#include "config.h"

#if USE_USEARCH

#include <Common/Logger.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>

#include <vector>

namespace DB
{

/// Defaults for `vector_spann` index parameters (see `spannIndexCreator` for SQL argument mapping).
static constexpr float default_vector_spann_centroid_ratio = 0.16f;
static constexpr float default_vector_spann_closure_epsilon = 10.0f;
static constexpr UInt32 default_vector_spann_max_replicas = 8;

/// Runtime parameters for MergeTree `vector_spann` secondary index (`TYPE vector_spann(...)`).
struct SpannParams
{
    UInt64 dimensions = 0;
    unum::usearch::metric_kind_t metric_kind{};
    unum::usearch::scalar_kind_t scalar_kind{};
    UsearchHnswParams usearch_hnsw_params;
    float centroid_ratio = default_vector_spann_centroid_ratio;
    /// Not read from index SQL yet; reserved for future closure-expansion tuning.
    float closure_epsilon = default_vector_spann_closure_epsilon;
    /// Not read from index SQL yet; reserved for future posting-list replica limits.
    UInt32 max_replicas = default_vector_spann_max_replicas;
};

/// One posting-list entry: row offset inside the skip-index granule plus the vector in `Float32`.
struct SpannPostingEntry
{
    UInt64 row_id = 0;
    std::vector<Float32> vector;
};

/// Centroid id -> byte range in the `SpannPostingLists` substream (filename suffix `.pl`, data extension `.idx`).
struct SpannCentroidOffset
{
    UInt64 offset = 0;
    UInt32 length = 0;
};

/// Skip-index granule: centroid graph in memory + offset table; posting bytes live on the posting substream.
struct MergeTreeIndexGranuleVectorSpann final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleVectorSpann(const String & index_name_, const SpannParams & params_);
    MergeTreeIndexGranuleVectorSpann(
        const String & index_name_,
        const SpannParams & params_,
        USearchIndexWithSerializationPtr centroid_index_,
        std::vector<SpannCentroidOffset> centroid_offsets_,
        std::vector<std::vector<SpannPostingEntry>> postings_by_centroid_);

    ~MergeTreeIndexGranuleVectorSpann() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return !centroid_index || centroid_index->size() == 0; }
    size_t memoryUsageBytes() const override;

    const String index_name;
    const SpannParams params;

    USearchIndexWithSerializationPtr centroid_index;
    std::vector<SpannCentroidOffset> centroid_offsets;
    /// Built during insert/merge; serialized into the posting substream. After deserialize, holds all postings for the granule (eager load).
    std::vector<std::vector<SpannPostingEntry>> postings_by_centroid;

private:
    static constexpr UInt64 FILE_FORMAT_VERSION = 1;

    LoggerPtr logger = getLogger("VectorSpannIndex");
};

struct MergeTreeIndexAggregatorVectorSpann final : public IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorVectorSpann(const String & index_name_, const Block & index_sample_block_, const SpannParams & params_);

    ~MergeTreeIndexAggregatorVectorSpann() override = default;

    bool empty() const override { return accumulated_vectors.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    /// Evenly spaced row indices in [0, n); deterministic for replicated builds. HBC is follow-up.
    std::vector<UInt64> selectEvenlySpacedCentroids() const;

    USearchIndexWithSerializationPtr buildCentroidIndex(const std::vector<UInt64> & centroid_row_ids) const;

    std::pair<std::vector<SpannCentroidOffset>, std::vector<std::vector<SpannPostingEntry>>> assignAndMakePostings(
        const USearchIndexWithSerializationPtr & centroid_index) const;

    const String index_name;
    const Block index_sample_block;
    const SpannParams params;

    std::vector<std::vector<Float32>> accumulated_vectors;
    std::vector<UInt64> accumulated_row_ids;
};

class MergeTreeIndexConditionVectorSpann final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionVectorSpann(
        const std::optional<VectorSearchParameters> & parameters_,
        const String & index_column_,
        unum::usearch::metric_kind_t metric_kind_,
        const SpannParams & params_,
        ContextPtr context);

    ~MergeTreeIndexConditionVectorSpann() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    NearestNeighbours calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule) const override;
    std::optional<size_t> getApproximateNearestNeighborsLimit() const override;
    std::string getDescription() const override { return ""; }

private:
    std::optional<VectorSearchParameters> parameters;
    const String index_column;
    const unum::usearch::metric_kind_t metric_kind;
    const SpannParams params;
    const float search_epsilon;
    const float index_fetch_multiplier;
    const size_t max_limit;
    const bool is_rescoring;
    const size_t max_posting_lists;
    const size_t expansion_search;
};

class MergeTreeIndexVectorSpann final : public IMergeTreeIndex
{
public:
    MergeTreeIndexVectorSpann(const IndexDescription & index_, SpannParams params_);
    ~MergeTreeIndexVectorSpann() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate,
        ContextPtr context,
        const std::optional<VectorSearchParameters> & parameters) const override;

    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix) const override;

    bool isVectorSimilarityIndex() const override { return true; }

private:
    const SpannParams params;
};

}

#endif
