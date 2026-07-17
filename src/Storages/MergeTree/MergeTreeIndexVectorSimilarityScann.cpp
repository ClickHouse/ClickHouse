#include "config.h"

#if USE_SCANN

#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityScann.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/Exception.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <cmath>
#include <numbers>
#include <numeric>

/// ScaNN headers — included only in this translation unit.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wshadow"
#include <scann/base/search_parameters.h>
#include <scann/tree_x_hybrid/tree_x_params.h>
#include <scann/base/single_machine_base.h>
#include <scann/base/single_machine_factory_options.h>
#include <scann/base/single_machine_factory_scann.h>
#include <scann/data_format/dataset.h>
#include <scann/data_format/docid_collection.h>
#include <scann/oss_wrappers/scann_serialize.h>
#include <scann/partitioning/partitioner.pb.h>
#include <scann/proto/centers.pb.h>
#include <scann/proto/scann.pb.h>
#include <scann/utils/threads.h>
#include <scann/utils/types.h>
#include <google/protobuf/text_format.h>
#pragma GCC diagnostic pop

namespace DB
{

namespace Setting
{
    extern const SettingsFloat vector_search_index_fetch_multiplier;
    extern const SettingsUInt64 max_limit_for_vector_search_queries;
    extern const SettingsBool vector_search_with_rescoring;
    extern const SettingsUInt64 scann_num_leaves_to_search;
    extern const SettingsUInt64 scann_candidate_pool_size;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_build_vector_similarity_index_thread_pool_size;
}

namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
extern const int INCORRECT_DATA;
extern const int ILLEGAL_COLUMN;
extern const int LOGICAL_ERROR;
extern const int INVALID_SETTING_VALUE;
}

// ---------------------------------------------------------------------------
// ScannSearcherWrapper — keeps the heavy ScaNN type out of the header.
// ---------------------------------------------------------------------------

struct ScannSearcherWrapper
{
    std::unique_ptr<research_scann::SingleMachineSearcherBase<float>> inner;
};

namespace
{

/// Global thread pool shared by all ScaNN index builds, mirroring Usearch's
/// getBuildVectorSimilarityIndexThreadPool(). A single shared pool bounds the total
/// number of ScaNN build threads to max_build_vector_similarity_index_thread_pool_size
/// regardless of how many parts build a ScaNN index concurrently, which avoids
/// oversubscription. ScaNN uses its own Eigen-based thread pool type and cannot reuse
/// the ClickHouse ThreadPool object directly, so this is a separate pool governed by the
/// same server setting.
///
/// Sharing the pool across concurrent builds is safe: ScaNN's ParallelFor uses a
/// work-stealing scheme where the calling thread completes the full range itself and the
/// tasks scheduled onto the pool are only opportunistic helpers. A saturated pool
/// therefore degrades to no speed-up, never a deadlock.
std::shared_ptr<research_scann::ThreadPool> getScannBuildThreadPool()
{
    static std::shared_ptr<research_scann::ThreadPool> pool = []
    {
        size_t build_threads = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
        if (build_threads == 0)
            build_threads = getNumberOfCPUCoresToUse();
        return research_scann::StartThreadPool("scann_build_pool", static_cast<int>(build_threads) - 1);
    }();
    return pool;
}

}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static size_t computePaddedDim(size_t dim)
{
    constexpr size_t ALIGN = 8;
    return (dim + ALIGN - 1) / ALIGN * ALIGN;
}

static size_t getAutoScannNumLeaves(size_t num_vectors, const ScannIndexParams & params)
{
    return params.num_leaves != 0
        ? static_cast<size_t>(params.num_leaves)
        : std::max(size_t(1), static_cast<size_t>(std::sqrt(static_cast<double>(num_vectors))));
}

static size_t getAutoScannNumLeavesToSearch(size_t num_vectors, const ScannIndexParams & params)
{
    /// Approximate MyScale's balanced alpha=2.0 profile:
    /// l_search = 0.75 * floor(1 + num_leaves * exp(alpha * 0.8) * 0.015).
    constexpr double alpha = 2.0;
    const size_t num_leaves = getAutoScannNumLeaves(num_vectors, params);
    const auto leaves_to_search = static_cast<size_t>(
        0.75 * std::floor(1.0 + static_cast<double>(num_leaves) * std::exp(alpha * 0.8) * 0.015));
    return std::clamp(leaves_to_search, size_t(1), num_leaves);
}

static size_t getAutoScannCandidatePoolSize(size_t num_candidates, size_t num_vectors, size_t data_dim)
{
    /// Approximate MyScale's balanced alpha=2.0 profile:
    /// num_reorder = 20 * floor(topK^0.65 * sqrt(alpha)) * 2.5.
    /// Alpha is fixed at 2.0, so use the standard sqrt(2) constant directly.
    double pool = 20.0 * std::floor(std::pow(static_cast<double>(num_candidates), 0.65) * std::numbers::sqrt2);
    if (num_vectors > 10000000)
        pool *= std::sqrt(static_cast<double>(num_vectors) / 1e7);
    if (data_dim >= 1024)
        pool = pool * 768.0 / std::min<double>(static_cast<double>(data_dim), 1536.0);

    /// ClickHouse ScaNN indexes are searched from memory, so use MyScale's memory-mode multiplier.
    pool *= 2.5;
    const auto pool_size = static_cast<size_t>(pool);
    return std::min(std::max(num_candidates, pool_size), num_vectors);
}

static std::string buildScannConfigString(
    const std::string & distance_measure,
    size_t num_leaves,
    size_t num_leaves_to_search,
    size_t training_sample_size,
    size_t min_cluster_size,
    size_t num_blocks,
    bool use_residual,
    const std::string & precision)
{
    /// num_clusters_per_block MUST be 16 here. The proto default is 256, but lookup_type
    /// INT8_LUT16 packs two 4-bit codes per byte (CreatePackedDataset: u1 * 16 + u0), so it
    /// can only address 16 centers per subspace. Leaving the default 256 trains a 256-center
    /// codebook whose codes (0..255) overflow the 4-bit packing and silently corrupt the
    /// asymmetric-hashing scores, which collapses candidate ranking: recall@100 then needs a
    /// ~100x larger exact-reordering pool (e.g. 0.40 vs 0.98 at pool=2000 on LAION 1M).
    ///
    /// noise_shaping_threshold enables anisotropic vector quantization (AVQ), ScaNN's standard
    /// technique for biasing per-block quantization error orthogonal to the datapoint direction
    /// to better preserve inner products. 0.2 is the long-standing ScaNN default.

    /// The exact-reordering vectors are stored at this precision: "f32" (float32, exact),
    /// "bf16" (bfloat16, half the size), or "i8" (scalar fixed point, a quarter of the size).
    /// Lower precision shrinks the index at a small recall cost. ScaNN derives the reorder
    /// helper for the configured precision from the (quantized) reorder dataset on restore.
    std::string reorder_quant;
    if (precision == "bf16")
        reorder_quant = "bfloat16 { enabled: true }";
    else if (precision == "i8")
        reorder_quant = "fixed_point { enabled: true }";

    /// SOAR (Spilling with Orthogonality-Amplified Residuals): assign each datapoint to two
    /// partitions — its nearest centroid plus a second one chosen to be orthogonal to the first
    /// residual — so boundary neighbours are not missed without raising num_leaves_to_search.
    /// Only meaningful for the residual (cosine/dotProduct) tree-AH path. The replica count is
    /// fixed at 2 by TWO_CENTER; lambda (residual orthogonality) and overretrieve_factor (query
    /// dedup) use the ScaNN defaults. Passed as a runtime arg, so its braces are not fmt-escaped.
    const std::string database_spilling = use_residual
        ? "  database_spilling {\n"
          "    spilling_type: TWO_CENTER_ORTHOGONALITY_AMPLIFIED\n"
          "    orthogonality_amplification_lambda: 1.5\n"
          "    overretrieve_factor: 2.0\n"
          "  }\n"
        : "";

    return fmt::format(
        "num_neighbors: 100\n"
        "distance_measure {{ distance_measure: \"{}\" }}\n"
        "partitioning {{\n"
        "  num_children: {}\n"
        "  min_cluster_size: {}\n"
        "  max_clustering_iterations: 12\n"
        "  single_machine_center_initialization: DEFAULT_KMEANS_PLUS_PLUS\n"
        "  partitioning_distance {{ distance_measure: \"SquaredL2Distance\" }}\n"
        "  query_spilling {{ spilling_type: FIXED_NUMBER_OF_CENTERS max_spill_centers: {} }}\n"
        "  expected_sample_size: {}\n"
        "  query_tokenization_distance_override {{ distance_measure: \"{}\" }}\n"
        "{}"
        "}}\n"
        "hash {{\n"
        "  asymmetric_hash {{\n"
        "    lookup_type: INT8_LUT16\n"
        "    num_clusters_per_block: 16\n"
        "    use_residual_quantization: {}\n"
        "    noise_shaping_threshold: 0.2\n"
        "    projection {{ projection_type: CHUNK num_blocks: {} num_dims_per_block: 2 input_dim: {} }}\n"
        "  }}\n"
        "}}\n"
        "exact_reordering {{ approx_num_neighbors: 100 {} }}\n",
        distance_measure,
        num_leaves,
        min_cluster_size,
        num_leaves_to_search,
        training_sample_size,
        distance_measure,
        database_spilling,
        use_residual ? "true" : "false",
        num_blocks,
        num_blocks * 2,  /// input_dim = padded_dim = num_blocks × num_dims_per_block
        reorder_quant);
}

// ---------------------------------------------------------------------------
// MergeTreeIndexGranuleVectorSimilarityScann
// ---------------------------------------------------------------------------

MergeTreeIndexGranuleVectorSimilarityScann::MergeTreeIndexGranuleVectorSimilarityScann(
    const ScannIndexParams & params_)
    : params(params_)
    , padded_dim(computePaddedDim(params_.dimensions))
    , log(getLogger("MergeTreeIndexVectorSimilarityScann"))
{
}

MergeTreeIndexGranuleVectorSimilarityScann::~MergeTreeIndexGranuleVectorSimilarityScann() = default;

size_t MergeTreeIndexGranuleVectorSimilarityScann::memoryUsageBytes() const
{
    size_t total = 0;

    /// Reorder vectors. Before build/restore they live in the granule members; afterwards they
    /// are moved into the searcher's reorder helper. The members are empty in the latter case,
    /// so fall back to the size implied by the precision when the searcher holds them.
    const size_t reorder_member_bytes =
        vectors.size() * sizeof(float)
        + bf16_data.size() * sizeof(int16_t)
        + int8_data.size() * sizeof(int8_t)
        + (int8_multipliers.size() + int8_norms.size()) * sizeof(float);
    if (reorder_member_bytes > 0)
    {
        total += reorder_member_bytes;
    }
    else if (searcher && searcher->inner)
    {
        size_t bytes_per_elem = sizeof(float);
        if (params.precision == "bf16")
            bytes_per_elem = sizeof(int16_t);
        else if (params.precision == "i8")
            bytes_per_elem = sizeof(int8_t);
        total += num_vectors * padded_dim * bytes_per_elem;
    }

    /// After buildIndexFromSerialized, hashed_data is moved into the searcher's hashed_dataset.
    if (searcher && searcher->inner)
    {
        if (const auto * hds = searcher->inner->hashed_dataset())
            total += hds->MemoryUsageExcludingDocids();
        else
            total += hashed_data.size();
    }
    else
    {
        total += hashed_data.size();
    }

    /// SOAR secondary codes (moved into the searcher's soar_hashed_dataset after restore; the
    /// member still reflects the on-disk size before that, but the searcher exposes no accessor,
    /// so account for the member only — it is the only copy we hold outside ScaNN).
    total += soar_hashed_data.size();

    total += serialized_partitioner_proto.size();
    total += serialized_codebook_proto.size();
    for (const auto & token : datapoints_by_token)
        total += token.size() * sizeof(uint32_t);

    return total;
}

void MergeTreeIndexGranuleVectorSimilarityScann::serializeBinary(WriteBuffer & ostr) const
{
    writeIntBinary(static_cast<UInt64>(num_vectors), ostr);
    writeIntBinary(static_cast<UInt64>(padded_dim), ostr);

    /// Precision tag for the exact-reordering vectors: 0 = f32, 1 = bf16, 2 = i8.
    /// When no index was built (too few vectors), no quantization happened and the granule still
    /// holds the raw float `vectors`, so fall back to f32 regardless of the configured precision.
    const bool index_built = (searcher && searcher->inner);
    const bool have_quantized = !bf16_data.empty() || !int8_data.empty();
    UInt8 precision_tag = 0;
    if (index_built || have_quantized)
        precision_tag = (params.precision == "bf16") ? 1 : (params.precision == "i8" ? 2 : 0);
    writeIntBinary(precision_tag, ostr);

    if (precision_tag == 0)
    {
        /// f32: raw float32 reorder vectors (from the granule, or read back from the searcher's
        /// dataset after buildIndexFromSerialized moved them out).
        if (!vectors.empty())
        {
            ostr.write(reinterpret_cast<const char *>(vectors.data()), vectors.size() * sizeof(float));
        }
        else
        {
            chassert(searcher && searcher->inner && searcher->inner->dataset());
            const auto * ds = static_cast<const research_scann::DenseDataset<float> *>(searcher->inner->dataset());
            auto span = ds->data();
            ostr.write(reinterpret_cast<const char *>(span.data()), span.size() * sizeof(float));
        }
    }
    else
    {
        /// bf16/i8: the quantized reorder vectors. Use the granule members when populated
        /// (freshly built), otherwise read them back from the searcher's reordering helper
        /// (after buildIndexFromSerialized moved the members into ScaNN).
        research_scann::SingleMachineFactoryOptions extracted;
        const bool from_members = (precision_tag == 1) ? !bf16_data.empty() : !int8_data.empty();
        if (!from_members)
        {
            chassert(searcher && searcher->inner);
            searcher->inner->reordering_helper().AppendDataToSingleMachineFactoryOptions(&extracted);
        }

        if (precision_tag == 1)
        {
            const int16_t * data = nullptr;
            size_t count = 0;
            if (from_members) { data = bf16_data.data(); count = bf16_data.size(); }
            else { auto span = extracted.bfloat16_dataset->data(); data = span.data(); count = span.size(); }
            ostr.write(reinterpret_cast<const char *>(data), count * sizeof(int16_t));
        }
        else
        {
            const int8_t * data = nullptr;
            size_t count = 0;
            const std::vector<float> * mult = nullptr;
            const std::vector<float> * norms = nullptr;
            std::vector<float> mult_tmp;
            std::vector<float> norms_tmp;
            if (from_members)
            {
                data = int8_data.data(); count = int8_data.size();
                mult = &int8_multipliers; norms = &int8_norms;
            }
            else
            {
                const auto & fp = extracted.pre_quantized_fixed_point;
                auto span = fp->fixed_point_dataset->data();
                data = span.data(); count = span.size();
                if (fp->multiplier_by_dimension) mult_tmp = *fp->multiplier_by_dimension;
                if (fp->squared_l2_norm_by_datapoint) norms_tmp = *fp->squared_l2_norm_by_datapoint;
                mult = &mult_tmp; norms = &norms_tmp;
            }
            ostr.write(reinterpret_cast<const char *>(data), count * sizeof(int8_t));
            writeIntBinary(static_cast<UInt64>(mult->size()), ostr);
            if (!mult->empty())
                ostr.write(reinterpret_cast<const char *>(mult->data()), mult->size() * sizeof(float));
            writeIntBinary(static_cast<UInt64>(norms->size()), ostr);
            if (!norms->empty())
                ostr.write(reinterpret_cast<const char *>(norms->data()), norms->size() * sizeof(float));
        }
    }

    /// Pre-trained ScaNN artifacts (all zero-length when index was not built).

    writeIntBinary(static_cast<UInt64>(serialized_partitioner_proto.size()), ostr);
    ostr.write(serialized_partitioner_proto.data(), serialized_partitioner_proto.size());

    writeIntBinary(static_cast<UInt64>(serialized_codebook_proto.size()), ostr);
    ostr.write(serialized_codebook_proto.data(), serialized_codebook_proto.size());

    const size_t hashed_rows = [&]() -> size_t
    {
        if (hashed_dim == 0)
            return 0;
        if (!hashed_data.empty())
            return hashed_data.size() / hashed_dim;
        /// hashed_data was moved into the searcher's hashed_dataset by buildIndexFromSerialized.
        if (searcher && searcher->inner && searcher->inner->hashed_dataset())
            return searcher->inner->hashed_dataset()->size();
        return 0;
    }();
    writeIntBinary(static_cast<UInt64>(hashed_rows), ostr);
    writeIntBinary(static_cast<UInt64>(hashed_dim), ostr);
    if (hashed_rows > 0)
    {
        if (!hashed_data.empty())
        {
            ostr.write(reinterpret_cast<const char *>(hashed_data.data()), hashed_data.size());
        }
        else
        {
            chassert(searcher && searcher->inner && searcher->inner->hashed_dataset());
            auto span = searcher->inner->hashed_dataset()->data();
            ostr.write(reinterpret_cast<const char *>(span.data()), span.size());
        }
    }

    /// SOAR secondary codes (empty for non-SOAR indexes → soar_rows = 0). No searcher fallback:
    /// the searcher exposes no soar_hashed_dataset() accessor, but a restored granule is never
    /// re-serialized (merges rebuild the index from scratch), so the member is authoritative here.
    const size_t soar_rows = (hashed_dim > 0 && !soar_hashed_data.empty()) ? soar_hashed_data.size() / hashed_dim : 0;
    writeIntBinary(static_cast<UInt64>(soar_rows), ostr);
    if (soar_rows > 0)
        ostr.write(reinterpret_cast<const char *>(soar_hashed_data.data()), soar_hashed_data.size());

    writeIntBinary(static_cast<UInt64>(datapoints_by_token.size()), ostr);
    for (const auto & token_dps : datapoints_by_token)
    {
        writeIntBinary(static_cast<UInt32>(token_dps.size()), ostr);
        if (!token_dps.empty())
            ostr.write(reinterpret_cast<const char *>(token_dps.data()),
                token_dps.size() * sizeof(UInt32));
    }
}

void MergeTreeIndexGranuleVectorSimilarityScann::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt64 n = 0;
    UInt64 pd = 0;
    readIntBinary(n, istr);
    readIntBinary(pd, istr);
    num_vectors = n;
    padded_dim = pd;

    /// Reorder vectors at the stored precision. The on-disk tag is authoritative (it must match
    /// how the artifacts were quantized), so set params.precision from it.
    UInt8 precision_tag = 0;
    readIntBinary(precision_tag, istr);
    if (precision_tag == 0)
    {
        params.precision = "f32";
        vectors.resize(num_vectors * padded_dim);
        istr.readStrict(reinterpret_cast<char *>(vectors.data()), vectors.size() * sizeof(float));
    }
    else if (precision_tag == 1)
    {
        params.precision = "bf16";
        bf16_data.resize(num_vectors * padded_dim);
        istr.readStrict(reinterpret_cast<char *>(bf16_data.data()), bf16_data.size() * sizeof(int16_t));
    }
    else if (precision_tag == 2)
    {
        params.precision = "i8";
        int8_data.resize(num_vectors * padded_dim);
        istr.readStrict(reinterpret_cast<char *>(int8_data.data()), int8_data.size() * sizeof(int8_t));
        UInt64 mult_len = 0;
        readIntBinary(mult_len, istr);
        int8_multipliers.resize(mult_len);
        if (mult_len > 0)
            istr.readStrict(reinterpret_cast<char *>(int8_multipliers.data()), mult_len * sizeof(float));
        UInt64 norms_len = 0;
        readIntBinary(norms_len, istr);
        int8_norms.resize(norms_len);
        if (norms_len > 0)
            istr.readStrict(reinterpret_cast<char *>(int8_norms.data()), norms_len * sizeof(float));
    }
    else
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Unsupported vector_similarity('scann', ...) reorder precision tag: {}", static_cast<int>(precision_tag));

    /// Read pre-trained artifacts and restore without retraining.

    UInt64 part_len = 0;
    readIntBinary(part_len, istr);
    if (part_len > 0)
    {
        serialized_partitioner_proto.resize(part_len);
        istr.readStrict(serialized_partitioner_proto.data(), part_len);
    }

    UInt64 codebook_len = 0;
    readIntBinary(codebook_len, istr);
    if (codebook_len > 0)
    {
        serialized_codebook_proto.resize(codebook_len);
        istr.readStrict(serialized_codebook_proto.data(), codebook_len);
    }

    UInt64 hashed_rows = 0;
    UInt64 hashed_dim_read = 0;
    readIntBinary(hashed_rows, istr);
    readIntBinary(hashed_dim_read, istr);
    hashed_dim = static_cast<size_t>(hashed_dim_read);
    if (hashed_rows > 0 && hashed_dim > 0)
    {
        hashed_data.resize(hashed_rows * hashed_dim);
        istr.readStrict(reinterpret_cast<char *>(hashed_data.data()), hashed_rows * hashed_dim);
    }

    /// SOAR secondary codes (soar_rows = 0 for non-SOAR indexes).
    UInt64 soar_rows = 0;
    readIntBinary(soar_rows, istr);
    if (soar_rows > 0 && hashed_dim > 0)
    {
        soar_hashed_data.resize(soar_rows * hashed_dim);
        istr.readStrict(reinterpret_cast<char *>(soar_hashed_data.data()), soar_rows * hashed_dim);
    }

    UInt64 num_tokens = 0;
    readIntBinary(num_tokens, istr);
    datapoints_by_token.resize(num_tokens);
    for (auto & token_dps : datapoints_by_token)
    {
        UInt32 count = 0;
        readIntBinary(count, istr);
        token_dps.resize(count);
        if (count > 0)
            istr.readStrict(reinterpret_cast<char *>(token_dps.data()),
                count * sizeof(UInt32));
    }

    buildIndexFromSerialized();
}

void MergeTreeIndexGranuleVectorSimilarityScann::buildIndex()
{
    if (num_vectors == 0)
        return;

    constexpr size_t MIN_VECTORS = 1000;
    if (num_vectors < MIN_VECTORS)
    {
        LOG_WARNING(log,
            "ScaNN requires at least {} vectors but granule has {}. "
            "Index not built; full granule scan will be used as fallback.",
            MIN_VECTORS, num_vectors);
        return;
    }

    /// For cosine distance, normalize vectors to unit length in place.
    /// Reject zero-magnitude vectors: cosineDistance([0,...], x) = NaN in exact mode,
    /// but after normalization ScaNN would report a finite distance instead.
    if (params.distance_name == "cosineDistance")
    {
        for (size_t i = 0; i < num_vectors; ++i)
        {
            float * v = vectors.data() + i * padded_dim;
            double sq_norm = 0.0;
            for (size_t d = 0; d < padded_dim; ++d)
                sq_norm += static_cast<double>(v[d]) * static_cast<double>(v[d]);
            if (sq_norm == 0.0 || !std::isfinite(sq_norm))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Zero-magnitude vector is not allowed for vector_similarity('scann', 'cosineDistance', ...) index");
            const float inv = static_cast<float>(1.0 / std::sqrt(sq_norm));
            for (size_t d = 0; d < padded_dim; ++d)
                v[d] *= inv;
        }
    }

    /// Map ClickHouse distance name to ScaNN distance measure.
    std::string scann_distance_measure;
    bool use_residual = false;
    if (params.distance_name == "L2Distance")
    {
        scann_distance_measure = "SquaredL2Distance";
    }
    else /// cosineDistance or dotProduct
    {
        scann_distance_measure = "DotProductDistance";
        use_residual = true;
    }

    /// Auto-tune partitioning parameters based on dataset size unless the index definition
    /// explicitly provides the build-time number of IVF leaves.
    const size_t num_leaves = params.num_leaves != 0
        ? static_cast<size_t>(params.num_leaves)
        : std::max(size_t(1), static_cast<size_t>(std::sqrt(static_cast<double>(num_vectors))));

    const size_t num_leaves_to_search = std::max(size_t(1), static_cast<size_t>(std::sqrt(static_cast<double>(num_leaves))));
    const size_t training_sample_size = std::min(num_vectors, num_leaves * 75);
    /// Keep min_cluster_size <= half the average cluster size so ScaNN's k-means
    /// can always satisfy the constraint; cap at 50 for large datasets.
    const size_t min_cluster_size = std::max(size_t(1), std::min(size_t(50), (num_vectors / num_leaves) / 2));
    const size_t num_blocks = std::max(size_t(1), padded_dim / 2);

    const std::string config_str = buildScannConfigString(
        scann_distance_measure, num_leaves, num_leaves_to_search,
        training_sample_size, min_cluster_size, num_blocks, use_residual, params.precision);

    LOG_DEBUG(log, "Building ScaNN index: num_vectors={} padded_dim={} num_leaves={} config=\n{}",
        num_vectors, padded_dim, num_leaves, config_str);

    research_scann::ScannConfig config;
    if (!google::protobuf::TextFormat::ParseFromString(config_str, &config))
        throw Exception(ErrorCodes::INCORRECT_DATA, "ScaNN index build failed: could not parse ScaNN config string");

    /// Move vectors[] into the dataset to avoid a duplicate copy in memory;
    /// serializeBinary reads the data back from the searcher's dataset when vectors is empty.
    auto dataset = std::make_shared<research_scann::DenseDataset<float>>(
        std::move(vectors), num_vectors);

    research_scann::SingleMachineFactoryOptions build_opts;
    build_opts.parallelization_pool = getScannBuildThreadPool();
    try
    {
        auto status_or = research_scann::SingleMachineFactoryScann<float>(
            config, std::move(dataset), std::move(build_opts));

        if (!status_or.ok())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ScaNN index build failed: {}", status_or.status().ToString());

        searcher = std::make_unique<ScannSearcherWrapper>();
        searcher->inner = std::move(status_or).value();
    }
    catch (const DB::Exception &)
    {
        throw;
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "ScaNN index build failed: {}", e.what());
    }
    catch (...)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "ScaNN index build failed: unknown exception");
    }

    LOG_DEBUG(log, "ScaNN index built successfully for {} vectors", num_vectors);

    /// Extract pre-trained artifacts so serializeBinary can persist them
    /// without retraining on the next server restart.
    auto opts_or = searcher->inner->ExtractSingleMachineFactoryOptions();
    if (!opts_or.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN index build failed: could not extract trained artifacts: {}",
            opts_or.status().ToString());

    const auto & opts = opts_or.value();

    if (opts.serialized_partitioner)
        opts.serialized_partitioner->SerializeToString(&serialized_partitioner_proto);

    if (opts.ah_codebook)
        opts.ah_codebook->SerializeToString(&serialized_codebook_proto);

    /// Persist the quantized (hashed) codes so that a later restore reuses them instead of
    /// re-quantizing every datapoint, which otherwise dominates cold-load time (~200s for 1M
    /// vectors). The codes come from the extracted options: ExtractSingleMachineFactoryOptions
    /// unpacks them from the leaf searchers' packed LUT16 format. They cannot be read back from
    /// the tree searcher's top-level hashed_dataset() during serializeBinary - it is null for
    /// Tree-AH, where the codes live in the per-leaf searchers - so copy them into the granule's
    /// hashed_data member here.
    if (opts.hashed_dataset && !opts.hashed_dataset->empty())
    {
        hashed_dim = opts.hashed_dataset->dimensionality();
        const auto span = opts.hashed_dataset->data();
        hashed_data.assign(span.data(), span.data() + span.size());
    }

    /// SOAR: persist the secondary-partition AH codes too. Same shape as hashed_dataset
    /// (num_vectors × hashed_dim). On restore the per-datapoint secondary token (the docid the
    /// reconstruction needs) is recomputed from datapoints_by_token, so only the flat codes are
    /// stored here.
    if (opts.soar_hashed_dataset && !opts.soar_hashed_dataset->empty())
    {
        const auto span = opts.soar_hashed_dataset->data();
        soar_hashed_data.assign(span.data(), span.data() + span.size());
    }

    if (opts.datapoints_by_token)
    {
        datapoints_by_token.clear();
        datapoints_by_token.reserve(opts.datapoints_by_token->size());
        for (const auto & token : *opts.datapoints_by_token)
            datapoints_by_token.emplace_back(token.begin(), token.end());
    }

    /// For non-f32 precision, persist the quantized exact-reordering vectors that ScaNN derived
    /// from the float dataset (extracted above), and drop the float vectors so they are neither
    /// kept in memory nor written to disk. The index is built entirely in float (AH codebook and
    /// IVF centroids train on the float dataset); only the reordering representation is quantized.
    if (params.precision == "bf16")
    {
        if (!opts.bfloat16_dataset || opts.bfloat16_dataset->empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "ScaNN bf16 reorder dataset was not produced");
        const auto span = opts.bfloat16_dataset->data();
        bf16_data.assign(span.data(), span.data() + span.size());
        vectors.clear();
        vectors.shrink_to_fit();
    }
    else if (params.precision == "i8")
    {
        const auto & fp = opts.pre_quantized_fixed_point;
        if (!fp || !fp->fixed_point_dataset || fp->fixed_point_dataset->empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "ScaNN i8 reorder dataset was not produced");
        const auto span = fp->fixed_point_dataset->data();
        int8_data.assign(span.data(), span.data() + span.size());
        if (fp->multiplier_by_dimension)
            int8_multipliers.assign(fp->multiplier_by_dimension->begin(), fp->multiplier_by_dimension->end());
        if (fp->squared_l2_norm_by_datapoint)
            int8_norms.assign(fp->squared_l2_norm_by_datapoint->begin(), fp->squared_l2_norm_by_datapoint->end());
        vectors.clear();
        vectors.shrink_to_fit();
    }

    const size_t hashed_rows_extracted = (opts.hashed_dataset && hashed_dim > 0) ? opts.hashed_dataset->size() : 0;
    LOG_DEBUG(log, "Extracted ScaNN artifacts: partitioner={} bytes, codebook={} bytes, "
        "hashed_dataset={}×{} bytes, {} IVF tokens",
        serialized_partitioner_proto.size(), serialized_codebook_proto.size(),
        hashed_rows_extracted, hashed_dim,
        datapoints_by_token.size());
}

void MergeTreeIndexGranuleVectorSimilarityScann::buildIndexFromSerialized()
{
    if (num_vectors == 0)
        return;

    constexpr size_t MIN_VECTORS = 1000;
    if (serialized_partitioner_proto.empty() || serialized_codebook_proto.empty())
    {
        /// buildIndex skips index construction for granules with fewer than MIN_VECTORS
        /// vectors and leaves all artifact fields empty. After DETACH/ATTACH or restart,
        /// treat the same condition (num_vectors < MIN_VECTORS + empty artifacts) as the
        /// identical no-index fallback so the part remains readable.
        if (num_vectors < MIN_VECTORS)
            return;
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN index restore failed: serialized artifacts are missing for {} vectors. "
            "Drop and recreate the index.",
            num_vectors);
    }

    research_scann::SingleMachineFactoryOptions opts;

    /// Build the per-leaf searchers in parallel during restore. With the precomputed
    /// hashed_dataset present this only repacks the codes into the leaf LUT16 layout (no
    /// re-quantization); without it (legacy indexes) it parallelizes the re-quantization so a
    /// cold load is not single-threaded.
    opts.parallelization_pool = getScannBuildThreadPool();

    opts.serialized_partitioner = std::make_shared<research_scann::SerializedPartitioner>();
    if (!opts.serialized_partitioner->ParseFromString(serialized_partitioner_proto))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN index restore failed: could not parse SerializedPartitioner");

    opts.ah_codebook = std::make_shared<research_scann::CentersForAllSubspaces>();
    if (!opts.ah_codebook->ParseFromString(serialized_codebook_proto))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN index restore failed: could not parse AH codebook");

    if (hashed_dim > 0 && !hashed_data.empty())
    {
        const size_t hashed_rows = hashed_data.size() / hashed_dim;
        opts.hashed_dataset = std::make_shared<research_scann::DenseDataset<uint8_t>>(
            std::move(hashed_data), hashed_rows);
    }

    /// SOAR: rebuild the secondary-partition dataset. The tree-AH residual searcher selects the
    /// primary vs SOAR code for a datapoint in a given leaf by reading the SOAR dataset's docid,
    /// which must equal that datapoint's secondary token. The extracted codes carry no docids, so
    /// recompute the secondary token as the token where the datapoint appears the second time when
    /// datapoints_by_token is scanned in token order (identical to ScaNN's build-time split).
    if (hashed_dim > 0 && !soar_hashed_data.empty())
    {
        const size_t soar_rows = soar_hashed_data.size() / hashed_dim;
        std::vector<int32_t> secondary_token(soar_rows, -1);
        std::vector<char> seen(soar_rows, 0);
        for (size_t token = 0; token < datapoints_by_token.size(); ++token)
            for (uint32_t dp : datapoints_by_token[token])
            {
                if (dp >= soar_rows)
                    continue;
                if (seen[dp])
                    secondary_token[dp] = static_cast<int32_t>(token);
                else
                    seen[dp] = 1;
            }

        auto docids = std::make_unique<research_scann::FixedLengthDocidCollection>(sizeof(int32_t));
        docids->Reserve(static_cast<research_scann::DatapointIndex>(soar_rows));
        for (size_t i = 0; i < soar_rows; ++i)
            if (!docids->Append(research_scann::strings::Int32ToKey(secondary_token[i])).ok())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ScaNN index restore failed: could not build SOAR docids");

        opts.soar_hashed_dataset = std::make_shared<research_scann::DenseDataset<uint8_t>>(
            std::move(soar_hashed_data), std::move(docids));
    }

    if (!datapoints_by_token.empty())
    {
        auto dbt = std::make_shared<std::vector<std::vector<research_scann::DatapointIndex>>>();
        dbt->reserve(datapoints_by_token.size());
        for (const auto & token : datapoints_by_token)
            dbt->emplace_back(token.begin(), token.end());
        opts.datapoints_by_token = std::move(dbt);
    }

    /// Reconstruct the same ScaNN config that was used during buildIndex().
    std::string scann_distance_measure;
    bool use_residual = false;
    if (params.distance_name == "L2Distance")
    {
        scann_distance_measure = "SquaredL2Distance";
    }
    else
    {
        scann_distance_measure = "DotProductDistance";
        use_residual = true;
    }

    const size_t num_leaves = params.num_leaves != 0
        ? static_cast<size_t>(params.num_leaves)
        : std::max(size_t(1), static_cast<size_t>(std::sqrt(static_cast<double>(num_vectors))));
    const size_t num_leaves_to_search = std::max(size_t(1),
        static_cast<size_t>(std::sqrt(static_cast<double>(num_leaves))));
    const size_t training_sample_size = std::min(num_vectors, num_leaves * 75);
    const size_t min_cluster_size = std::max(size_t(1), std::min(size_t(50), (num_vectors / num_leaves) / 2));
    const size_t num_blocks = std::max(size_t(1), padded_dim / 2);

    const std::string config_str = buildScannConfigString(
        scann_distance_measure, num_leaves, num_leaves_to_search,
        training_sample_size, min_cluster_size, num_blocks, use_residual, params.precision);

    research_scann::ScannConfig config;
    if (!google::protobuf::TextFormat::ParseFromString(config_str, &config))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN index restore failed: could not parse ScaNN config string");

    /// Reorder dataset at the stored precision. For f32 the float vectors are the reordering
    /// dataset; for bf16/i8 the quantized dataset is supplied via opts and no float dataset is
    /// needed (the AH leaf searchers are rebuilt from the precomputed hashed codes). In all
    /// cases the members are moved into ScaNN so only a single copy is held in memory.
    std::shared_ptr<research_scann::DenseDataset<float>> dataset;
    if (params.precision == "bf16")
    {
        opts.bfloat16_dataset = std::make_shared<research_scann::DenseDataset<int16_t>>(
            std::move(bf16_data), num_vectors);
    }
    else if (params.precision == "i8")
    {
        auto fp = std::make_shared<research_scann::PreQuantizedFixedPoint>();
        fp->fixed_point_dataset = std::make_shared<research_scann::DenseDataset<int8_t>>(
            std::move(int8_data), num_vectors);
        fp->multiplier_by_dimension = std::make_shared<std::vector<float>>(std::move(int8_multipliers));
        fp->squared_l2_norm_by_datapoint = std::make_shared<std::vector<float>>(std::move(int8_norms));
        opts.pre_quantized_fixed_point = std::move(fp);
    }
    else
    {
        dataset = std::make_shared<research_scann::DenseDataset<float>>(
            std::move(vectors), num_vectors);
    }

    try
    {
        auto status_or = research_scann::SingleMachineFactoryScann<float>(
            config, std::move(dataset), std::move(opts));

        if (!status_or.ok())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ScaNN index restore failed: {}", status_or.status().ToString());

        searcher = std::make_unique<ScannSearcherWrapper>();
        searcher->inner = std::move(status_or).value();
    }
    catch (const DB::Exception &)
    {
        throw;
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "ScaNN index restore failed: {}", e.what());
    }
    catch (...)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "ScaNN index restore failed: unknown exception");
    }

    LOG_DEBUG(log, "ScaNN index restored from serialized state for {} vectors", num_vectors);
}

// ---------------------------------------------------------------------------
// MergeTreeIndexAggregatorVectorSimilarityScann
// ---------------------------------------------------------------------------

MergeTreeIndexAggregatorVectorSimilarityScann::MergeTreeIndexAggregatorVectorSimilarityScann(
    const ScannIndexParams & params_, const String & column_name_)
    : params(params_)
    , column_name(column_name_)
    , granule(std::make_shared<MergeTreeIndexGranuleVectorSimilarityScann>(params_))
{
}

bool MergeTreeIndexAggregatorVectorSimilarityScann::empty() const
{
    return granule->empty();
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSimilarityScann::getGranuleAndReset()
{
    granule->buildIndex();
    auto result = granule;
    granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarityScann>(params);
    return result;
}

void MergeTreeIndexAggregatorVectorSimilarityScann::update(
    const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Position {} is not less than block rows {}", *pos, block.rows());

    const size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    const auto & index_column = block.getByName(column_name).column;
    const ColumnPtr column_cut = index_column->cut(*pos, rows_read);

    const auto * column_array = typeid_cast<const ColumnArray *>(column_cut.get());
    if (!column_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expected Array column for vector_similarity('scann', ...) index");

    const auto & offsets = column_array->getOffsets();
    const auto & data_col = column_array->getData();

    const size_t dims = params.dimensions;
    const size_t pd = granule->padded_dim;

    /// Validate dimensions for each row and append padded vectors.
    const auto & data_type = block.getByName(column_name).type;
    const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get());
    if (!array_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array data type");

    const TypeIndex nested = array_type->getNestedType()->getTypeId();
    WhichDataType which(nested);

    for (size_t row = 0; row < rows_read; ++row)
    {
        /// offsets[-1] == 0 by PaddedPODArray convention.
        const size_t row_start = offsets[static_cast<ssize_t>(row) - 1];
        const size_t row_end   = offsets[row];
        const size_t row_len   = row_end - row_start;

        if (row_len != dims)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Array has {} elements, expected {} for vector_similarity('scann', ...) index",
                row_len, dims);

        const size_t old_size = granule->vectors.size();
        granule->vectors.resize(old_size + pd, 0.0f);
        float * dst = granule->vectors.data() + old_size;

        if (which.isFloat32())
        {
            const auto & float_col = typeid_cast<const ColumnFloat32 &>(data_col);
            for (size_t d = 0; d < dims; ++d)
                dst[d] = float_col.getData()[row_start + d];
        }
        else if (which.isFloat64())
        {
            const auto & double_col = typeid_cast<const ColumnFloat64 &>(data_col);
            for (size_t d = 0; d < dims; ++d)
                dst[d] = static_cast<float>(double_col.getData()[row_start + d]);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "vector_similarity('scann', ...) index supports only Array(Float32) and Array(Float64)");
        }

        for (size_t d = 0; d < dims; ++d)
            if (!std::isfinite(dst[d]))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Vector for vector_similarity('scann', ...) index must not contain non-finite values (NaN or Inf)");

        if (params.distance_name == "cosineDistance")
        {
            double sq_norm = 0.0;
            for (size_t d = 0; d < dims; ++d)
                sq_norm += static_cast<double>(dst[d]) * static_cast<double>(dst[d]);
            if (sq_norm == 0.0 || !std::isfinite(sq_norm))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Zero-magnitude vector is not allowed for vector_similarity('scann', 'cosineDistance', ...) index");
        }
    }

    granule->num_vectors += rows_read;
    *pos += rows_read;
}

// ---------------------------------------------------------------------------
// MergeTreeIndexConditionVectorSimilarityScann
// ---------------------------------------------------------------------------

MergeTreeIndexConditionVectorSimilarityScann::MergeTreeIndexConditionVectorSimilarityScann(
    const std::optional<VectorSearchParameters> & parameters_,
    const String & index_column_,
    const ScannIndexParams & index_params_,
    ContextPtr context)
    : parameters(parameters_)
    , index_column(index_column_)
    , index_params(index_params_)
    , index_fetch_multiplier(static_cast<double>(context->getSettingsRef()[Setting::vector_search_index_fetch_multiplier]))
    , max_limit(context->getSettingsRef()[Setting::max_limit_for_vector_search_queries])
    , is_rescoring(context->getSettingsRef()[Setting::vector_search_with_rescoring])
    , scann_num_leaves_to_search(context->getSettingsRef()[Setting::scann_num_leaves_to_search])
    , scann_candidate_pool_size(context->getSettingsRef()[Setting::scann_candidate_pool_size])
{
    static constexpr double MAX_INDEX_FETCH_MULTIPLIER = 1000.0;
    if (!std::isfinite(index_fetch_multiplier)
        || index_fetch_multiplier <= 0.0 || index_fetch_multiplier > MAX_INDEX_FETCH_MULTIPLIER
        || (parameters && !std::isfinite(index_fetch_multiplier * static_cast<double>(parameters->limit))))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Setting 'vector_search_index_fetch_multiplier' must be greater than 0.0 and less than {}",
            MAX_INDEX_FETCH_MULTIPLIER);
}

std::string MergeTreeIndexConditionVectorSimilarityScann::getDescription() const
{
    return "vector_similarity(scann, " + index_params.distance_name + ", " + std::to_string(index_params.dimensions) + ")";
}

bool MergeTreeIndexConditionVectorSimilarityScann::alwaysUnknownOrTrue() const
{
    if (!parameters)
        return true;
    if (parameters->column != index_column)
        return true;
    if (parameters->distance_function != index_params.distance_name)
        return true;
    return false;
}

bool MergeTreeIndexConditionVectorSimilarityScann::mayBeTrueOnGranule(
    MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "mayBeTrueOnGranule is not supported for vector_similarity('scann', ...) index");
}

NearestNeighbours MergeTreeIndexConditionVectorSimilarityScann::calculateApproximateNearestNeighbors(
    MergeTreeIndexGranulePtr granule_) const
{
    if (!parameters)
    {
        /// Should not be reached: alwaysUnknownOrTrue() returns true when parameters is null,
        /// so the engine skips this index before calling calculateApproximateNearestNeighbors.
        NearestNeighbours empty;
        return empty;
    }

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleVectorSimilarityScann>(granule_);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has unexpected type");

    /// Fallback: return all rows if index was not built (too few vectors).
    /// Do not set distances so the executor treats this as a non-optimized granule and
    /// computes exact distances for every returned row.
    if (!granule->searcher || !granule->searcher->inner)
    {
        NearestNeighbours result;
        result.rows.resize(granule->num_vectors);
        std::iota(result.rows.begin(), result.rows.end(), UInt64(0));
        return result;
    }

    size_t topk            = parameters->limit;
    const size_t pd        = granule->padded_dim;
    const size_t orig_dims = index_params.dimensions;

    /// Mirror HNSW behaviour: expand the candidate set when additional filters are present
    /// (post-filtering may discard results) or when rescoring is enabled.
    if (parameters->additional_filters_present || is_rescoring)
        topk = std::min(static_cast<size_t>(static_cast<double>(topk) * index_fetch_multiplier), max_limit);

    const auto & ref = parameters->reference_vector;
    if (ref.size() != orig_dims)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Reference vector dimension {} does not match index dimension {}",
            ref.size(), orig_dims);

    /// Build padded query vector.
    std::vector<float> query(pd, 0.0f);
    for (size_t i = 0; i < orig_dims; ++i)
    {
        const float v = static_cast<float>(ref[i]);
        if (!std::isfinite(v))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Query vector for vector_similarity('scann', ...) must not contain non-finite values (NaN or Inf)");
        query[i] = v;
    }

    /// Normalize for cosine distance (same as build-time normalization).
    /// Reject zero-magnitude query vectors for the same reason as at index build time.
    if (index_params.distance_name == "cosineDistance")
    {
        double sq_norm = 0.0;
        for (float v : query) sq_norm += static_cast<double>(v) * static_cast<double>(v);
        if (sq_norm == 0.0 || !std::isfinite(sq_norm))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Zero-magnitude query vector is not allowed for vector_similarity('scann', 'cosineDistance', ...)");
        const float inv = static_cast<float>(1.0 / std::sqrt(sq_norm));
        for (float & v : query) v *= inv;
    }

    /// Run search.
    research_scann::DenseDataset<float> query_dataset(std::move(query), 1);

    /// num_candidates: rows returned to ClickHouse for its own exact reranking.
    /// candidate_pool: AH candidate pool fed into ScaNN's internal exact reranker.
    const size_t num_candidates = std::min(topk, granule->num_vectors);
    const size_t candidate_pool = std::max(
        num_candidates,
        (scann_candidate_pool_size > 0)
            ? std::min(scann_candidate_pool_size, granule->num_vectors)
            : getAutoScannCandidatePoolSize(num_candidates, granule->num_vectors, index_params.dimensions));
    const size_t num_leaves_to_search = scann_num_leaves_to_search > 0
        ? scann_num_leaves_to_search
        : getAutoScannNumLeavesToSearch(granule->num_vectors, index_params);

    static constexpr size_t MAX_INT32 = static_cast<size_t>(std::numeric_limits<int32_t>::max());
    if (candidate_pool > MAX_INT32)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN candidate pool size {} exceeds int32_t limit", candidate_pool);
    if (num_candidates > MAX_INT32)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN num_candidates {} exceeds int32_t limit", num_candidates);

    std::vector<research_scann::SearchParameters> search_params(1);
    search_params[0].set_pre_reordering_num_neighbors(static_cast<int32_t>(candidate_pool));
    search_params[0].set_post_reordering_num_neighbors(static_cast<int32_t>(num_candidates));
    search_params[0].set_pre_reordering_epsilon(std::numeric_limits<float>::infinity());
    search_params[0].set_post_reordering_epsilon(std::numeric_limits<float>::infinity());

    if (num_leaves_to_search > MAX_INT32)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "scann_num_leaves_to_search {} exceeds int32_t limit", num_leaves_to_search);

    auto tree_params = std::make_shared<research_scann::TreeXOptionalParameters>();
    tree_params->set_num_partitions_to_search_override(static_cast<int32_t>(num_leaves_to_search));
    search_params[0].set_searcher_specific_optional_parameters(std::move(tree_params));
    std::vector<research_scann::NNResultsVector> result_vecs(1);

    const auto status = granule->searcher->inner->FindNeighborsBatched(
        query_dataset,
        absl::MakeSpan(search_params),
        absl::MakeSpan(result_vecs));

    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN search failed: {}", status.ToString());

    const auto & nn = result_vecs[0];

    /// If ScaNN returned fewer candidates than requested (e.g. scann_num_leaves_to_search
    /// searched too few IVF partitions), the approximate result set is incomplete.
    /// Fall back to a full-granule scan without distances: returning all row offsets with
    /// distances unset causes the upstream executor to read every row in the granule and
    /// compute exact distances, guaranteeing correct result cardinality.
    if (nn.size() < num_candidates)
    {
        NearestNeighbours result;
        result.rows.resize(granule->num_vectors);
        std::iota(result.rows.begin(), result.rows.end(), UInt64{0});
        return result;
    }

    NearestNeighbours result;
    result.rows.reserve(nn.size());
    if (parameters->return_distances)
        result.distances = std::vector<float>();

    for (const auto & [idx, dist] : nn)
    {
        result.rows.push_back(static_cast<UInt64>(idx));
        if (result.distances)
        {
            /// ScaNN distances must be stored in the representation expected by
            /// optimizeVectorSearchSecondPass, which applies sqrt(_distance) for
            /// L2Distance (because usearch/hnsw returns squared L2).
            /// - cosineDistance: ScaNN returns -dot(a_norm, b_norm); store as 1 - cos(θ)
            /// - L2Distance:     ScaNN returns squared L2; store as-is (optimizer applies sqrt)
            /// - dotProduct:     ScaNN returns -dot(a, b); store as dot(a, b)
            float converted = 0.0f;
            if (index_params.distance_name == "cosineDistance")
                converted = 1.0f + dist;
            else if (index_params.distance_name == "L2Distance")
                converted = std::max(0.0f, dist); /// keep squared; optimizer wraps in sqrt
            else /// dotProduct
                converted = -dist;
            result.distances->push_back(converted);
        }
    }

    return result;
}

// ---------------------------------------------------------------------------
// MergeTreeIndexVectorSimilarityScann
// ---------------------------------------------------------------------------

MergeTreeIndexVectorSimilarityScann::MergeTreeIndexVectorSimilarityScann(
    StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_, const ScannIndexParams & params_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , params(params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSimilarityScann::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarityScann>(params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarityScann::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarityScann>(
        params, index.column_names[0]);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarityScann::createIndexCondition(
    const ActionsDAG::Node * /*predicate*/, ContextPtr context) const
{
    /// Called when no VectorSearchParameters are available (e.g. non-vector-search queries).
    /// Return a condition with null parameters so alwaysUnknownOrTrue() = true → index is skipped.
    return std::make_shared<MergeTreeIndexConditionVectorSimilarityScann>(
        std::nullopt, index.column_names[0], params, context);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarityScann::createIndexCondition(
    const ActionsDAG::Node * /*predicate*/, ContextPtr context,
    const std::optional<VectorSearchParameters> & parameters) const
{
    return std::make_shared<MergeTreeIndexConditionVectorSimilarityScann>(
        parameters, index.column_names[0], params, context);
}

}

#endif /// USE_SCANN
