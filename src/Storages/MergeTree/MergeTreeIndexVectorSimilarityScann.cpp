#include "config.h"

#if USE_SCANN

#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityScann.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <cmath>
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
#include <scann/partitioning/partitioner.pb.h>
#include <scann/proto/centers.pb.h>
#include <scann/proto/scann.pb.h>
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static size_t computePaddedDim(size_t dim)
{
    constexpr size_t ALIGN = 8;
    return (dim + ALIGN - 1) / ALIGN * ALIGN;
}

static std::string buildScannConfigString(
    const std::string & distance_measure,
    size_t num_leaves,
    size_t num_leaves_to_search,
    size_t training_sample_size,
    size_t num_blocks,
    bool use_residual)
{
    return fmt::format(
        "num_neighbors: 100\n"
        "distance_measure {{ distance_measure: \"{}\" }}\n"
        "partitioning {{\n"
        "  num_children: {}\n"
        "  min_cluster_size: 50\n"
        "  max_clustering_iterations: 12\n"
        "  single_machine_center_initialization: DEFAULT_KMEANS_PLUS_PLUS\n"
        "  partitioning_distance {{ distance_measure: \"SquaredL2Distance\" }}\n"
        "  query_spilling {{ spilling_type: FIXED_NUMBER_OF_CENTERS max_spill_centers: {} }}\n"
        "  expected_sample_size: {}\n"
        "  query_tokenization_distance_override {{ distance_measure: \"{}\" }}\n"
        "}}\n"
        "hash {{\n"
        "  asymmetric_hash {{\n"
        "    lookup_type: INT8_LUT16\n"
        "    use_residual_quantization: {}\n"
        "    projection {{ projection_type: CHUNK num_blocks: {} num_dims_per_block: 2 }}\n"
        "  }}\n"
        "}}\n"
        "exact_reordering {{ approx_num_neighbors: 100 }}\n",
        distance_measure,
        num_leaves,
        num_leaves_to_search,
        training_sample_size,
        distance_measure,
        use_residual ? "true" : "false",
        num_blocks);
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
    size_t total = vectors.size() * sizeof(float);
    total += serialized_partitioner_proto.size();
    total += serialized_codebook_proto.size();
    total += hashed_data.size(); /// uint8_t
    for (const auto & token : datapoints_by_token)
        total += token.size() * sizeof(uint32_t);

    /// When the live searcher is present, it holds a separate copy of the dataset
    /// (built in buildIndex/buildIndexFromSerialized) plus an internal hashed dataset.
    /// Include both to avoid underestimating the cache weight.
    if (searcher && searcher->inner)
    {
        if (const auto * ds = searcher->inner->dataset())
            total += ds->MemoryUsageExcludingDocids();
        if (const auto * hds = searcher->inner->hashed_dataset())
            total += hds->MemoryUsageExcludingDocids();
    }

    return total;
}

void MergeTreeIndexGranuleVectorSimilarityScann::serializeBinary(WriteBuffer & ostr) const
{
    writeIntBinary(FILE_FORMAT_VERSION, ostr); /// 1
    writeIntBinary(static_cast<UInt64>(num_vectors), ostr);
    writeIntBinary(static_cast<UInt64>(padded_dim), ostr);
    ostr.write(reinterpret_cast<const char *>(vectors.data()), vectors.size() * sizeof(float));

    /// Pre-trained ScaNN artifacts (all zero-length when index was not built).

    writeIntBinary(static_cast<UInt64>(serialized_partitioner_proto.size()), ostr);
    ostr.write(serialized_partitioner_proto.data(), serialized_partitioner_proto.size());

    writeIntBinary(static_cast<UInt64>(serialized_codebook_proto.size()), ostr);
    ostr.write(serialized_codebook_proto.data(), serialized_codebook_proto.size());

    const size_t hashed_rows = (hashed_dim > 0) ? (hashed_data.size() / hashed_dim) : 0;
    writeIntBinary(static_cast<UInt64>(hashed_rows), ostr);
    writeIntBinary(static_cast<UInt64>(hashed_dim), ostr);
    if (hashed_rows > 0)
        ostr.write(reinterpret_cast<const char *>(hashed_data.data()), hashed_data.size());

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
    UInt8 fmt_version;
    readIntBinary(fmt_version, istr);
    if (fmt_version != FILE_FORMAT_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Unsupported vector_similarity('scann', ...) index version: {}", static_cast<int>(fmt_version));

    UInt64 n, pd;
    readIntBinary(n, istr);
    readIntBinary(pd, istr);
    num_vectors = n;
    padded_dim = pd;

    vectors.resize(num_vectors * padded_dim);
    istr.readStrict(reinterpret_cast<char *>(vectors.data()), vectors.size() * sizeof(float));

    /// Read pre-trained artifacts and restore without retraining.

    UInt64 part_len;
    readIntBinary(part_len, istr);
    if (part_len > 0)
    {
        serialized_partitioner_proto.resize(part_len);
        istr.readStrict(serialized_partitioner_proto.data(), part_len);
    }

    UInt64 codebook_len;
    readIntBinary(codebook_len, istr);
    if (codebook_len > 0)
    {
        serialized_codebook_proto.resize(codebook_len);
        istr.readStrict(serialized_codebook_proto.data(), codebook_len);
    }

    UInt64 hashed_rows, hashed_dim_read;
    readIntBinary(hashed_rows, istr);
    readIntBinary(hashed_dim_read, istr);
    hashed_dim = static_cast<size_t>(hashed_dim_read);
    if (hashed_rows > 0 && hashed_dim > 0)
    {
        hashed_data.resize(hashed_rows * hashed_dim);
        istr.readStrict(reinterpret_cast<char *>(hashed_data.data()), hashed_rows * hashed_dim);
    }

    UInt64 num_tokens;
    readIntBinary(num_tokens, istr);
    datapoints_by_token.resize(num_tokens);
    for (auto & token_dps : datapoints_by_token)
    {
        UInt32 count;
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
    if (params.distance_name == "cosineDistance")
    {
        for (size_t i = 0; i < num_vectors; ++i)
        {
            float * v = vectors.data() + i * padded_dim;
            float sq_norm = 0.0f;
            for (size_t d = 0; d < padded_dim; ++d)
                sq_norm += v[d] * v[d];
            if (sq_norm > 0.0f)
            {
                const float inv = 1.0f / std::sqrt(sq_norm);
                for (size_t d = 0; d < padded_dim; ++d)
                    v[d] *= inv;
            }
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

    /// Auto-tune partitioning parameters based on dataset size.
    const size_t num_leaves = std::max(size_t(1),
        static_cast<size_t>(std::sqrt(static_cast<double>(num_vectors))));

    const size_t num_leaves_to_search = std::max(size_t(1), static_cast<size_t>(std::sqrt(static_cast<double>(num_leaves))));
    const size_t training_sample_size = std::min(num_vectors, num_leaves * 75);
    const size_t num_blocks = std::max(size_t(1), padded_dim / 2);

    const std::string config_str = buildScannConfigString(
        scann_distance_measure, num_leaves, num_leaves_to_search,
        training_sample_size, num_blocks, use_residual);

    LOG_DEBUG(log, "Building ScaNN index: num_vectors={} padded_dim={} num_leaves={} config=\n{}",
        num_vectors, padded_dim, num_leaves, config_str);

    research_scann::ScannConfig config;
    if (!google::protobuf::TextFormat::ParseFromString(config_str, &config))
        throw Exception(ErrorCodes::INCORRECT_DATA, "ScaNN index build failed: could not parse ScaNN config string");

    auto dataset = std::make_shared<research_scann::DenseDataset<float>>(
        std::vector<float>(vectors), /// copy — ScaNN takes ownership
        num_vectors);

    research_scann::SingleMachineFactoryOptions build_opts;
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

    if (opts.hashed_dataset && opts.hashed_dataset->size() > 0)
    {
        hashed_dim = opts.hashed_dataset->dimensionality();
        auto span = opts.hashed_dataset->data();
        hashed_data.assign(span.begin(), span.end());
    }

    if (opts.datapoints_by_token)
    {
        datapoints_by_token.clear();
        datapoints_by_token.reserve(opts.datapoints_by_token->size());
        for (const auto & token : *opts.datapoints_by_token)
            datapoints_by_token.emplace_back(token.begin(), token.end());
    }

    LOG_DEBUG(log, "Extracted ScaNN artifacts: partitioner={} bytes, codebook={} bytes, "
        "hashed_dataset={}×{} bytes, {} IVF tokens",
        serialized_partitioner_proto.size(), serialized_codebook_proto.size(),
        hashed_data.size() / std::max(hashed_dim, size_t(1)), hashed_dim,
        datapoints_by_token.size());
}

void MergeTreeIndexGranuleVectorSimilarityScann::buildIndexFromSerialized()
{
    if (num_vectors == 0)
        return;

    if (serialized_partitioner_proto.empty() || serialized_codebook_proto.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN index restore failed: serialized artifacts are missing for {} vectors. "
            "The index may have been built with an older version; drop and recreate it.",
            num_vectors);

    research_scann::SingleMachineFactoryOptions opts;

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
            std::vector<uint8_t>(hashed_data), hashed_rows);
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

    const size_t num_leaves = std::max(size_t(1),
        static_cast<size_t>(std::sqrt(static_cast<double>(num_vectors))));
    const size_t num_leaves_to_search = std::max(size_t(1),
        static_cast<size_t>(std::sqrt(static_cast<double>(num_leaves))));
    const size_t training_sample_size = std::min(num_vectors, num_leaves * 75);
    const size_t num_blocks = std::max(size_t(1), padded_dim / 2);

    const std::string config_str = buildScannConfigString(
        scann_distance_measure, num_leaves, num_leaves_to_search,
        training_sample_size, num_blocks, use_residual);

    research_scann::ScannConfig config;
    if (!google::protobuf::TextFormat::ParseFromString(config_str, &config))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN index restore failed: could not parse ScaNN config string");

    /// The float dataset is required for the exact-reordering step.
    /// vectors[] already contains (potentially normalized) floats from deserialization.
    auto dataset = std::make_shared<research_scann::DenseDataset<float>>(
        std::vector<float>(vectors), num_vectors);

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
    , index_fetch_multiplier(context->getSettingsRef()[Setting::vector_search_index_fetch_multiplier])
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
        if (!std::isfinite(ref[i]))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Query vector for vector_similarity('scann', ...) must not contain non-finite values (NaN or Inf)");
        query[i] = static_cast<float>(ref[i]);
    }

    /// Normalize for cosine distance (same as build-time normalization).
    if (index_params.distance_name == "cosineDistance")
    {
        float sq_norm = 0.0f;
        for (float v : query) sq_norm += v * v;
        if (sq_norm > 0.0f)
        {
            const float inv = 1.0f / std::sqrt(sq_norm);
            for (float & v : query) v *= inv;
        }
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
            : std::min(num_candidates * 1000, granule->num_vectors));

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

    if (scann_num_leaves_to_search > 0)
    {
        auto tree_params = std::make_shared<research_scann::TreeXOptionalParameters>();
        tree_params->set_num_partitions_to_search_override(static_cast<int32_t>(scann_num_leaves_to_search));
        search_params[0].set_searcher_specific_optional_parameters(std::move(tree_params));
    }
    std::vector<research_scann::NNResultsVector> result_vecs(1);

    const auto status = granule->searcher->inner->FindNeighborsBatched(
        query_dataset,
        absl::MakeSpan(search_params),
        absl::MakeSpan(result_vecs));

    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ScaNN search failed: {}", status.ToString());

    const auto & nn = result_vecs[0];
    NearestNeighbours result;
    result.rows.reserve(nn.size());
    if (parameters->return_distances)
        result.distances = std::vector<float>();

    for (const auto & [idx, dist] : nn)
    {
        result.rows.push_back(static_cast<UInt64>(idx));
        if (result.distances)
        {
            /// ScaNN distances must be converted to match ClickHouse function semantics:
            /// - cosineDistance: ScaNN returns -dot(a_norm, b_norm) = -cos(θ); ClickHouse = 1 - cos(θ)
            /// - L2Distance:     ScaNN returns squared L2; ClickHouse = sqrt(squared L2)
            /// - dotProduct:     ScaNN returns -dot(a, b);  ClickHouse = dot(a, b)
            float converted;
            if (index_params.distance_name == "cosineDistance")
                converted = 1.0f + dist;
            else if (index_params.distance_name == "L2Distance")
                converted = std::sqrt(std::max(0.0f, dist));
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
    const IndexDescription & index_, const ScannIndexParams & params_)
    : IMergeTreeIndex(index_)
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
