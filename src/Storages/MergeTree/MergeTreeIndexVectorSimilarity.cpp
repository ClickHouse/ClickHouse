#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>

#if USE_USEARCH

#include <usearch/index_plugins.hpp>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/formatReadable.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/HashTable/HashMap.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/TargetSpecific.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/castColumn.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <numbers>
#include <ranges>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event USearchAddCount;
    extern const Event USearchAddVisitedMembers;
    extern const Event USearchAddComputedDistances;
    extern const Event USearchSearchCount;
    extern const Event USearchSearchVisitedMembers;
    extern const Event USearchSearchComputedDistances;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 hnsw_candidate_list_size_for_search;
    extern const SettingsFloat vector_search_index_fetch_multiplier;
    extern const SettingsUInt64 max_limit_for_vector_search_queries;
    extern const SettingsBool vector_search_with_rescoring;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_build_vector_similarity_index_thread_pool_size;
}

namespace ErrorCodes
{
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Indexing methods: "hnsw" (usearch graph) and "fastknn" (flat brute-force scan over quantized codes)
const std::set<String> methods = {"hnsw", "fastknn"};

/// Maps from user-facing name to internal name
const std::unordered_map<String, unum::usearch::metric_kind_t> distanceFunctionToMetricKind = {
    {"L2Distance", unum::usearch::metric_kind_t::l2sq_k},
    {"cosineDistance", unum::usearch::metric_kind_t::cos_k},
    {"dotProduct", unum::usearch::metric_kind_t::ip_k}};

/// Maps from user-facing name to internal name
const std::unordered_map<String, unum::usearch::scalar_kind_t> quantizationToScalarKind = {
    {"f64", unum::usearch::scalar_kind_t::f64_k},
    {"f32", unum::usearch::scalar_kind_t::f32_k},
    {"f16", unum::usearch::scalar_kind_t::f16_k},
    {"bf16", unum::usearch::scalar_kind_t::bf16_k},
    {"i8", unum::usearch::scalar_kind_t::i8_k},
    {"b1", unum::usearch::scalar_kind_t::b1x8_k},
    /// 'b1_projected' (fastknn only): same 1-bit packed storage and Hamming scan as 'b1', but each vector is
    /// multiplied by a fixed random projection before sign-binarizing. The projection makes the sign bits behave as
    /// random hyperplane hashes, so Hamming becomes an (unbiased) estimate of the angle between vectors.
    {"b1_projected", unum::usearch::scalar_kind_t::b1x8_k}};
/// Usearch provides more quantizations but ^^ above ones seem the only ones comprehensively supported across all distance functions.

template<typename T>
concept is_set = std::same_as<T, std::set<typename T::key_type, typename T::key_compare, typename T::allocator_type>>;

template<typename T>
concept is_unordered_map = std::same_as<T, std::unordered_map<typename T::key_type, typename T::mapped_type, typename T::hasher, typename T::key_equal, typename T::allocator_type>>;

template <typename T>
String joinByComma(const T & t)
{
    if constexpr (is_set<T>)
    {
        return fmt::format("{}", fmt::join(t, ", "));
    }
    else if constexpr (is_unordered_map<T>)
    {
        auto keys = std::views::keys(t);
        return fmt::format("{}", fmt::join(keys, ", "));
    }
    std::unreachable();
}

}

USearchIndexWithSerialization::USearchIndexWithSerialization(
    size_t dimensions,
    unum::usearch::metric_kind_t metric_kind,
    unum::usearch::scalar_kind_t scalar_kind,
    UsearchHnswParams usearch_hnsw_params)
{
    USearchIndex::metric_t metric(dimensions, metric_kind, scalar_kind);

    unum::usearch::index_dense_config_t config(usearch_hnsw_params.connectivity, usearch_hnsw_params.expansion_add, default_expansion_search);
    config.enable_key_lookups = false; /// we don't do row-to-vector lookups

    auto result = USearchIndex::make(metric, config);
    if (!result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not create vector similarity index. Error: {}", result.error.release());
    swap(result.index);
}

void USearchIndexWithSerialization::serialize(WriteBuffer & ostr) const
{
    auto callback = [&ostr](void * from, size_t n)
    {
        /// USearch may call callback from noexcept function
        try
        {
            ostr.write(reinterpret_cast<const char *>(from), n);
            return true;
        }
        catch (...)
        {
            tryLogCurrentException("VectorSimilarityIndex", "An error while serializing USearch index");
            return false;
        }
    };

    if (auto result = Base::save_to_stream(callback); !result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not save vector similarity index. Error: {}", result.error.release());
}

void USearchIndexWithSerialization::deserialize(ReadBuffer & istr)
{
    auto callback = [&istr](void * from, size_t n)
    {
        /// USearch may call callback from noexcept function
        try
        {
            istr.readStrict(reinterpret_cast<char *>(from), n);
            return true;
        }
        catch (...)
        {
            tryLogCurrentException("VectorSimilarityIndex", "An error while deserializing USearch index");
            return false;
        }
    };

    if (auto result = Base::load_from_stream(callback); !result)
        /// See the comment in MergeTreeIndexGranuleVectorSimilarity::deserializeBinary why we throw here
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not load vector similarity index. Please drop the index and create it again. Error: {}", result.error.release());

    /// USearch pre-allocates internal data structures for at most N threads. This makes the implicit assumption that the caller (this
    /// class) uses at most this number of threads. The problem here is that there is no such guarantee in ClickHouse because of potential
    /// oversubscription. Therefore, set N as 2 * the available cores - that should be pretty safe. In the unlikely case there are still
    /// more threads at runtime than this limit, we patched usearch to return an error.
    try_reserve(unum::usearch::index_limits_t(limits().members, 2 * getNumberOfCPUCoresToUse()));
}

USearchIndexWithSerialization::Statistics USearchIndexWithSerialization::getStatistics() const
{
    USearchIndex::stats_t global_stats = Base::stats();

    Statistics statistics = {
        .max_level = max_level(),
        .connectivity = connectivity(),
        .size = size(),
        .capacity = capacity(),
        .memory_usage = memory_usage(),
        .bytes_per_vector = bytes_per_vector(),
        .scalar_words = scalar_words(),
        .nodes = global_stats.nodes,
        .edges = global_stats.edges,
        .max_edges = global_stats.max_edges,
        .level_stats = {}};

    for (size_t i = 0; i < statistics.max_level; ++i)
        statistics.level_stats.push_back(Base::stats(i));

    return statistics;
}

String USearchIndexWithSerialization::Statistics::toString() const
{
    return fmt::format("max_level = {}, connectivity = {}, size = {}, capacity = {}, memory_usage = {}, bytes_per_vector = {}, scalar_words = {}, nodes = {}, edges = {}, max_edges = {}",
            max_level, connectivity, size, capacity, ReadableSize(memory_usage), bytes_per_vector, scalar_words, nodes, edges, max_edges);

}

size_t USearchIndexWithSerialization::memoryUsageBytes() const
{
    /// Memory consumption is extremely high, asked in Discord: https://discord.com/channels/1063947616615923875/1064496121520590878/1309266814299144223
    return Base::memory_usage();
}

MergeTreeIndexGranuleVectorSimilarity::MergeTreeIndexGranuleVectorSimilarity(
    const String & index_name_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : MergeTreeIndexGranuleVectorSimilarity(index_name_, metric_kind_, scalar_kind_, usearch_hnsw_params_, nullptr)
{
}

MergeTreeIndexGranuleVectorSimilarity::MergeTreeIndexGranuleVectorSimilarity(
    const String & index_name_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_,
    USearchIndexWithSerializationPtr index_)
    : index_name(index_name_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
    , index(std::move(index_))
{
}

void MergeTreeIndexGranuleVectorSimilarity::serializeBinary(WriteBuffer & ostr) const
{
    LOG_TRACE(logger, "Start writing vector similarity index");

    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty vector similarity index {}", backQuote(index_name));

    writeIntBinary(FILE_FORMAT_VERSION, ostr);

    /// Number of dimensions is required in the index constructor,
    /// so it must be written and read separately from the other part
    writeIntBinary(static_cast<UInt64>(index->dimensions()), ostr);

    index->serialize(ostr);

    auto statistics = index->getStatistics();
    LOG_TRACE(logger, "Wrote vector similarity index: {}", statistics.toString());
}

void MergeTreeIndexGranuleVectorSimilarity::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    LOG_TRACE(logger, "Start loading vector similarity index");

    UInt64 file_version = 0;
    readIntBinary(file_version, istr);
    if (file_version != FILE_FORMAT_VERSION)
        throw Exception(
            ErrorCodes::FORMAT_VERSION_TOO_OLD,
            "Vector similarity index could not be loaded because its version is too old (current version: {}, persisted version: {}). Please drop the index and create it again.",
            FILE_FORMAT_VERSION, file_version);
        /// More fancy error handling would be: Set a flag on the index that it failed to load. During usage return all granules, i.e.
        /// behave as if the index does not exist. Since format changes are expected to happen only rarely and it is "only" an index, keep it simple for now.

    UInt64 dimensions = 0;
    readIntBinary(dimensions, istr);
    index = std::make_shared<USearchIndexWithSerialization>(dimensions, metric_kind, scalar_kind, usearch_hnsw_params);

    index->deserialize(istr);

    auto statistics = index->getStatistics();
    LOG_TRACE(logger, "Loaded vector similarity index: {}", statistics.toString());
}

MergeTreeIndexAggregatorVectorSimilarity::MergeTreeIndexAggregatorVectorSimilarity(
    const String & index_name_,
    const Block & index_sample_block_,
    UInt64 dimensions_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , dimensions(dimensions_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSimilarity::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index_name, metric_kind, scalar_kind, usearch_hnsw_params, index);
    index = nullptr;
    return granule;
}

namespace
{

/// Check two things to prevent undefined behavior further down in Usearch
/// - No vector element is +inf, -inf or nan.
/// - In the case of i8 quantization (which is obscure): additionally, the vector magnitude must not be zero.
template <typename T>
void checkVectorIsSane(
    const T * vector,
    size_t dimension,
    unum::usearch::scalar_kind_t scalar_kind,
    int error_code,
    std::string_view context)
{
    double magnitude_squared = 0.0;
    for (size_t i = 0; i != dimension; ++i)
    {
        T casted = static_cast<T>(vector[i]);
        if constexpr (std::is_same_v<T, BFloat16>)
        {
            if (!casted.isFinite())
                throw Exception(error_code,
                    "Vector for vector similarity index ({}) must not contain non-finite values (NaN or Inf)", context);
        }
        else
        {
            if (!std::isfinite(casted))
                throw Exception(error_code,
                    "Vector for vector similarity index ({}) must not contain non-finite values (NaN or Inf)", context);
        }

        if (scalar_kind == unum::usearch::scalar_kind_t::i8_k)
        {
            double v = static_cast<double>(vector[i]);
            magnitude_squared += v * v;
        }
    }

    if (scalar_kind == unum::usearch::scalar_kind_t::i8_k && magnitude_squared == 0.0)
        throw Exception(error_code,
            "Zero-magnitude vectors for vector similarity index ({}) are not supported with `i8` quantization", context);
}

template <typename Column>
void updateImpl(const ColumnArray * column_array, const ColumnArray::Offsets & column_array_offsets, USearchIndexWithSerializationPtr & index, size_t dimensions, [[maybe_unused]] unum::usearch::scalar_kind_t scalar_kind, size_t rows)
{
    const auto & column_array_data = column_array->getData();
    const auto & column_array_data_float = typeid_cast<const Column &>(column_array_data);
    const auto & column_array_data_float_data = column_array_data_float.getData();

    /// Check all sizes are the same
    for (size_t row = 0; row < rows - 1; ++row)
        if (column_array_offsets[row + 1] - column_array_offsets[row] != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column with vector similarity index must have equal length");

    /// Reserving space is mandatory
    size_t max_thread_pool_size = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
    if (max_thread_pool_size == 0)
        max_thread_pool_size = getNumberOfCPUCoresToUse();
    unum::usearch::index_limits_t limits(roundUpToPowerOfTwoOrZero(index->size() + rows), max_thread_pool_size);
    index->reserve(limits);

    /// Vector index creation is slooooow. Add the new rows in parallel. The threadpool is global to avoid oversubscription when multiple
    /// indexes are build simultaneously (e.g. multiple merges run at the same time).
    auto & thread_pool = Context::getGlobalContextInstance()->getBuildVectorSimilarityIndexThreadPool();

    /// The lambda must be declared before the runner so that during stack unwinding
    /// the runner is destroyed first (waits for all tasks) and the lambda is destroyed second.
    auto add_vector_to_index = [&](USearchIndex::vector_key_t key, size_t row)
    {
        /// Check if the query has been cancelled. USearch internally does not check for cancellation,
        /// and a single `add` call can take a very long time under sanitizers. Without this check, KILL QUERY
        /// cannot stop the index building. The check is cheap (reads an atomic flag).
        if (auto query_context = CurrentThread::tryGetQueryContext())
            if (auto query_status = query_context->getProcessListElementSafe())
                query_status->throwIfKilled();

        const typename Column::ValueType & value = column_array_data_float_data[column_array_offsets[row - 1]];

        checkVectorIsSane(&value, dimensions, scalar_kind, ErrorCodes::INCORRECT_DATA, "indexed vector");

        unum::usearch::index_dense_t::add_result_t result;

        /// Note: add is thread-safe
        if constexpr (std::is_same_v<Column, ColumnBFloat16>)
        {
            /// bf16 was standardized with C++23 but libcxx does not support it yet.
            /// As a result, ClickHouse and usearch each emulate bf16 and we need to implement some ugly special handling for bf16 below.
            result = index->add(key, reinterpret_cast<const unum::usearch::bf16_bits_t *>(&value.raw()));
        }
        else
        {
            static_assert(std::is_same_v<Column, ColumnFloat32> || std::is_same_v<Column, ColumnFloat64>);
            result = index->add(key, &value);
        }

        if (!result)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Could not add data to vector similarity index. Error: {}", result.error.release());

        ProfileEvents::increment(ProfileEvents::USearchAddCount);
        ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, result.visited_members);
        ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, result.computed_distances);
    };


    size_t index_size = index->size();
    ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::MERGETREE_VECTOR_SIM_INDEX);
    for (size_t row = 0; row < rows; ++row)
    {
        auto key = static_cast<USearchIndex::vector_key_t>(index_size + row);
        /// Passing add_vector_to_index by reference is safe because it outlives the runner
        runner.enqueueAndKeepTrack([&add_vector_to_index, key, row] { add_vector_to_index(key, row); });
    }

    runner.waitForAllToFinishAndRethrowFirstError();
}

}

void MergeTreeIndexAggregatorVectorSimilarity::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (rows_read == 0)
        return;

    if (rows_read > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Index granularity is too big: more than {} rows per index granule.", std::numeric_limits<UInt32>::max());

    if (index_sample_block.columns() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected that index is build over a single column");

    const auto & index_column_name = index_sample_block.getByPosition(0).name;

    const auto & index_column = block.getByName(index_column_name).column;
    ColumnPtr column_cut = index_column->cut(*pos, rows_read);

    const auto * column_array = typeid_cast<const ColumnArray *>(column_cut.get());
    if (!column_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array(Float32|Float64|BFloat16) column");

    if (column_array->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Array is unexpectedly empty");

    const size_t rows = column_array->size();

    const auto & column_array_offsets = column_array->getOffsets();
    const size_t dimensions_inserted = column_array_offsets[0];

    if (dimensions != dimensions_inserted)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Array values in column with vector similarity index have {} elements, expects {} elements", dimensions_inserted, dimensions);

    if (!index)
        index = std::make_shared<USearchIndexWithSerialization>(dimensions, metric_kind, scalar_kind, usearch_hnsw_params);

    /// We use Usearch's index_dense_t as index type which supports only 4 bio entries according to https://github.com/unum-cloud/usearch/tree/main/cpp
    if (index->size() + rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Size of vector similarity index would exceed 4 billion entries");

    const auto * data_type_array = typeid_cast<const DataTypeArray *>(block.getByName(index_column_name).type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float32|Float64|BFloat16)");

    const TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
    WhichDataType which(nested_type_index);
    if (which.isFloat32())
        updateImpl<ColumnFloat32>(column_array, column_array_offsets, index, dimensions, scalar_kind, rows);
    else if (which.isFloat64())
        updateImpl<ColumnFloat64>(column_array, column_array_offsets, index, dimensions, scalar_kind, rows);
    else if (which.isBFloat16())
        updateImpl<ColumnBFloat16>(column_array, column_array_offsets, index, dimensions, scalar_kind, rows);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");


    *pos += rows_read;
}

MergeTreeIndexConditionVectorSimilarity::MergeTreeIndexConditionVectorSimilarity(
    const std::optional<VectorSearchParameters> & parameters_,
    const String & index_column_,
    unum::usearch::metric_kind_t metric_kind_,
    ContextPtr context)
    : parameters(parameters_)
    , index_column(index_column_)
    , metric_kind(metric_kind_)
    , expansion_search(context->getSettingsRef()[Setting::hnsw_candidate_list_size_for_search])
    , index_fetch_multiplier(context->getSettingsRef()[Setting::vector_search_index_fetch_multiplier])
    , max_limit(context->getSettingsRef()[Setting::max_limit_for_vector_search_queries])
    , is_rescoring(context->getSettingsRef()[Setting::vector_search_with_rescoring])
{
    static constexpr auto MAX_INDEX_FETCH_MULTIPLIER = 1000.0;

    if (expansion_search == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'hnsw_candidate_list_size_for_search' must not be 0");

    if (!std::isfinite(index_fetch_multiplier)
        || index_fetch_multiplier <= 0.0f || static_cast<double>(index_fetch_multiplier) > MAX_INDEX_FETCH_MULTIPLIER
        || (parameters && !std::isfinite(static_cast<double>(index_fetch_multiplier) * static_cast<double>(parameters->limit))))
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'vector_search_index_fetch_multiplier' must be greater than 0.0 and less than {}", MAX_INDEX_FETCH_MULTIPLIER);
}

bool MergeTreeIndexConditionVectorSimilarity::mayBeTrueOnGranule(MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn & /*update_partial_disjunction_result_fn*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for vector similarity indexes");
}

bool MergeTreeIndexConditionVectorSimilarity::alwaysUnknownOrTrue() const
{
    if (!parameters)
        return true;

    /// The vector similarity index was build on a specific column.
    /// It can only be used if the ORDER BY clause in the SELECT query is against the same column.
    if (parameters->column != index_column)
        return true;

    /// The vector similarity index was build for a specific distance function.
    /// It can only be used if the ORDER BY clause in the SELECT query uses the same distance function.
    if ((parameters->distance_function == "L2Distance" && metric_kind != unum::usearch::metric_kind_t::l2sq_k)
        || (parameters->distance_function == "cosineDistance" && metric_kind != unum::usearch::metric_kind_t::cos_k && metric_kind != unum::usearch::metric_kind_t::hamming_k)
        || (parameters->distance_function == "dotProduct" && metric_kind != unum::usearch::metric_kind_t::ip_k))
            return true;

    return false;
}

NearestNeighbours MergeTreeIndexConditionVectorSimilarity::calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule_) const
{
    if (!parameters)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected vector_search_parameters to be set");

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleVectorSimilarity>(granule_);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    const USearchIndexWithSerializationPtr index = granule->index;

    if (parameters->reference_vector.size() != index->dimensions())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
            parameters->reference_vector.size(), index->dimensions());

    checkVectorIsSane(
        parameters->reference_vector.data(), parameters->reference_vector.size(),
        granule->scalar_kind, ErrorCodes::INCORRECT_QUERY, "reference vector in the SELECT query");

    size_t limit = parameters->limit;
    if (parameters->additional_filters_present || is_rescoring)
        /// Additional filters mean post-filtering which means that matches may be removed. To compensate, allow to fetch more rows by a factor.
        /// Similarly, if rescoring is on, fetch more neighbours from the index and pass them for the final re-ranking by ORDER BY ... LIMIT.
        limit = std::min(static_cast<size_t>(static_cast<double>(limit) * static_cast<double>(index_fetch_multiplier)), max_limit);

    /// We want to run the search with the user-provided value for setting hnsw_candidate_list_size_for_search (aka. expansion_search).
    /// The way to do this in USearch is to call index_dense_gt::change_expansion_search. Unfortunately, this introduces a need to
    /// synchronize index access, see https://github.com/unum-cloud/usearch/issues/500. As a workaround, we extended USearch' search method
    /// to accept a custom expansion_add setting. The config value is only used on the fly, i.e. not persisted in the index.
    auto search_result = index->search(parameters->reference_vector.data(), limit, USearchIndex::any_thread(), false, expansion_search);
    if (!search_result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not search in vector similarity index. Error: {}", search_result.error.release());

    NearestNeighbours result;
    result.rows.resize(search_result.size());
    if (parameters->return_distances)
    {
        result.distances = std::vector<float>(search_result.size());
        search_result.dump_to(result.rows.data(), result.distances.value().data());
    }
    else
    {
        search_result.dump_to(result.rows.data());
    }

    ProfileEvents::increment(ProfileEvents::USearchSearchCount);
    ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, search_result.visited_members);
    ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, search_result.computed_distances);

    return result;
}

/// ============================ Flat ("fastknn") implementation ============================

MergeTreeIndexGranuleVectorSimilarityFlat::MergeTreeIndexGranuleVectorSimilarityFlat(
    const String & index_name_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    size_t dimensions_)
    : index_name(index_name_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , dimensions(dimensions_)
    , bytes_per_vector(dimensions_ / 8) /// b1 (binary) packs 8 dimensions per byte
{
}

void MergeTreeIndexGranuleVectorSimilarityFlat::serializeBinary(WriteBuffer & ostr) const
{
    writeIntBinary(FILE_FORMAT_VERSION, ostr);
    writeIntBinary(static_cast<UInt64>(dimensions), ostr);
    writeIntBinary(static_cast<UInt64>(bytes_per_vector), ostr);
    writeIntBinary(static_cast<UInt64>(num_vectors), ostr);
    if (!codes.empty())
        ostr.write(reinterpret_cast<const char *>(codes.data()), codes.size());
}

void MergeTreeIndexGranuleVectorSimilarityFlat::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt64 file_version = 0;
    readIntBinary(file_version, istr);
    if (file_version != FILE_FORMAT_VERSION)
        throw Exception(ErrorCodes::FORMAT_VERSION_TOO_OLD, "Unsupported flat vector similarity index version {}", file_version);

    UInt64 dims = 0;
    UInt64 bpv = 0;
    UInt64 count = 0;
    readIntBinary(dims, istr);
    readIntBinary(bpv, istr);
    readIntBinary(count, istr);
    bytes_per_vector = bpv;
    num_vectors = count;
    codes.resize(num_vectors * bytes_per_vector);
    if (!codes.empty())
        istr.readStrict(reinterpret_cast<char *>(codes.data()), codes.size());
}

namespace
{

/// Fixed seed for the random projection. The projection must be identical at index-build time and at query
/// time, so it is regenerated deterministically from this seed rather than stored. If this generator ever changes,
/// the flat granule FILE_FORMAT_VERSION must be bumped and existing indexes rebuilt.
constexpr UInt64 RANDOM_PROJECTION_SEED = 0x9E3779B97F4A7C15ULL;

/// Second, independent seed for the 'turboquant' QJL (Quantized Johnson-Lindenstrauss) projection, applied to the
/// MSE-quantization residual. Must differ from RANDOM_PROJECTION_SEED so the two projections are independent.
constexpr UInt64 RANDOM_PROJECTION_SEED_QJL = 0xD1B54A32D192ED03ULL;

/// Number of (sign-flip + Walsh-Hadamard) rounds. Three rounds of HD approximate a random rotation well, so the
/// resulting sign bits behave as random-hyperplane hashes (angular LSH).
constexpr int PROJECTION_ROUNDS = 3;

/// Smallest power of two >= n.
inline size_t projectionPaddedSize(size_t n)
{
    size_t p = 1;
    while (p < n)
        p <<= 1;
    return p;
}

/// In-place Walsh-Hadamard transform (n must be a power of two), O(n log n), add/sub only.
inline void walshHadamardTransform(float * a, size_t n)
{
    for (size_t len = 1; len < n; len <<= 1)
        for (size_t i = 0; i < n; i += (len << 1))
            for (size_t j = i; j < i + len; ++j)
            {
                const float u = a[j];
                const float v = a[j + len];
                a[j] = u + v;
                a[j + len] = u - v;
            }
}

/// A fast structured random projection: PROJECTION_ROUNDS blocks of (random ±1 diagonal) * Walsh-Hadamard, replacing
/// a dense d*d Gaussian matrix (O(d*d) per vector) with an O(d log d) transform. Returns the concatenated sign-flip
/// diagonals (PROJECTION_ROUNDS * padded entries, each +-1), generated deterministically from the fixed seed.
std::vector<float> generateRandomProjection(size_t dimensions, UInt64 seed = RANDOM_PROJECTION_SEED)
{
    const size_t padded = projectionPaddedSize(dimensions);
    std::vector<float> sign_flips(static_cast<size_t>(PROJECTION_ROUNDS) * padded);

    /// splitmix64 bit stream -> +-1
    UInt64 state = seed;
    for (auto & value : sign_flips)
    {
        UInt64 z = (state += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        z = z ^ (z >> 31);
        value = (z & 1) ? 1.0f : -1.0f;
    }
    return sign_flips;
}

/// Projects `x` (dimensions elements) using the structured transform into `work` (size = padded). The first
/// `dimensions` entries of `work` are the projected coordinates whose signs form the code. `work` must have at least
/// `padded` (= sign_flips.size() / PROJECTION_ROUNDS) elements.
template <typename T>
void applyRandomProjection(const std::vector<float> & sign_flips, const T * x, size_t dimensions, float * work)
{
    const size_t padded = sign_flips.size() / PROJECTION_ROUNDS;
    for (size_t i = 0; i < dimensions; ++i)
        work[i] = static_cast<float>(x[i]);
    for (size_t i = dimensions; i < padded; ++i)
        work[i] = 0.0f;

    for (int round = 0; round < PROJECTION_ROUNDS; ++round)
    {
        const float * flip = sign_flips.data() + static_cast<size_t>(round) * padded;
        for (size_t i = 0; i < padded; ++i)
            work[i] *= flip[i];
        walshHadamardTransform(work, padded);
    }
}

/// RaBitQ (1-bit, asymmetric estimator), data-oblivious / no codebook (Gao & Long, "RaBitQ", SIGMOD 2024).
/// After the same random projection used by 'b1_projected', the data vector is sign-binarized (1 bit/coordinate) exactly
/// like 'b1_projected'. RaBitQ additionally stores a per-vector correction factor that turns the sign code into an
/// UNBIASED estimator of the cosine similarity. With `o` the unit (rotated) data vector and `s_i = sign(o_i)`, the
/// stored value is `inv_factor = ||o||_2 / ||o||_1` (note `||o||_2 = 1`, so `factor = sum_i |o_i| >= 1` and
/// `inv_factor` in (0, 1]). The estimator is asymmetric (the query has more resolution than the 1-bit data):
///   cos(o, q) ~= (sum_i s_i * q_i) * (1 / ||q||_2) * inv_factor,    with sum_i s_i * q_i = 2 * sum_{s_i = +1} q_i - sum_i q_i.
/// Layout per vector: `dimensions / 8` packed sign bits, followed by the 4-byte `inv_factor`. The sign code matches
/// 'b1_projected' bit-for-bit; the extra factor and the query resolution are what lift recall above plain SimHash.
///
/// The data half `sum_{s_i = +1} q_i` is the inner product of the 1-bit data code with the query. Rather than a
/// full-precision float dot product (which costs like the multi-bit 'turboquant' scan, not like the 1-bit 'b1' scan),
/// the query is uniformly quantized to RABITQ_QUERY_BITS bits and stored "bit-sliced" as one bit-plane per bit. The
/// inner product then reduces to a few AND+popcount passes over the data code (see `RaBitQQuery`), which is back in
/// 'b1's popcount regime - the same trick RaBitQ/BBQ use to keep the 1-bit scan fast.
template <typename T>
void encodeRaBitQ(const std::vector<float> & projection, const T * x, size_t dimensions, float * work, char * dst)
{
    applyRandomProjection(projection, x, dimensions, work);

    const size_t code_bytes = dimensions / 8;
    std::memset(dst, 0, code_bytes);
    double l1 = 0.0;
    double l2sq = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
    {
        const float w = work[i];
        l1 += std::abs(static_cast<double>(w));
        l2sq += static_cast<double>(w) * static_cast<double>(w);
        if (w >= 0.0f)
            dst[i >> 3] |= static_cast<char>(1u << (i & 7u));
    }
    /// inv_factor = ||o||_2 / ||o||_1 = sqrt(l2sq) / l1. A zero vector -> 0 (estimated cosine 0, i.e. distance 1).
    const float inv_factor = (l1 > 0.0) ? static_cast<float>(std::sqrt(l2sq) / l1) : 0.0f;
    std::memcpy(dst + code_bytes, &inv_factor, sizeof(float));
}

/// Number of bits the query is uniformly quantized to for the bit-sliced scan. 4 bits gives the query 16 levels, which
/// (with the per-vector correction factor) keeps the asymmetric estimator's accuracy while making the scan popcount-based.
constexpr int RABITQ_QUERY_BITS = 4;

/// The query side of the RaBitQ estimator, prepared once per query. The (rotated) query is uniformly quantized to
/// RABITQ_QUERY_BITS bits per coordinate: `q_tilde_i = round((q_i - q_min) / delta)` in [0, 2^bits - 1]. It is stored
/// "bit-sliced" as RABITQ_QUERY_BITS bit-planes, each `code_bytes = dimensions / 8` bytes with the same bit layout as a
/// data code (coordinate i -> byte i/8, bit i%8). `q_total = sum_i q_i` and `inv_qnorm = 1 / ||q||_2` use the exact
/// (un-quantized) query so only the data-code inner product is approximate.
struct RaBitQQuery
{
    std::vector<UInt8> planes; /// RABITQ_QUERY_BITS * code_bytes, plane j at offset j * code_bytes
    size_t code_bytes = 0;
    float delta = 0.0f;
    float q_min = 0.0f;
    float q_total = 0.0f;
    float inv_qnorm = 0.0f;
};

/// Build the bit-sliced query from a (rotated, full-precision) query vector `q` of `dimensions` coordinates.
RaBitQQuery buildRaBitQQuery(const float * q, size_t dimensions)
{
    RaBitQQuery out;
    out.code_bytes = dimensions / 8;
    out.planes.assign(static_cast<size_t>(RABITQ_QUERY_BITS) * out.code_bytes, 0);

    double total = 0.0;
    double sumsq = 0.0;
    float q_min = q[0];
    float q_max = q[0];
    for (size_t i = 0; i < dimensions; ++i)
    {
        total += static_cast<double>(q[i]);
        sumsq += static_cast<double>(q[i]) * static_cast<double>(q[i]);
        q_min = std::min(q_min, q[i]);
        q_max = std::max(q_max, q[i]);
    }
    out.q_total = static_cast<float>(total);
    out.inv_qnorm = (sumsq > 0.0) ? static_cast<float>(1.0 / std::sqrt(sumsq)) : 0.0f;
    out.q_min = q_min;

    constexpr int levels = (1 << RABITQ_QUERY_BITS) - 1; /// 15 for 4 bits
    const float range = q_max - q_min;
    out.delta = (range > 0.0f) ? (range / static_cast<float>(levels)) : 0.0f;
    const float inv_delta = (out.delta > 0.0f) ? (1.0f / out.delta) : 0.0f;

    for (size_t i = 0; i < dimensions; ++i)
    {
        int q_tilde = static_cast<int>(std::lround((q[i] - q_min) * inv_delta));
        q_tilde = std::clamp(q_tilde, 0, levels);
        for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
            if ((q_tilde >> j) & 1)
                out.planes[static_cast<size_t>(j) * out.code_bytes + (i >> 3)] |= static_cast<UInt8>(1u << (i & 7u));
    }
    return out;
}

/// Reconstruct `sum_i s_i * q_i` (the inner product of the +-1 sign code with the bit-sliced query) from the popcounts.
/// `pc = popcount(code)` = number of +1 sign bits; `plane_pc[j] = popcount(code AND query_plane_j)`. This is the shared
/// estimator core used by both 'rabitq' and the exact 'turboquant' (which sums two such dots).
inline float raBitQNumeratorFromCounts(const RaBitQQuery & q, UInt64 pc, const UInt64 * plane_pc)
{
    /// sum_i b_i * q_tilde_i = sum_j 2^j * popcount(code AND plane_j); approx sum over set bits of q_i:
    ///   sum_{b_i=1} q_i ~= delta * (sum_j 2^j * plane_pc[j]) + q_min * pc.
    UInt64 weighted = 0;
    for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
        weighted += plane_pc[j] << j;
    const float sum_set = q.delta * static_cast<float>(weighted) + q.q_min * static_cast<float>(pc);
    return 2.0f * sum_set - q.q_total; /// sum_i s_i * q_i
}

/// Scalar bit-sliced dot: AND+popcount the +-1 sign code against each query bit-plane, 8 bytes at a time, and return
/// `sum_i s_i * q_i`. `code` points at `q.code_bytes` packed sign bits (no trailing factor is read here).
inline float raBitQNumeratorScalar(const RaBitQQuery & q, const char * code)
{
    const size_t code_bytes = q.code_bytes;
    const UInt8 * planes = q.planes.data();
    UInt64 pc = 0;
    UInt64 plane_pc[RABITQ_QUERY_BITS] = {};
    size_t b = 0;
    for (; b + 8 <= code_bytes; b += 8)
    {
        UInt64 cw = 0;
        std::memcpy(&cw, code + b, sizeof(cw));
        pc += static_cast<UInt64>(std::popcount(cw));
        for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
        {
            UInt64 pw = 0;
            std::memcpy(&pw, planes + static_cast<size_t>(j) * code_bytes + b, sizeof(pw));
            plane_pc[j] += static_cast<UInt64>(std::popcount(cw & pw));
        }
    }
    for (; b < code_bytes; ++b)
    {
        const unsigned cw = static_cast<UInt8>(code[b]);
        pc += static_cast<UInt64>(std::popcount(cw));
        for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
            plane_pc[j] += static_cast<UInt64>(std::popcount(cw & static_cast<unsigned>(planes[static_cast<size_t>(j) * code_bytes + b])));
    }
    return raBitQNumeratorFromCounts(q, pc, plane_pc);
}

#if USE_MULTITARGET_CODE
/// AVX-512 (Ice Lake) bit-sliced dot: VPOPCNTDQ counts 8x 64-bit lanes per instruction, so each 512-bit chunk of the
/// sign code is AND-ed with each query bit-plane and popcounted in one `_mm512_popcnt_epi64`. This keeps the 1-bit
/// scan in the popcount regime (close to 'b1') rather than the float-arithmetic regime of the old 'turboquant'.
X86_64_ICELAKE_FUNCTION_SPECIFIC_ATTRIBUTE
inline float raBitQNumeratorICELAKE(const RaBitQQuery & q, const char * code)
{
    const size_t code_bytes = q.code_bytes;
    const UInt8 * planes = q.planes.data();
    __m512i acc_pc = _mm512_setzero_si512();
    __m512i acc[RABITQ_QUERY_BITS];
    for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
        acc[j] = _mm512_setzero_si512();

    size_t b = 0;
    for (; b + 64 <= code_bytes; b += 64)
    {
        const __m512i c = _mm512_loadu_si512(reinterpret_cast<const void *>(code + b));
        acc_pc = _mm512_add_epi64(acc_pc, _mm512_popcnt_epi64(c));
        for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
        {
            const __m512i pj = _mm512_loadu_si512(reinterpret_cast<const void *>(planes + static_cast<size_t>(j) * code_bytes + b));
            acc[j] = _mm512_add_epi64(acc[j], _mm512_popcnt_epi64(_mm512_and_si512(c, pj)));
        }
    }
    UInt64 pc = _mm512_reduce_add_epi64(acc_pc);
    UInt64 plane_pc[RABITQ_QUERY_BITS];
    for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
        plane_pc[j] = _mm512_reduce_add_epi64(acc[j]);

    /// Tail (code_bytes not a multiple of 64): finish byte-wise.
    for (; b < code_bytes; ++b)
    {
        const unsigned cw = static_cast<UInt8>(code[b]);
        pc += static_cast<UInt64>(std::popcount(cw));
        for (int j = 0; j < RABITQ_QUERY_BITS; ++j)
            plane_pc[j] += static_cast<UInt64>(std::popcount(cw & static_cast<unsigned>(planes[static_cast<size_t>(j) * code_bytes + b])));
    }
    return raBitQNumeratorFromCounts(q, pc, plane_pc);
}
#endif

/// Dispatch to the Ice Lake (VPOPCNTDQ) kernel when available (decided once per query), else the scalar version.
inline float raBitQNumeratorFast(const RaBitQQuery & q, const char * code, bool use_icelake)
{
#if USE_MULTITARGET_CODE
    if (use_icelake)
        return raBitQNumeratorICELAKE(q, code);
#endif
    (void)use_icelake;
    return raBitQNumeratorScalar(q, code);
}

/// 'rabitq' estimator -> cosineDistance: cos = (sum_i s_i q_i) * inv_qnorm * inv_factor. `inv_factor` follows the
/// `q.code_bytes` sign bits in the code.
inline float raBitQDistanceFast(const RaBitQQuery & q, const char * code, bool use_icelake)
{
    const float numerator = raBitQNumeratorFast(q, code, use_icelake);
    float inv_factor = NAN;
    std::memcpy(&inv_factor, code + q.code_bytes, sizeof(float));
    return 1.0f - numerator * q.inv_qnorm * inv_factor;
}

/// ---- Exact TurboQuant (inner-product / cosine), data-oblivious / no codebook (Zandieh et al., arXiv:2504.19874) ----
/// The MSE-optimal scalar quantizer is biased for inner products, so TurboQuant uses a two-stage scheme: a (b-1)-bit MSE
/// quantizer plus a 1-bit QJL (Quantized Johnson-Lindenstrauss) transform on the residual, giving an UNBIASED estimator.
/// Our 2-bit version is the faithful b=2 case: 1-bit MSE + 1-bit QJL + a per-vector residual norm.
///
/// Let `a` be the unit (rotated by P1) data vector (coordinates ~ N(0, 1/d)). MSE 1-bit: s = sign(a), reconstruction
/// `a~_mse = c0 * s` with c0 = E[|a_i|] = sqrt(2/pi)/sqrt(d). Residual `r = a - a~_mse`, `gamma = ||r||_2`. QJL: a second
/// independent projection P2, `z = sign(P2 * r)`. The asymmetric cosine estimator (query y stays full precision, only
/// the data is quantized) is, with q1 = unit(P1 y):
///   cos(y, x) ~= c0 * <q1, s> + k * gamma * <P2 q1, z>,    k = sqrt(pi/2) / (d * padded).
/// The (d * padded) in k (vs the paper's sqrt(pi/2)/d) accounts for our unnormalized 3-round Walsh-Hadamard projection,
/// whose per-coordinate output variance is padded^2 * ||v||^2 rather than the unit-variance Gaussian the paper assumes.
/// Each <., .> is a sum of a full-precision (bit-sliced) query against a +-1 sign code - the same popcount dot as RaBitQ.
/// Layout per vector: d/8 MSE sign bits, d/8 QJL sign bits, then the 4-byte `gamma` (d/4 + sizeof(float) bytes total).
constexpr double TURBOQUANT_PI = std::numbers::pi;

template <typename T>
void encodeTurboQuant(const std::vector<float> & p1, const std::vector<float> & p2,
                      const T * x, size_t dimensions, float * work1, float * work2, char * dst)
{
    const float c0 = static_cast<float>(std::sqrt(2.0 / (TURBOQUANT_PI * static_cast<double>(dimensions))));

    /// Stage 1: rotate by P1 and normalize the first `dimensions` coordinates to unit L2.
    applyRandomProjection(p1, x, dimensions, work1);
    double nrm2 = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
        nrm2 += static_cast<double>(work1[i]) * static_cast<double>(work1[i]);
    const float inv_norm = (nrm2 > 0.0) ? static_cast<float>(1.0 / std::sqrt(nrm2)) : 0.0f;

    const size_t code_bytes = dimensions / 8;
    std::memset(dst, 0, 2 * code_bytes);

    /// MSE signs s = sign(a); residual r = a - c0 * s into `work2`.
    for (size_t i = 0; i < dimensions; ++i)
    {
        const float a = work1[i] * inv_norm;
        if (a >= 0.0f)
        {
            dst[i >> 3] |= static_cast<char>(1u << (i & 7u));
            work2[i] = a - c0;
        }
        else
            work2[i] = a + c0;
    }

    double g2 = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
        g2 += static_cast<double>(work2[i]) * static_cast<double>(work2[i]);
    const float gamma = static_cast<float>(std::sqrt(g2));

    /// Stage 2 (QJL): z = sign(P2 * r). `work1` (P1 output) is free to reuse as the P2 output buffer.
    applyRandomProjection(p2, work2, dimensions, work1);
    char * z_dst = dst + code_bytes;
    for (size_t i = 0; i < dimensions; ++i)
        if (work1[i] >= 0.0f)
            z_dst[i >> 3] |= static_cast<char>(1u << (i & 7u));

    std::memcpy(dst + 2 * code_bytes, &gamma, sizeof(float));
}

/// The query state for the exact TurboQuant scan: the bit-sliced unit rotated query (for the MSE-sign dot) and the
/// bit-sliced P2-projection of it (for the QJL-sign dot), plus the two estimator constants.
struct TurboQuantQuery
{
    RaBitQQuery mse;
    RaBitQQuery qjl;
    float c0 = 0.0f;
    float k = 0.0f;
};

TurboQuantQuery buildTurboQuantQuery(const std::vector<float> & p1, const std::vector<float> & p2,
                                     const Float64 * y, size_t dimensions)
{
    const size_t padded = p1.size() / PROJECTION_ROUNDS;
    std::vector<float> work1(padded);
    std::vector<float> work2(padded);

    applyRandomProjection(p1, y, dimensions, work1.data());
    double nrm2 = 0.0;
    for (size_t i = 0; i < dimensions; ++i)
        nrm2 += static_cast<double>(work1[i]) * static_cast<double>(work1[i]);
    const float inv_norm = (nrm2 > 0.0) ? static_cast<float>(1.0 / std::sqrt(nrm2)) : 0.0f;
    for (size_t i = 0; i < dimensions; ++i)
        work1[i] *= inv_norm; /// q1 = unit(P1 y)

    TurboQuantQuery out;
    out.mse = buildRaBitQQuery(work1.data(), dimensions);
    applyRandomProjection(p2, work1.data(), dimensions, work2.data()); /// P2 * q1
    out.qjl = buildRaBitQQuery(work2.data(), dimensions);
    out.c0 = static_cast<float>(std::sqrt(2.0 / (TURBOQUANT_PI * static_cast<double>(dimensions))));
    out.k = static_cast<float>(std::sqrt(TURBOQUANT_PI / 2.0) / (static_cast<double>(dimensions) * static_cast<double>(padded)));
    return out;
}

/// Exact TurboQuant cosine estimator -> cosineDistance. `code` is `d/8` MSE sign bits, `d/8` QJL sign bits, then `gamma`.
inline float turboQuantDistanceFast(const TurboQuantQuery & q, const char * code, bool use_icelake)
{
    const size_t code_bytes = q.mse.code_bytes; /// = dimensions / 8
    const float mse_dot = raBitQNumeratorFast(q.mse, code, use_icelake);
    const float qjl_dot = raBitQNumeratorFast(q.qjl, code + code_bytes, use_icelake);
    float gamma = NAN;
    std::memcpy(&gamma, code + 2 * code_bytes, sizeof(float));
    const float cosine = q.c0 * mse_dot + q.k * gamma * qjl_dot;
    return 1.0f - cosine;
}

/// ----------------------------- 'e8' quantization (data-independent lattice product quantization) -----------------------------
/// Product quantization whose codebook is the fixed E8 lattice instead of a trained (k-means) codebook. After the same
/// random rotation used by the other projection-based methods, each 8-dimensional sub-vector is quantized to the nearest
/// point of a truncated, scaled E8 lattice. E8 is the optimal lattice quantizer in 8 dimensions, and because the random
/// rotation makes the sub-vectors approximately isotropic, this fixed lattice is near-optimal WITHOUT any data-dependent
/// training - so, exactly like `generateRandomProjection`, the codebook is regenerated deterministically from
/// (dimensions, bits) at build and query time and never stored (no dictionary, no codebook persistence).
///
/// Unlike 'rabitq'/'turboquant' this supports both cosineDistance and L2Distance. The scan estimates the asymmetric
/// inner product <q_hat, x_hat> of the unit (rotated) query and data directions via an ADC lookup table, and the stored
/// per-vector L2 norm reconstructs the true distance for either metric:
///   cosineDistance: 1 - <q_hat, x_hat>
///   L2Distance:     ||q||^2 + ||x||^2 - 2 ||q|| ||x|| <q_hat, x_hat>
/// Layout per vector: M = dimensions/8 sub-quantizer codes (1 byte each for bits <= 8, else 2 bytes), then the 4-byte
/// L2 norm of the original vector.
constexpr size_t E8_SUBDIM = 8;

/// Nearest point of D8 = { integer vectors with even coordinate sum } to `y` (8 doubles), returned as integers in `out`.
inline void nearestD8(const double * y, int * out)
{
    Int64 isum = 0;
    size_t worst = 0;
    double worst_delta = -1.0;
    for (size_t i = 0; i < E8_SUBDIM; ++i)
    {
        const double r = std::nearbyint(y[i]);
        const int ri = static_cast<int>(r);
        out[i] = ri;
        isum += ri;
        const double delta = std::abs(y[i] - r);
        if (delta > worst_delta)
        {
            worst_delta = delta;
            worst = i;
        }
    }
    /// Odd sum -> not in D8; flip the worst-rounded coordinate to its second-nearest integer to restore an even sum.
    if ((isum & 1) != 0)
        out[worst] += (y[worst] >= static_cast<double>(out[worst])) ? 1 : -1;
}

/// Nearest E8 point to `y`, written to `out2` as 2*coordinate integers (so all-integer: even entries = the integer
/// coset, odd entries = the half-integer coset). The half-integer coset is handled by quantizing y - 1/2 to D8 and
/// shifting back. Returns nothing; this is O(d).
inline void nearestE8(const double * y, int * out2)
{
    int a[E8_SUBDIM];
    nearestD8(y, a);
    double da = 0.0;
    for (size_t i = 0; i < E8_SUBDIM; ++i)
    {
        const double d = y[i] - a[i];
        da += d * d;
    }

    double yshift[E8_SUBDIM];
    for (size_t i = 0; i < E8_SUBDIM; ++i)
        yshift[i] = y[i] - 0.5;
    int b[E8_SUBDIM];
    nearestD8(yshift, b);
    double db = 0.0;
    for (size_t i = 0; i < E8_SUBDIM; ++i)
    {
        const double d = yshift[i] - b[i];
        db += d * d;
    }

    if (da <= db)
        for (size_t i = 0; i < E8_SUBDIM; ++i)
            out2[i] = 2 * a[i];          /// integer coset -> even entries
    else
        for (size_t i = 0; i < E8_SUBDIM; ++i)
            out2[i] = 2 * b[i] + 1;      /// half-integer coset -> odd entries
}

/// Pack the 8 (2*coordinate) integers of a lattice point into a 64-bit key. Entries are small (|2*coord| <= 7 for the
/// shells we enumerate), so int8 per entry is exact.
inline UInt64 e8Key(const int * coord2)
{
    UInt64 k = 0;
    for (size_t i = 0; i < E8_SUBDIM; ++i)
        k = (k << 8) | static_cast<UInt8>(static_cast<int8_t>(coord2[i]));
    return k;
}

struct E8Codebook
{
    size_t bits = 0;
    size_t num_points = 0;                              /// 2^bits
    float scale = 0.0f;                                 /// alpha: lattice units -> real (rotated, unit-normalized) space
    bool use_clamp = false;                             /// whether to clamp query points into the fully-contained region
    float clamp_radius = 0.0f;                          /// clamp radius (lattice units); guarantees decode lands in-set
    std::vector<float> coords;                          /// num_points * 8, already multiplied by `scale`
    HashMap<UInt64, UInt32> point_to_index;             /// e8Key(2*coord) -> codebook index (flat, cache-friendly)
};

/// Recursively enumerate all E8 lattice points (in 2*coordinate units, so all entries share parity `parity`) whose
/// squared norm (in 2*coordinate units) is <= `max_sumsq`. E8 membership reduces to: all entries same parity and the
/// sum of the 2*coordinates is divisible by 4.
void e8Collect(int depth, Int64 sumsq, Int64 max_sumsq, int parity, std::array<int, E8_SUBDIM> & cur, std::vector<std::pair<int, std::array<int, E8_SUBDIM>>> & out)
{
    if (depth == static_cast<int>(E8_SUBDIM))
    {
        Int64 s = 0;
        for (size_t i = 0; i < E8_SUBDIM; ++i)
            s += cur[i];
        if (((s % 4) + 4) % 4 != 0)
            return;
        out.emplace_back(static_cast<int>(sumsq), cur);
        return;
    }
    const Int64 remaining = max_sumsq - sumsq;
    const int maxc = static_cast<int>(std::floor(std::sqrt(static_cast<double>(remaining))));
    for (int v = -maxc; v <= maxc; ++v)
    {
        if ((((v % 2) + 2) % 2) != parity)
            continue;
        const Int64 ns = sumsq + static_cast<Int64>(v) * v;
        if (ns > max_sumsq)
            continue;
        cur[static_cast<size_t>(depth)] = v;
        e8Collect(depth + 1, ns, max_sumsq, parity, cur, out);
    }
}

/// Build the (data-independent) E8 codebook of 2^bits points for a given vector dimensionality. The lattice is truncated
/// to the 2^bits points closest to the origin (deterministic tie-break by squared norm then lexicographic coordinates)
/// and scaled so its RMS radius matches that of an 8-dim sub-vector of a unit vector (sqrt(8 / dimensions)).
std::shared_ptr<const E8Codebook> buildE8Codebook(size_t dimensions, size_t bits)
{
    const size_t num_points = static_cast<size_t>(1) << bits;

    std::vector<std::pair<int, std::array<int, E8_SUBDIM>>> pts;
    Int64 max_sumsq = 8;
    while (true)
    {
        pts.clear();
        std::array<int, E8_SUBDIM> cur{};
        e8Collect(0, 0, max_sumsq, 0, cur, pts); /// integer coset
        e8Collect(0, 0, max_sumsq, 1, cur, pts); /// half-integer coset
        if (pts.size() >= num_points)
            break;
        max_sumsq *= 2;
    }

    std::sort(pts.begin(), pts.end(),
        [](const auto & x, const auto & y)
        {
            if (x.first != y.first)
                return x.first < y.first;
            return x.second < y.second;
        });

    /// `complete_q` is the squared norm (in 2*coordinate units) of the largest shell that is ENTIRELY contained in the
    /// truncated set. Inside the corresponding radius the E8 decoder is guaranteed to land on a stored point, so we can
    /// avoid the (expensive) brute-force fallback entirely by clamping query points there. The set keeps the
    /// `num_points` points closest to the origin; the outermost shell may be only partially included.
    const int last_q = pts[num_points - 1].first;
    int complete_q = last_q;
    if (num_points < pts.size() && pts[num_points].first == last_q)
    {
        size_t i = num_points;
        while (i > 0 && pts[i - 1].first == last_q)
            --i;
        complete_q = (i > 0) ? pts[i - 1].first : 0;
    }
    const double r_complete = std::sqrt(static_cast<double>(complete_q) / 4.0);
    const double r_ref = (r_complete > 0.5) ? r_complete : std::sqrt(static_cast<double>(last_q) / 4.0);

    pts.resize(num_points);

    auto cb = std::make_shared<E8Codebook>();
    cb->bits = bits;
    cb->num_points = num_points;

    /// Scale so a typical sub-vector norm (sqrt(8/dimensions) for a unit rotated vector) maps to r_ref / 1.75. The
    /// sub-vector norm is concentrated, so r_ref then corresponds to roughly +3 sigma and almost every point decodes
    /// inside the fully-contained region.
    const double target_rms = std::sqrt(static_cast<double>(E8_SUBDIM) / static_cast<double>(dimensions));
    cb->scale = static_cast<float>(1.75 * target_rms / std::max(r_ref, 1e-6));
    const double typical_radius = static_cast<double>(target_rms) / std::max(static_cast<double>(cb->scale), 1e-12);

    /// Clamp query points to (r_complete - covering_radius) when that comfortably exceeds the typical data radius (the
    /// E8 covering radius is 1). The clamp then guarantees every decode lands on a stored point WITHOUT collapsing the
    /// (concentrated) data onto the origin - which is only possible for codebooks large enough to have inner room
    /// (otherwise the small codebook's brute-force fallback is cheap anyway).
    if (r_complete - 1.0 > typical_radius)
    {
        cb->use_clamp = true;
        cb->clamp_radius = static_cast<float>(r_complete - 1.0);
    }

    cb->coords.resize(num_points * E8_SUBDIM);
    cb->point_to_index.reserve(num_points * 2);
    for (size_t k = 0; k < num_points; ++k)
    {
        int c2[E8_SUBDIM];
        for (size_t i = 0; i < E8_SUBDIM; ++i)
        {
            c2[i] = pts[k].second[i];
            cb->coords[k * E8_SUBDIM + i] = cb->scale * (static_cast<float>(c2[i]) * 0.5f);
        }
        cb->point_to_index[e8Key(c2)] = static_cast<UInt32>(k);
    }
    return cb;
}

/// Process-wide cache of E8 codebooks keyed by (dimensions, bits). The codebook depends only on these two values (it is
/// not data-dependent), so build and query threads share one immutable instance.
std::shared_ptr<const E8Codebook> getE8Codebook(size_t dimensions, size_t bits)
{
    static std::mutex mutex;
    static std::map<std::pair<size_t, size_t>, std::shared_ptr<const E8Codebook>> cache;
    std::lock_guard lock(mutex);
    const auto key = std::make_pair(dimensions, bits);
    auto it = cache.find(key);
    if (it != cache.end())
        return it->second;
    auto cb = buildE8Codebook(dimensions, bits);
    cache.emplace(key, cb);
    return cb;
}

/// Encode one rotated, unit-normalized vector `rhat` (`dimensions` floats) into M = dimensions/8 sub-quantizer codes
/// written to `dst` (`code_bytes` each), followed by the 4-byte original L2 norm `norm`.
inline void encodeE8(const E8Codebook & cb, const float * rhat, size_t dimensions, size_t code_bytes, float norm, char * dst)
{
    const size_t M = dimensions / E8_SUBDIM;
    const double inv_scale = (cb.scale > 0.0f) ? 1.0 / static_cast<double>(cb.scale) : 0.0;
    for (size_t m = 0; m < M; ++m)
    {
        const float * sub = rhat + m * E8_SUBDIM;
        double y[E8_SUBDIM];
        double yn2 = 0.0;
        for (size_t i = 0; i < E8_SUBDIM; ++i)
        {
            y[i] = static_cast<double>(sub[i]) * inv_scale; /// to lattice units
            yn2 += y[i] * y[i];
        }

        /// Clamp into the fully-contained region so the decode is guaranteed to hit a stored point (no fallback scan).
        if (cb.use_clamp && yn2 > static_cast<double>(cb.clamp_radius) * static_cast<double>(cb.clamp_radius))
        {
            const double f = static_cast<double>(cb.clamp_radius) / std::sqrt(yn2);
            for (double & yi : y)
                yi *= f;
        }

        int c2[E8_SUBDIM];
        nearestE8(y, c2);

        UInt32 idx = 0;
        const auto * it = cb.point_to_index.find(e8Key(c2));
        if (it != nullptr)
        {
            idx = it->getMapped();
        }
        else
        {
            /// The nearest lattice point fell outside the truncated codebook (a rare large-norm sub-vector): fall back
            /// to a brute-force nearest search over the stored points.
            float best = std::numeric_limits<float>::max();
            for (size_t k = 0; k < cb.num_points; ++k)
            {
                const float * c = cb.coords.data() + k * E8_SUBDIM;
                float d = 0.0f;
                for (size_t i = 0; i < E8_SUBDIM; ++i)
                {
                    const float e = sub[i] - c[i];
                    d += e * e;
                }
                if (d < best)
                {
                    best = d;
                    idx = static_cast<UInt32>(k);
                }
            }
        }

        if (code_bytes == 1)
            dst[m] = static_cast<char>(static_cast<UInt8>(idx));
        else
        {
            const UInt16 v = static_cast<UInt16>(idx);
            std::memcpy(dst + m * 2, &v, sizeof(UInt16));
        }
    }
    std::memcpy(dst + M * code_bytes, &norm, sizeof(float));
}

/// The query side of the E8 estimator, prepared once per query: a per-subspace ADC table of inner products between the
/// (rotated, unit-normalized) query sub-vectors and every codebook point.
struct E8Query
{
    std::vector<float> lut;     /// M * num_points inner products, subspace m at offset m * num_points
    size_t M = 0;
    size_t num_points = 0;
    size_t code_bytes = 0;
    bool is_l2 = false;
    float q_norm = 0.0f;        /// ||q|| of the original query (L2 only)
    float q_norm_sq = 0.0f;
};

E8Query buildE8Query(const E8Codebook & cb, const float * rqhat, size_t dimensions, size_t code_bytes, bool is_l2, float q_norm)
{
    E8Query q;
    q.M = dimensions / E8_SUBDIM;
    q.num_points = cb.num_points;
    q.code_bytes = code_bytes;
    q.is_l2 = is_l2;
    q.q_norm = q_norm;
    q.q_norm_sq = q_norm * q_norm;
    q.lut.resize(q.M * q.num_points);
    for (size_t m = 0; m < q.M; ++m)
    {
        const float * sub = rqhat + m * E8_SUBDIM;
        float * lut_m = q.lut.data() + m * q.num_points;
        for (size_t k = 0; k < q.num_points; ++k)
        {
            const float * c = cb.coords.data() + k * E8_SUBDIM;
            float d = 0.0f;
            for (size_t i = 0; i < E8_SUBDIM; ++i)
                d += sub[i] * c[i];
            lut_m[k] = d;
        }
    }
    return q;
}

/// ADC scan: sum the per-subspace inner products for this vector's codes, then map to the requested distance.
inline float e8Distance(const E8Query & q, const char * code)
{
    float ip = 0.0f;
    if (q.code_bytes == 1)
    {
        const UInt8 * c = reinterpret_cast<const UInt8 *>(code);
        for (size_t m = 0; m < q.M; ++m)
            ip += q.lut[m * q.num_points + c[m]];
    }
    else
    {
        for (size_t m = 0; m < q.M; ++m)
        {
            UInt16 idx = 0;
            std::memcpy(&idx, code + m * 2, sizeof(UInt16));
            ip += q.lut[m * q.num_points + idx];
        }
    }

    float norm_x = 0.0f;
    std::memcpy(&norm_x, code + q.M * q.code_bytes, sizeof(float));

    if (q.is_l2)
        return q.q_norm_sq + norm_x * norm_x - 2.0f * q.q_norm * norm_x * ip;
    return 1.0f - ip;
}

}

MergeTreeIndexAggregatorVectorSimilarityFlat::MergeTreeIndexAggregatorVectorSimilarityFlat(
    const String & index_name_,
    const Block & index_sample_block_,
    UInt64 dimensions_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    bool projected_,
    bool turboquant_,
    bool rabitq_,
    bool e8_,
    size_t e8_bits_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , dimensions(dimensions_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , projected(projected_)
    , turboquant(turboquant_)
    , rabitq(rabitq_)
    , e8(e8_)
    , e8_bits(e8_bits_)
    /// e8: M = dim/8 sub-quantizer codes (1 byte for bits <= 8, else 2) + a 4-byte norm; turboquant: 2 bits/coord
    /// (1-bit MSE + 1-bit QJL) + a 4-byte residual norm; rabitq: 1 bit/coord + a 4-byte factor; else 1 bit/coord
    , bytes_per_vector(
          e8_ ? (dimensions_ / E8_SUBDIM) * (e8_bits_ <= 8 ? 1 : 2) + sizeof(float)
              : (turboquant_ ? dimensions_ / 4 + sizeof(float) : (rabitq_ ? dimensions_ / 8 + sizeof(float) : dimensions_ / 8)))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSimilarityFlat::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarityFlat>(index_name, metric_kind, scalar_kind, dimensions);
    granule->bytes_per_vector = bytes_per_vector;
    granule->num_vectors = num_vectors;
    granule->codes = std::move(codes);
    codes.clear();
    num_vectors = 0;
    return granule;
}

namespace
{

/// Sign-binarize `rows` vectors from `column_array` into packed bit-codes appended to `codes`.
/// If `projection` is non-empty ('b1_projected'), each vector is randomly projected before sign-binarizing.
/// The projected path is O(d log d) per vector (Walsh-Hadamard) and is parallelized over the build thread pool.
template <typename Column>
void quantizeRowsToBinary(
    const ColumnArray * column_array, const ColumnArray::Offsets & offsets,
    size_t dimensions, size_t bytes_per_vector, std::vector<UInt8> & codes, size_t & num_vectors, size_t rows,
    const std::vector<float> & projection, const std::vector<float> & projection_qjl, bool turboquant, bool rabitq,
    bool e8, size_t e8_code_bytes, const E8Codebook * e8_codebook)
{
    const auto & data = typeid_cast<const Column &>(column_array->getData()).getData();

    for (size_t row = 0; row + 1 < rows; ++row)
        if (offsets[row + 1] - offsets[row] != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column with vector similarity index must have equal length");

    const size_t base = num_vectors;
    codes.resize((base + rows) * bytes_per_vector);
    num_vectors = base + rows;

    /// `projection` is non-empty for all projection-based methods; `projected` means specifically 'b1_projected'.
    const bool projected = !projection.empty() && !turboquant && !rabitq && !e8;
    const size_t padded = projection.empty() ? 0 : projection.size() / PROJECTION_ROUNDS;

    /// Encode one row into its (disjoint) packed code slot. `work`/`work2` are per-thread scratch buffers of `padded`
    /// floats; `work2` is used only by the 'turboquant' QJL stage, the others use just `work`.
    auto encode_row = [&](size_t row, float * work, float * work2)
    {
        const size_t start = (row == 0) ? 0 : offsets[row - 1];
        const typename Column::ValueType * src = &data[start];
        checkVectorIsSane(src, dimensions, unum::usearch::scalar_kind_t::b1x8_k, ErrorCodes::INCORRECT_DATA, "indexed vector");

        char * dst = reinterpret_cast<char *>(codes.data() + (base + row) * bytes_per_vector);
        if (turboquant)
        {
            /// 'turboquant': 1-bit MSE quantizer + 1-bit QJL on the residual + per-vector residual norm.
            encodeTurboQuant(projection, projection_qjl, src, dimensions, work, work2, dst);
        }
        else if (rabitq)
        {
            /// 'rabitq': project, sign-binarize, and append the per-vector correction factor.
            encodeRaBitQ(projection, src, dimensions, work, dst);
        }
        else if (e8)
        {
            /// 'e8': rotate, unit-normalize the rotated direction, then quantize each 8-dim sub-vector to the nearest
            /// E8 lattice point. The original L2 norm is appended (used by the L2Distance scan).
            double norm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                norm_sq += static_cast<double>(src[i]) * static_cast<double>(src[i]);
            const float norm = static_cast<float>(std::sqrt(norm_sq));

            applyRandomProjection(projection, src, dimensions, work);
            double rnorm_sq = 0.0;
            for (size_t i = 0; i < dimensions; ++i)
                rnorm_sq += static_cast<double>(work[i]) * static_cast<double>(work[i]);
            const float inv_rnorm = (rnorm_sq > 0.0) ? static_cast<float>(1.0 / std::sqrt(rnorm_sq)) : 0.0f;
            for (size_t i = 0; i < dimensions; ++i)
                work[i] *= inv_rnorm;

            encodeE8(*e8_codebook, work, dimensions, e8_code_bytes, norm, dst);
        }
        else if (projected)
        {
            /// 'b1_projected': project, then sign-binarize the projected (Float32) vector.
            applyRandomProjection(projection, src, dimensions, work);
            unum::usearch::cast_gt<unum::usearch::f32_t, unum::usearch::b1x8_t>::try_(reinterpret_cast<const char *>(work), dimensions, dst);
        }
        else if constexpr (std::is_same_v<Column, ColumnBFloat16>)
            unum::usearch::cast_gt<unum::usearch::bf16_bits_t, unum::usearch::b1x8_t>::try_(reinterpret_cast<const char *>(src), dimensions, dst);
        else if constexpr (std::is_same_v<Column, ColumnFloat32>)
            unum::usearch::cast_gt<unum::usearch::f32_t, unum::usearch::b1x8_t>::try_(reinterpret_cast<const char *>(src), dimensions, dst);
        else
        {
            static_assert(std::is_same_v<Column, ColumnFloat64>);
            unum::usearch::cast_gt<unum::usearch::f64_t, unum::usearch::b1x8_t>::try_(reinterpret_cast<const char *>(src), dimensions, dst);
        }
    };

    auto throw_if_killed = []
    {
        if (auto query_context = CurrentThread::tryGetQueryContext())
            if (auto query_status = query_context->getProcessListElementSafe())
                query_status->throwIfKilled();
    };

    size_t threads_count = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
    if (threads_count == 0)
        threads_count = getNumberOfCPUCoresToUse();

    /// Only the projection-based paths are expensive enough to be worth parallelizing; raw-sign is cheap.
    static constexpr size_t min_rows_for_parallel = 256;
    const size_t num_threads = ((projected || turboquant || rabitq || e8) && rows >= min_rows_for_parallel) ? std::min(threads_count, rows) : 1;

    if (num_threads <= 1)
    {
        std::vector<float> work(2 * padded); /// two scratch buffers (turboquant QJL needs both)
        throw_if_killed();
        for (size_t row = 0; row < rows; ++row)
            encode_row(row, work.data(), work.data() + padded);
    }
    else
    {
        const size_t per_thread = (rows + num_threads - 1) / num_threads;
        auto & pool = Context::getGlobalContextInstance()->getBuildVectorSimilarityIndexThreadPool();
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::MERGETREE_VECTOR_SIM_INDEX);
        for (size_t t = 0; t < num_threads; ++t)
        {
            const size_t begin = t * per_thread;
            if (begin >= rows)
                break;
            const size_t end = std::min(rows, begin + per_thread);
            runner.enqueueAndKeepTrack([&encode_row, &throw_if_killed, padded, begin, end]
            {
                throw_if_killed();
                std::vector<float> work(2 * padded);
                for (size_t row = begin; row < end; ++row)
                    encode_row(row, work.data(), work.data() + padded);
            });
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }
}

}

void MergeTreeIndexAggregatorVectorSimilarityFlat::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    if (index_sample_block.columns() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected that index is build over a single column");

    const auto & index_column_name = index_sample_block.getByPosition(0).name;
    const auto & index_column = block.getByName(index_column_name).column;
    ColumnPtr column_cut = index_column->cut(*pos, rows_read);

    const auto * column_array = typeid_cast<const ColumnArray *>(column_cut.get());
    if (!column_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array(Float32|Float64|BFloat16) column");
    if (column_array->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Array is unexpectedly empty");

    const size_t rows = column_array->size();
    const auto & column_array_offsets = column_array->getOffsets();
    const size_t dimensions_inserted = column_array_offsets[0];
    if (dimensions != dimensions_inserted)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Array values in column with vector similarity index have {} elements, expects {} elements", dimensions_inserted, dimensions);

    const auto * data_type_array = typeid_cast<const DataTypeArray *>(block.getByName(index_column_name).type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float32|Float64|BFloat16)");

    /// For 'b1_projected', 'turboquant', 'rabitq', and 'e8', build the (deterministic) random projection once and reuse it.
    /// 'turboquant' additionally needs the second, independent QJL projection.
    if ((projected || turboquant || rabitq || e8) && projection.empty())
        projection = generateRandomProjection(dimensions);
    if (turboquant && projection_qjl.empty())
        projection_qjl = generateRandomProjection(dimensions, RANDOM_PROJECTION_SEED_QJL);

    /// 'e8': resolve the (deterministic, cached) E8 codebook for this (dimensions, bits). `code_bytes` packs the
    /// sub-quantizer index into 1 byte (bits <= 8) or 2 bytes (bits <= 16).
    std::shared_ptr<const E8Codebook> e8_codebook;
    const size_t e8_code_bytes = (e8_bits <= 8) ? 1 : 2;
    if (e8)
        e8_codebook = getE8Codebook(dimensions, e8_bits);
    const E8Codebook * e8_codebook_ptr = e8_codebook.get();

    const TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
    WhichDataType which(nested_type_index);
    if (which.isFloat32())
        quantizeRowsToBinary<ColumnFloat32>(column_array, column_array_offsets, dimensions, bytes_per_vector, codes, num_vectors, rows, projection, projection_qjl, turboquant, rabitq, e8, e8_code_bytes, e8_codebook_ptr);
    else if (which.isFloat64())
        quantizeRowsToBinary<ColumnFloat64>(column_array, column_array_offsets, dimensions, bytes_per_vector, codes, num_vectors, rows, projection, projection_qjl, turboquant, rabitq, e8, e8_code_bytes, e8_codebook_ptr);
    else if (which.isBFloat16())
        quantizeRowsToBinary<ColumnBFloat16>(column_array, column_array_offsets, dimensions, bytes_per_vector, codes, num_vectors, rows, projection, projection_qjl, turboquant, rabitq, e8, e8_code_bytes, e8_codebook_ptr);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");

    *pos += rows_read;
}

MergeTreeIndexConditionVectorSimilarityFlat::MergeTreeIndexConditionVectorSimilarityFlat(
    const std::optional<VectorSearchParameters> & parameters_,
    const String & index_column_,
    unum::usearch::metric_kind_t metric_kind_,
    bool projected_,
    bool turboquant_,
    bool rabitq_,
    bool e8_,
    size_t e8_bits_,
    ContextPtr context)
    : parameters(parameters_)
    , index_column(index_column_)
    , metric_kind(metric_kind_)
    , projected(projected_)
    , turboquant(turboquant_)
    , rabitq(rabitq_)
    , e8(e8_)
    , e8_bits(e8_bits_)
    , index_fetch_multiplier(context->getSettingsRef()[Setting::vector_search_index_fetch_multiplier])
    , max_limit(context->getSettingsRef()[Setting::max_limit_for_vector_search_queries])
    , is_rescoring(context->getSettingsRef()[Setting::vector_search_with_rescoring])
{
    static constexpr auto MAX_INDEX_FETCH_MULTIPLIER = 1000.0;
    if (!std::isfinite(index_fetch_multiplier)
        || index_fetch_multiplier <= 0.0f || static_cast<double>(index_fetch_multiplier) > MAX_INDEX_FETCH_MULTIPLIER
        || (parameters && !std::isfinite(static_cast<double>(index_fetch_multiplier) * static_cast<double>(parameters->limit))))
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'vector_search_index_fetch_multiplier' must be greater than 0.0 and less than {}", MAX_INDEX_FETCH_MULTIPLIER);
}

bool MergeTreeIndexConditionVectorSimilarityFlat::mayBeTrueOnGranule(MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for vector similarity indexes");
}

bool MergeTreeIndexConditionVectorSimilarityFlat::alwaysUnknownOrTrue() const
{
    if (!parameters)
        return true;
    if (parameters->column != index_column)
        return true;
    if ((parameters->distance_function == "L2Distance" && metric_kind != unum::usearch::metric_kind_t::l2sq_k)
        || (parameters->distance_function == "cosineDistance" && metric_kind != unum::usearch::metric_kind_t::cos_k && metric_kind != unum::usearch::metric_kind_t::hamming_k)
        || (parameters->distance_function == "dotProduct" && metric_kind != unum::usearch::metric_kind_t::ip_k))
            return true;
    return false;
}

NearestNeighbours MergeTreeIndexConditionVectorSimilarityFlat::calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule_) const
{
    if (!parameters)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected vector_search_parameters to be set");

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleVectorSimilarityFlat>(granule_);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    if (parameters->reference_vector.size() != granule->dimensions)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
            parameters->reference_vector.size(), granule->dimensions);

    checkVectorIsSane(parameters->reference_vector.data(), granule->dimensions, granule->scalar_kind, ErrorCodes::INCORRECT_QUERY, "reference vector in the SELECT query");

    size_t limit = parameters->limit;
    if (parameters->additional_filters_present || is_rescoring)
        limit = std::min(static_cast<size_t>(static_cast<double>(limit) * static_cast<double>(index_fetch_multiplier)), max_limit);

    const size_t num_vectors = granule->num_vectors;
    const size_t bytes_per_vector = granule->bytes_per_vector;
    limit = std::min(limit, num_vectors);

    /// Quantize the (full-precision) reference vector to the same packed representation as the index.
    /// For 'b1_projected'/'turboquant', project the query with the same fixed-seed transform used at build time.
    std::vector<UInt8> query_code(bytes_per_vector);
    TurboQuantQuery turboquant_query; /// turboquant only: the two bit-sliced query projections for the popcount scan
    RaBitQQuery rabitq_query;         /// rabitq only: the bit-sliced query for the popcount scan
    E8Query e8_query;                 /// e8 only: the per-subspace ADC lookup table
    if (e8)
    {
        /// Rotate the query with the same fixed-seed transform, unit-normalize the rotated direction, and build the ADC
        /// inner-product table against the (deterministic) E8 codebook. For L2Distance the query's L2 norm is kept.
        const std::vector<float> projection = generateRandomProjection(granule->dimensions);
        const size_t padded = projection.size() / PROJECTION_ROUNDS;
        std::vector<float> query_rotated(padded);
        applyRandomProjection(projection, parameters->reference_vector.data(), granule->dimensions, query_rotated.data());

        double q_norm_sq = 0.0;
        for (size_t i = 0; i < granule->dimensions; ++i)
            q_norm_sq += static_cast<double>(parameters->reference_vector[i]) * static_cast<double>(parameters->reference_vector[i]);
        const float q_norm = static_cast<float>(std::sqrt(q_norm_sq));

        double rnorm_sq = 0.0;
        for (size_t i = 0; i < granule->dimensions; ++i)
            rnorm_sq += static_cast<double>(query_rotated[i]) * static_cast<double>(query_rotated[i]);
        const float inv_rnorm = (rnorm_sq > 0.0) ? static_cast<float>(1.0 / std::sqrt(rnorm_sq)) : 0.0f;
        for (size_t i = 0; i < granule->dimensions; ++i)
            query_rotated[i] *= inv_rnorm;

        const size_t e8_code_bytes = (e8_bits <= 8) ? 1 : 2;
        auto codebook = getE8Codebook(granule->dimensions, e8_bits);
        const bool is_l2 = (granule->metric_kind == unum::usearch::metric_kind_t::l2sq_k);
        e8_query = buildE8Query(*codebook, query_rotated.data(), granule->dimensions, e8_code_bytes, is_l2, q_norm);
    }
    else if (turboquant)
    {
        const std::vector<float> p1 = generateRandomProjection(granule->dimensions);
        const std::vector<float> p2 = generateRandomProjection(granule->dimensions, RANDOM_PROJECTION_SEED_QJL);
        turboquant_query = buildTurboQuantQuery(p1, p2, parameters->reference_vector.data(), granule->dimensions);
    }
    else if (rabitq)
    {
        /// Project the query with the same fixed-seed transform, then bit-slice it to RABITQ_QUERY_BITS bits.
        const std::vector<float> projection = generateRandomProjection(granule->dimensions);
        const size_t padded = projection.size() / PROJECTION_ROUNDS;
        std::vector<float> query_projected(padded);
        applyRandomProjection(projection, parameters->reference_vector.data(), granule->dimensions, query_projected.data());
        rabitq_query = buildRaBitQQuery(query_projected.data(), granule->dimensions);
    }
    else if (projected)
    {
        const std::vector<float> projection = generateRandomProjection(granule->dimensions);
        const size_t padded = projection.size() / PROJECTION_ROUNDS;
        std::vector<float> projected_query(padded);
        applyRandomProjection(projection, parameters->reference_vector.data(), granule->dimensions, projected_query.data());
        unum::usearch::cast_gt<unum::usearch::f32_t, unum::usearch::b1x8_t>::try_(
            reinterpret_cast<const char *>(projected_query.data()), granule->dimensions, reinterpret_cast<char *>(query_code.data()));
    }
    else
    {
        unum::usearch::cast_gt<unum::usearch::f64_t, unum::usearch::b1x8_t>::try_(
            reinterpret_cast<const char *>(parameters->reference_vector.data()), granule->dimensions, reinterpret_cast<char *>(query_code.data()));
    }

    if (limit == 0 || num_vectors == 0)
        return {};

    /// usearch's punned metric computes Hamming over b1x8 codes using AVX popcount (not used for turboquant,
    /// which uses its own dequantized squared-L2).
    unum::usearch::metric_punned_t metric;
    if (!turboquant && !rabitq && !e8)
        metric = unum::usearch::metric_punned_t(granule->dimensions, granule->metric_kind, granule->scalar_kind);

    const char * base = reinterpret_cast<const char *>(granule->codes.data());
    const char * query = reinterpret_cast<const char *>(query_code.data());

    /// Decide once whether the turboquant/rabitq scans can use their AVX-512 kernels (both bit-sliced popcount scans
    /// need VPOPCNTDQ -> Ice Lake).
    bool use_icelake_turboquant = false;
    bool use_icelake_rabitq = false;
#if USE_MULTITARGET_CODE
    use_icelake_turboquant = turboquant && isArchSupported(TargetArch::x86_64_icelake);
    use_icelake_rabitq = rabitq && isArchSupported(TargetArch::x86_64_icelake);
#endif

    /// Exhaustive scan. Unlike HNSW search (O(log N) per query, fine single-threaded), a flat scan is O(N) and the
    /// index-analysis phase only parallelizes across parts, so a single large part would otherwise scan single-threaded.
    /// We compute distances in parallel into disjoint slices of `scored` and then partial-sort to the top `limit`.
    /// Note: we deliberately do NOT use usearch's `exact_search_t` here - it increments a single shared atomic counter
    /// once per vector, and for the single-query case that contention dominates (~99% of runtime) over the tiny Hamming.
    std::vector<std::pair<float, UInt32>> scored(num_vectors);

    auto throw_if_killed = []
    {
        if (auto query_context = CurrentThread::tryGetQueryContext())
            if (auto query_status = query_context->getProcessListElementSafe())
                query_status->throwIfKilled();
    };

    auto scan_range = [&](size_t begin, size_t end)
    {
        for (size_t i = begin; i < end; ++i)
        {
            /// Check cancellation periodically (not per vector - that lookup would itself dominate the cheap Hamming).
            if ((i & 0xFFFFu) == 0)
                throw_if_killed();
            float dist = NAN;
            if (e8)
                dist = e8Distance(e8_query, reinterpret_cast<const char *>(base + i * bytes_per_vector));
            else if (turboquant)
                dist = turboQuantDistanceFast(turboquant_query, base + i * bytes_per_vector, use_icelake_turboquant);
            else if (rabitq)
                dist = raBitQDistanceFast(rabitq_query, base + i * bytes_per_vector, use_icelake_rabitq);
            else
                dist = static_cast<float>(metric(base + i * bytes_per_vector, query));
            scored[i] = {dist, static_cast<UInt32>(i)};
        }
    };

    /// Hops on the vector index build threadpool
    size_t threads_count = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
    if (threads_count == 0)
        threads_count = getNumberOfCPUCoresToUse();
    const size_t num_threads = std::min(threads_count, num_vectors);

    if (num_threads <= 1)
    {
        scan_range(0, num_vectors);
    }
    else
    {
        const size_t per_thread = (num_vectors + num_threads - 1) / num_threads;
        auto & pool = Context::getGlobalContextInstance()->getBuildVectorSimilarityIndexThreadPool();
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::MERGETREE_VECTOR_SIM_INDEX);
        for (size_t t = 0; t < num_threads; ++t)
        {
            const size_t begin = t * per_thread;
            if (begin >= num_vectors)
                break;
            const size_t end = std::min(num_vectors, begin + per_thread);
            runner.enqueueAndKeepTrack([&scan_range, begin, end] { scan_range(begin, end); });
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    if (limit < num_vectors)
        std::partial_sort(scored.begin(), scored.begin() + limit, scored.end());
    else
        std::sort(scored.begin(), scored.end());

    NearestNeighbours result;
    result.rows.resize(limit);
    if (parameters->return_distances)
        result.distances = std::vector<float>(limit);
    for (size_t i = 0; i < limit; ++i)
    {
        result.rows[i] = scored[i].second;
        if (parameters->return_distances)
            result.distances.value()[i] = scored[i].first;
    }

    return result;
}

/// ============================ End flat ("fastknn") implementation ============================

MergeTreeIndexVectorSimilarity::MergeTreeIndexVectorSimilarity(
    const IndexDescription & index_,
    const String & method_,
    UInt64 dimensions_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    bool projected_,
    bool turboquant_,
    bool rabitq_,
    bool e8_,
    size_t e8_bits_,
    UsearchHnswParams usearch_hnsw_params_)
    : IMergeTreeIndex(index_)
    , method(method_)
    , dimensions(dimensions_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , projected(projected_)
    , turboquant(turboquant_)
    , rabitq(rabitq_)
    , e8(e8_)
    , e8_bits(e8_bits_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSimilarity::createIndexGranule() const
{
    if (method == "fastknn")
        return std::make_shared<MergeTreeIndexGranuleVectorSimilarityFlat>(index.name, metric_kind, scalar_kind, dimensions);
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index.name, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarity::createIndexAggregator() const
{
    if (method == "fastknn")
        return std::make_shared<MergeTreeIndexAggregatorVectorSimilarityFlat>(index.name, index.sample_block, dimensions, metric_kind, scalar_kind, projected, turboquant, rabitq, e8, e8_bits);
    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarity>(index.name, index.sample_block, dimensions, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG::Node * /*predicate*/, ContextPtr /*context*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function not supported for vector similarity index");
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG::Node * /*predicate*/, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const
{
    const String & index_column = index.column_names[0];
    if (method == "fastknn")
        return std::make_shared<MergeTreeIndexConditionVectorSimilarityFlat>(parameters, index_column, metric_kind, projected, turboquant, rabitq, e8, e8_bits, context);
    return std::make_shared<MergeTreeIndexConditionVectorSimilarity>(parameters, index_column, metric_kind, context);
}

MergeTreeIndexPtr vectorSimilarityIndexCreator(const IndexDescription & index)
{
    FieldVector args = getFieldsFromIndexArgumentsAST(index.arguments);
    const String method = args[0].safeGet<String>();
    UInt64 dimensions = args[2].safeGet<UInt64>();

    /// Default parameters:
    unum::usearch::metric_kind_t metric_kind = distanceFunctionToMetricKind.at(args[1].safeGet<String>());
    unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::bf16_k;
    UsearchHnswParams usearch_hnsw_params;

    bool projected = false;
    bool turboquant = false;
    bool rabitq = false;
    bool e8 = false;
    size_t e8_bits = 8;

    /// Optional parameters:
    const bool has_six_args = (args.size() == 6);
    if (has_six_args)
    {
        const String quantization = args[3].safeGet<String>();
        usearch_hnsw_params = {.connectivity  = args[4].safeGet<UInt64>(),
                               .expansion_add = args[5].safeGet<UInt64>()};

        /// 'turboquant'/'rabitq'/'e8' (fastknn only) are not usearch scalar_kinds: they store their own codes and use
        /// their own scan kernels. Keep the metric as the requested distance (cosineDistance -> cos_k, L2Distance -> l2sq_k).
        if (quantization == "turboquant")
        {
            turboquant = true;
            scalar_kind = unum::usearch::scalar_kind_t::f32_k; /// unused by the turboquant scan
        }
        else if (quantization == "rabitq")
        {
            rabitq = true;
            scalar_kind = unum::usearch::scalar_kind_t::f32_k; /// unused by the rabitq scan
        }
        else if (quantization == "e8")
        {
            /// 'e8' (fastknn only): the fifth argument carries the number of bits per 8-dim sub-quantizer.
            e8 = true;
            e8_bits = args[4].safeGet<UInt64>();
            scalar_kind = unum::usearch::scalar_kind_t::f32_k; /// unused by the e8 scan
        }
        else
        {
            scalar_kind = quantizationToScalarKind.at(quantization);

            /// Special handling for binary quantization:
            if (scalar_kind == unum::usearch::scalar_kind_t::b1x8_k)
                metric_kind = unum::usearch::metric_kind_t::hamming_k;

            /// 'b1_projected' shares 'b1' storage and the Hamming metric, but projects vectors before signing.
            projected = (quantization == "b1_projected");
        }
    }

    return std::make_shared<MergeTreeIndexVectorSimilarity>(index, method, dimensions, metric_kind, scalar_kind, projected, turboquant, rabitq, e8, e8_bits, usearch_hnsw_params);
}

void vectorSimilarityIndexValidator(const IndexDescription & index, bool /* attach */)
{
    FieldVector args = getFieldsFromIndexArgumentsAST(index.arguments);
    const bool has_three_args = (args.size() == 3);
    const bool has_six_args = (args.size() == 6);

    /// Check number and type of arguments
    if (!has_three_args && !has_six_args)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index must have three or six arguments");
    if (args[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "First argument of vector similarity index (method) must be of type String");
    if (args[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (args[2].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (dimensions) must be of type UInt64");
    if (has_six_args)
    {
        if (args[3].getType() != Field::Types::String)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fourth argument of vector similarity index (quantization) must be of type String");
        if (args[4].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fifth argument of vector similarity index (hnsw_max_connections_per_layer) must be of type UInt64");
        if (args[5].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Sixth argument of vector similarity index (hnsw_candidate_list_size_for_construction) must be of type UInt64");
    }

    /// Check that passed arguments are supported
    if (!methods.contains(args[0].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "First argument (method) of vector similarity index is not supported. Supported methods are: {}", joinByComma(methods));
    if (!distanceFunctionToMetricKind.contains(args[1].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Second argument (distance function) of vector similarity index is not supported. Supported distance function are: {}", joinByComma(distanceFunctionToMetricKind));
    if (args[2].safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (dimensions) of vector similarity index must be > 0");

    /// The flat "fastknn" method needs the quantization argument and currently only supports binary ('b1') codes.
    const bool is_fastknn = (args[0].safeGet<String>() == "fastknn");
    if (is_fastknn && !has_six_args)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The 'fastknn' method requires the six-argument form so a quantization can be specified (currently only 'b1' is supported)");

    if (has_six_args)
    {
        const String quantization = args[3].safeGet<String>();
        const bool is_turboquant = (quantization == "turboquant");
        const bool is_rabitq = (quantization == "rabitq");
        const bool is_e8 = (quantization == "e8");

        if (!is_turboquant && !is_rabitq && !is_e8 && !quantizationToScalarKind.contains(quantization))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Fourth argument (quantization) of vector similarity index is not supported. Supported quantizations are: {} (and 'turboquant', 'rabitq', 'e8' for the 'fastknn' method)", joinByComma(quantizationToScalarKind));

        if (is_e8)
        {
            /// 'e8': fastknn-only lattice product quantization. Supports both cosineDistance and L2Distance; dimension
            /// must be a multiple of 8 (one E8 sub-quantizer per 8 coordinates). The fifth argument is the number of
            /// bits per sub-quantizer (1..16); it sets the codebook size (2^bits points) and hence the storage.
            if (!is_fastknn)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'e8' quantization is only supported by the 'fastknn' method");
            const auto e8_metric = distanceFunctionToMetricKind.at(args[1].safeGet<String>());
            if (e8_metric != unum::usearch::metric_kind_t::cos_k && e8_metric != unum::usearch::metric_kind_t::l2sq_k)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'e8' quantization can only be used with cosineDistance or L2Distance as distance function");
            if (args[2].safeGet<UInt64>() % 8 != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'e8' quantization requires that the dimension is a multiple of 8");
            const UInt64 e8_bits = args[4].safeGet<UInt64>();
            if (e8_bits < 1 || e8_bits > 16)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'e8' quantization requires that the fifth argument (bits per sub-quantizer) is between 1 and 16");
        }
        else if (is_turboquant)
        {
            /// 'turboquant': fastknn-only, cosine distance, dimension multiple of 8 (two 1-bit sign planes -> dim/8 bytes each).
            if (!is_fastknn)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'turboquant' quantization is only supported by the 'fastknn' method");
            if (distanceFunctionToMetricKind.at(args[1].safeGet<String>()) != unum::usearch::metric_kind_t::cos_k)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'turboquant' quantization can only be used with the cosine distance as distance function");
            if (args[2].safeGet<UInt64>() % 8 != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'turboquant' quantization requires that the dimension is a multiple of 8");
        }
        else if (is_rabitq)
        {
            /// 'rabitq': fastknn-only, cosine distance, dimension multiple of 8 (1 bit/coord -> 8 codes/byte).
            if (!is_fastknn)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'rabitq' quantization is only supported by the 'fastknn' method");
            if (distanceFunctionToMetricKind.at(args[1].safeGet<String>()) != unum::usearch::metric_kind_t::cos_k)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'rabitq' quantization can only be used with the cosine distance as distance function");
            if (args[2].safeGet<UInt64>() % 8 != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "'rabitq' quantization requires that the dimension is a multiple of 8");
        }
        else
        {
            if (is_fastknn && quantizationToScalarKind.at(quantization) != unum::usearch::scalar_kind_t::b1x8_k)
                throw Exception(ErrorCodes::INCORRECT_DATA, "The 'fastknn' method currently supports only 'b1', 'b1_projected', 'turboquant', and 'rabitq' quantization");

            /// More checks for binary quantization
            if (quantizationToScalarKind.at(quantization) == unum::usearch::scalar_kind_t::b1x8_k)
            {
                if (distanceFunctionToMetricKind.at(args[1].safeGet<String>()) != unum::usearch::metric_kind_t::cos_k)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Binary quantization in vector similarity index can only be used with the cosine distance as distance function");
                if (args[2].safeGet<UInt64>() % 8 != 0)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Binary quantization in vector similarity index requires that the dimension is a multiple of 8");
            }
        }

        /// Call Usearch's own parameter validation method for HNSW-specific parameters (the flat method ignores them).
        if (!is_fastknn)
        {
            UInt64 connectivity = args[4].safeGet<UInt64>();
            UInt64 expansion_add = args[5].safeGet<UInt64>();
            UInt64 expansion_search = default_expansion_search;
            unum::usearch::index_dense_config_t config(connectivity, expansion_add, expansion_search);
            if (auto error = config.validate(); error)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parameters passed to vector similarity index. Error: {}", error.release());
        }
    }

    /// Check that the index is created on a single column
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Vector similarity index must be created on a single column");

    /// Check that the data type is Array(Float32|Float64|BFloat16)
    DataTypePtr data_type = index.sample_block.getDataTypes()[0];
    const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector similarity index can only be created on columns of type Array(Float32|Float64|BFloat16)");
    TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
    WhichDataType which(nested_type_index);
    if (!which.isNativeFloat() && !which.isBFloat16())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector similarity index can only be created on columns of type Array(Float32|Float64|BFloat16)");
}

}

#endif
