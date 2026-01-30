#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>

#if USE_USEARCH

#include <usearch/index_plugins.hpp>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/BitHelpers.h>
#include <Common/setThreadName.h>
#include <Common/formatReadable.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/CurrentThread.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <limits>
#include <ranges>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>

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

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_experimental_leann_optimization_for_hnsw;
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

/// The only indexing method currently supported by USearch
const std::set<String> methods = {"hnsw"};

/// Maps from user-facing name to internal name
const std::unordered_map<String, unum::usearch::metric_kind_t> distanceFunctionToMetricKind = {
    {"L2Distance", unum::usearch::metric_kind_t::l2sq_k},
    {"cosineDistance", unum::usearch::metric_kind_t::cos_k}};

/// Maps from user-facing name to internal name
const std::unordered_map<String, unum::usearch::scalar_kind_t> quantizationToScalarKind = {
    {"f64", unum::usearch::scalar_kind_t::f64_k},
    {"f32", unum::usearch::scalar_kind_t::f32_k},
    {"f16", unum::usearch::scalar_kind_t::f16_k},
    {"bf16", unum::usearch::scalar_kind_t::bf16_k},
    {"i8", unum::usearch::scalar_kind_t::i8_k},
    {"b1", unum::usearch::scalar_kind_t::b1x8_k}};
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

template <typename ScheduleFunc>
void runBuildIndexTasks(ScheduleFunc && schedule_tasks)
{
    auto & thread_pool = Context::getGlobalContextInstance()->getBuildVectorSimilarityIndexThreadPool();
    ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::MERGETREE_VECTOR_SIM_INDEX);

    schedule_tasks(runner);

    runner.waitForAllToFinishAndRethrowFirstError();
}

inline void reserveIndexForSize(USearchIndexWithSerializationPtr & index, size_t target_size)
{
    size_t max_thread_pool_size = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
    if (max_thread_pool_size == 0)
        max_thread_pool_size = getNumberOfCPUCoresToUse();
    unum::usearch::index_limits_t limits(roundUpToPowerOfTwoOrZero(target_size), max_thread_pool_size);
    index->reserve(limits);
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
    UsearchHnswParams usearch_hnsw_params_,
    LeaNNParams leann_params_)
    : MergeTreeIndexGranuleVectorSimilarity(index_name_, metric_kind_, scalar_kind_, usearch_hnsw_params_, nullptr, leann_params_)
{
}

MergeTreeIndexGranuleVectorSimilarity::MergeTreeIndexGranuleVectorSimilarity(
    const String & index_name_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_,
    USearchIndexWithSerializationPtr index_,
    LeaNNParams leann_params_)
    : index_name(index_name_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
    , leann_params(leann_params_)
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

    UInt64 file_version;
    readIntBinary(file_version, istr);
    if (file_version != FILE_FORMAT_VERSION)
        throw Exception(
            ErrorCodes::FORMAT_VERSION_TOO_OLD,
            "Vector similarity index could not be loaded because its version is too old (current version: {}, persisted version: {}). Please drop the index and create it again.",
            FILE_FORMAT_VERSION, file_version);
        /// More fancy error handling would be: Set a flag on the index that it failed to load. During usage return all granules, i.e.
        /// behave as if the index does not exist. Since format changes are expected to happen only rarely and it is "only" an index, keep it simple for now.

    UInt64 dimensions;
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
    UsearchHnswParams usearch_hnsw_params_,
    LeaNNParams leann_params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , dimensions(dimensions_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
    , leann_params(leann_params_)
{
}

namespace
{

/// Perform hub node pruning on the HNSW graph (LeaNN, Section 5 of https://arxiv.org/pdf/2506.08276).
void performHubPruning(
    USearchIndexWithSerializationPtr & index,
    const std::vector<std::vector<Float32>> & stored_vectors,
    size_t dimensions,
    unum::usearch::metric_kind_t metric_kind,
    unum::usearch::scalar_kind_t scalar_kind,
    UsearchHnswParams usearch_hnsw_params,
    double hub_pruning_ratio,
    size_t sample_size)
{
    if (!index || index->size() == 0 || stored_vectors.empty() || stored_vectors.size() != index->size())
        return;

    LoggerPtr logger = getLogger("VectorSimilarityIndex");
    LOG_TRACE(logger, "Starting hub node pruning. Index size before pruning: {}", index->size());

    if (stored_vectors.empty() || stored_vectors[0].empty())
        return;

    std::vector<Float32> min_values(dimensions, std::numeric_limits<Float32>::max());
    std::vector<Float32> max_values(dimensions, std::numeric_limits<Float32>::lowest());

    for (const auto & vec : stored_vectors)
    {
        if (vec.size() != dimensions)
            continue;
        for (size_t dim = 0; dim < dimensions; ++dim)
        {
            min_values[dim] = std::min(min_values[dim], vec[dim]);
            max_values[dim] = std::max(max_values[dim], vec[dim]);
        }
    }

    const size_t num_samples = std::min(sample_size, index->size());
    std::unordered_map<USearchIndex::vector_key_t, size_t> node_visit_counts;

    pcg64_fast rng{randomSeed()};
    std::vector<Float32> random_query_vector(dimensions);
    const size_t search_limit = std::min(static_cast<size_t>(50), index->size() / 2);

    for (size_t i = 0; i < num_samples; ++i)
    {
        for (size_t dim = 0; dim < dimensions; ++dim)
        {
            std::uniform_real_distribution<Float32> dim_dist(min_values[dim], max_values[dim]);
            random_query_vector[dim] = dim_dist(rng);
        }
        auto search_result = index->search(random_query_vector.data(), search_limit, USearchIndex::any_thread(), false, default_expansion_search);
        if (search_result)
        {
            std::vector<USearchIndex::vector_key_t> result_keys(search_result.size());
            search_result.dump_to(result_keys.data());

            for (auto key : result_keys)
            {
                node_visit_counts[key]++;
            }
        }
    }

    std::vector<std::pair<USearchIndex::vector_key_t, size_t>> node_scores;
    for (const auto & [key, count] : node_visit_counts)
    {
        node_scores.emplace_back(key, count);
    }

    /// Sort by visit count (descending)
    std::sort(node_scores.begin(), node_scores.end(),
              [](const auto & a, const auto & b) { return a.second > b.second; });

    const size_t target_hub_nodes = std::max(static_cast<size_t>(1), static_cast<size_t>(index->size() * hub_pruning_ratio));
    const size_t num_hub_nodes = std::min(target_hub_nodes, node_scores.size());
    std::unordered_set<USearchIndex::vector_key_t> hub_node_keys;
    for (size_t i = 0; i < num_hub_nodes && i < node_scores.size(); ++i)
    {
        hub_node_keys.insert(node_scores[i].first);
    }

    if (hub_node_keys.size() < target_hub_nodes)
    {
        for (size_t i = index->size(); i > 0 && hub_node_keys.size() < target_hub_nodes; --i)
        {
            hub_node_keys.insert(static_cast<USearchIndex::vector_key_t>(i - 1));
        }
    }

    LOG_TRACE(logger, "Identified {} hub nodes out of {} total nodes", hub_node_keys.size(), index->size());

    auto pruned_index = std::make_shared<USearchIndexWithSerialization>(dimensions, metric_kind, scalar_kind, usearch_hnsw_params);

    reserveIndexForSize(pruned_index, hub_node_keys.size());

    runBuildIndexTasks([&](auto & runner)
    {
        size_t hub_index = 0;
        for (auto key : hub_node_keys)
        {
            if (key >= stored_vectors.size())
                continue;

            const auto & vector = stored_vectors[key];
            auto new_key = static_cast<USearchIndex::vector_key_t>(hub_index++);

            runner.enqueueAndKeepTrack([&pruned_index, &vector, new_key]
            {
                auto result = pruned_index->add(new_key, vector.data());
                if (!result)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Could not add hub node to pruned index. Error: {}", result.error.release());
            });
        }
    });

    /// Replace original index with pruned index
    index = pruned_index;

    auto stats_after = index->getStatistics();
    LOG_TRACE(logger, "Hub pruning complete. Pruned index: {} nodes (reduced from {}), {} edges",
              stats_after.nodes, node_scores.size(), stats_after.edges);
}

}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSimilarity::getGranuleAndReset()
{
    /// Check experimental setting guard
    bool leann_enabled = false;
    if (leann_params.hub_pruning_ratio > 0.0)
    {
        auto query_context = CurrentThread::get().getQueryContext();
        if (query_context)
        {
            const auto & merge_tree_settings = query_context->getMergeTreeSettings();
            if (merge_tree_settings[MergeTreeSetting::allow_experimental_leann_optimization_for_hnsw])
                leann_enabled = true;
        }
    }

    if (leann_enabled && index && index->size() > 0 && !stored_vectors.empty())
    {
        performHubPruning(index, stored_vectors, dimensions, metric_kind, scalar_kind, usearch_hnsw_params, leann_params.hub_pruning_ratio, leann_params.sample_size);
    }

    LeaNNParams effective_leann_params = leann_params;
    if (!leann_enabled)
        effective_leann_params.hub_pruning_ratio = 0.0;

    auto granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index_name, metric_kind, scalar_kind, usearch_hnsw_params, index, effective_leann_params);

    /// Clear stored vectors after use
    stored_vectors.clear();
    index = nullptr;
    return granule;
}

namespace
{

template <typename Column>
void updateImpl(const ColumnArray * column_array, const ColumnArray::Offsets & column_array_offsets, USearchIndexWithSerializationPtr & index, size_t dimensions, size_t rows)
{
    const auto & column_array_data = column_array->getData();
    const auto & column_array_data_float = typeid_cast<const Column &>(column_array_data);
    const auto & column_array_data_float_data = column_array_data_float.getData();

    /// Check all sizes are the same
    for (size_t row = 0; row < rows - 1; ++row)
        if (column_array_offsets[row + 1] - column_array_offsets[row] != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column with vector similarity index must have equal length");

    /// Reserving space is mandatory
    reserveIndexForSize(index, index->size() + rows);

    /// Vector index creation is slooooow. Add the new rows in parallel. The threadpool is global to avoid oversubscription when multiple
    /// indexes are build simultaneously (e.g. multiple merges run at the same time).
    auto add_vector_to_index = [&](USearchIndex::vector_key_t key, size_t row)
    {
        const typename Column::ValueType & value = column_array_data_float_data[column_array_offsets[row - 1]];
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

    runBuildIndexTasks([&](auto & runner)
    {
        for (size_t row = 0; row < rows; ++row)
        {
            auto key = static_cast<USearchIndex::vector_key_t>(index_size + row);
            runner.enqueueAndKeepTrack([&add_vector_to_index, key, row] { add_vector_to_index(key, row); });
        }
    });
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

    /// Store vectors for hub pruning if enabled
    if (leann_params.hub_pruning_ratio > 0.0)
    {
        stored_vectors.reserve(stored_vectors.size() + rows);

        if (which.isFloat32())
        {
            const auto * column_float32 = typeid_cast<const ColumnFloat32 *>(&column_array->getData());
            const auto & data = column_float32->getData();
            for (size_t row = 0; row < rows; ++row)
            {
                size_t start = (row == 0) ? 0 : column_array_offsets[row - 1];
                size_t end = column_array_offsets[row];
                stored_vectors.emplace_back(data.begin() + start, data.begin() + end);
            }
        }
        else if (which.isFloat64())
        {
            const auto * column_float64 = typeid_cast<const ColumnFloat64 *>(&column_array->getData());
            const auto & data = column_float64->getData();
            for (size_t row = 0; row < rows; ++row)
            {
                size_t start = (row == 0) ? 0 : column_array_offsets[row - 1];
                size_t end = column_array_offsets[row];
                std::vector<Float32> vec;
                vec.reserve(end - start);
                for (size_t i = start; i < end; ++i)
                    vec.push_back(static_cast<Float32>(data[i]));
                stored_vectors.push_back(std::move(vec));
            }
        }
        else if (which.isBFloat16())
        {
            const auto * column_bf16 = typeid_cast<const ColumnBFloat16 *>(&column_array->getData());
            const auto & data = column_bf16->getData();
            for (size_t row = 0; row < rows; ++row)
            {
                size_t start = (row == 0) ? 0 : column_array_offsets[row - 1];
                size_t end = column_array_offsets[row];
                std::vector<Float32> vec;
                vec.reserve(end - start);
                for (size_t i = start; i < end; ++i)
                    vec.push_back(static_cast<Float32>(data[i]));
                stored_vectors.push_back(std::move(vec));
            }
        }
    }

    if (which.isFloat32())
        updateImpl<ColumnFloat32>(column_array, column_array_offsets, index, dimensions, rows);
    else if (which.isFloat64())
        updateImpl<ColumnFloat64>(column_array, column_array_offsets, index, dimensions, rows);
    else if (which.isBFloat16())
        updateImpl<ColumnBFloat16>(column_array, column_array_offsets, index, dimensions, rows);
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
        || index_fetch_multiplier <= 0.0 || index_fetch_multiplier > MAX_INDEX_FETCH_MULTIPLIER
        || (parameters && !std::isfinite(index_fetch_multiplier * static_cast<double>(parameters->limit))))
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
        || (parameters->distance_function == "cosineDistance" && metric_kind != unum::usearch::metric_kind_t::cos_k && metric_kind != unum::usearch::metric_kind_t::hamming_k))
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

    size_t limit = parameters->limit;
    if (parameters->additional_filters_present || is_rescoring)
        /// Additional filters mean post-filtering which means that matches may be removed. To compensate, allow to fetch more rows by a factor.
        /// Similarly, if rescoring is on, fetch more neighbours from the index and pass them for the final re-ranking by ORDER BY ... LIMIT.
        limit = std::min(static_cast<size_t>(static_cast<double>(limit) * index_fetch_multiplier), max_limit);

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

MergeTreeIndexVectorSimilarity::MergeTreeIndexVectorSimilarity(
    const IndexDescription & index_,
    UInt64 dimensions_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_,
    LeaNNParams leann_params_)
    : IMergeTreeIndex(index_)
    , dimensions(dimensions_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
    , leann_params(leann_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSimilarity::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index.name, metric_kind, scalar_kind, usearch_hnsw_params, leann_params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarity::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarity>(index.name, index.sample_block, dimensions, metric_kind, scalar_kind, usearch_hnsw_params, leann_params);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG::Node * /*predicate*/, ContextPtr /*context*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function not supported for vector similarity index");
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG::Node * /*predicate*/, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const
{
    const String & index_column = index.column_names[0];
    return std::make_shared<MergeTreeIndexConditionVectorSimilarity>(parameters, index_column, metric_kind, context);
}

MergeTreeIndexPtr vectorSimilarityIndexCreator(const IndexDescription & index)
{
    UInt64 dimensions = index.arguments[2].safeGet<UInt64>();

    /// Default parameters:
    unum::usearch::metric_kind_t metric_kind = distanceFunctionToMetricKind.at(index.arguments[1].safeGet<String>());
    unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::bf16_k;
    UsearchHnswParams usearch_hnsw_params;
    LeaNNParams leann_params;

    /// Optional parameters (only when 7 arguments are provided):
    const bool has_seven_args = (index.arguments.size() == 7);
    if (has_seven_args)
    {
        scalar_kind = quantizationToScalarKind.at(index.arguments[3].safeGet<String>());
        usearch_hnsw_params = {.connectivity  = index.arguments[4].safeGet<UInt64>(),
                               .expansion_add = index.arguments[5].safeGet<UInt64>()};

        /// Special handling for binary quantization:
        if (scalar_kind == unum::usearch::scalar_kind_t::b1x8_k)
            metric_kind = unum::usearch::metric_kind_t::hamming_k;

        /// Parse LeaNN parameters (7th argument)
        String leann_arg = index.arguments[6].safeGet<String>();

        if (leann_arg == "true" || leann_arg == "enable_hub_pruning")
        {
            /// Default to 0.5 (50%) for backward compatibility
            leann_params.hub_pruning_ratio = 0.5;
        }
        else if (leann_arg == "false" || leann_arg.empty())
        {
            leann_params.hub_pruning_ratio = 0.0;
        }
        else if (leann_arg.find('=') != String::npos)
        {
            /// Parse key-value pairs (e.g., "ratio=0.5,sample_size=200")
            size_t start = 0;
            while (start < leann_arg.size())
            {
                size_t comma_pos = leann_arg.find(',', start);
                String pair;
                if (comma_pos == String::npos)
                {
                    pair = leann_arg.substr(start);
                    start = leann_arg.size();
                }
                else
                {
                    pair = leann_arg.substr(start, comma_pos - start);
                    start = comma_pos + 1;
                }

                size_t eq_pos = pair.find('=');
                if (eq_pos == String::npos)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid key-value pair format in seventh argument: {}", pair);

                String key = pair.substr(0, eq_pos);
                String value = pair.substr(eq_pos + 1);

                if (key == "ratio")
                {
                    try
                    {
                        double ratio = std::stod(value);
                        if (ratio < 0.0 || ratio > 1.0)
                            throw Exception(ErrorCodes::INCORRECT_DATA, "hub_pruning_ratio must be between 0.0 and 1.0");
                        leann_params.hub_pruning_ratio = ratio;
                    }
                    catch (const std::exception &)
                    {
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid numeric value for hub_pruning_ratio: {}", value);
                    }
                }
                else if (key == "sample_size")
                {
                    try
                    {
                        size_t sample_size = std::stoull(value);
                        if (sample_size == 0)
                            throw Exception(ErrorCodes::INCORRECT_DATA, "sample_size must be greater than 0");
                        leann_params.sample_size = sample_size;
                    }
                    catch (const std::exception &)
                    {
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid numeric value for sample_size: {}", value);
                    }
                }
                else
                {
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown parameter in seventh argument: {}", key);
                }
            }
        }
        else
        {
            /// Try to parse as numeric ratio (0.0 to 1.0) for backward compatibility
            try
            {
                double ratio = std::stod(leann_arg);
                if (ratio < 0.0 || ratio > 1.0)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Seventh argument (hub_pruning_ratio) must be between 0.0 and 1.0");
                leann_params.hub_pruning_ratio = ratio;
            }
            catch (const std::exception &)
            {
                throw Exception(ErrorCodes::INCORRECT_DATA, "Seventh argument must be a numeric value between 0.0 and 1.0, key-value pairs (e.g., 'ratio=0.5,sample_size=200'), or 'true'/'enable_hub_pruning'/'false'");
            }
        }
    }

    return std::make_shared<MergeTreeIndexVectorSimilarity>(index, dimensions, metric_kind, scalar_kind, usearch_hnsw_params, leann_params);
}

void vectorSimilarityIndexValidator(const IndexDescription & index, bool /* attach */)
{
    const bool has_three_args = (index.arguments.size() == 3);
    const bool has_seven_args = (index.arguments.size() == 7);

    /// Check number and type of arguments
    if (!has_three_args && !has_seven_args)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index must have three or seven arguments");
    if (index.arguments[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "First argument of vector similarity index (method) must be of type String");
    if (index.arguments[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (index.arguments[2].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (dimensions) must be of type UInt64");
    if (has_seven_args)
    {
        if (index.arguments[3].getType() != Field::Types::String)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fourth argument of vector similarity index (quantization) must be of type String");
        if (index.arguments[4].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fifth argument of vector similarity index (hnsw_max_connections_per_layer) must be of type UInt64");
        if (index.arguments[5].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Sixth argument of vector similarity index (hnsw_candidate_list_size_for_construction) must be of type UInt64");
    }

    /// Check that passed arguments are supported
    if (!methods.contains(index.arguments[0].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "First argument (method) of vector similarity index is not supported. Supported methods are: {}", joinByComma(methods));
    if (!distanceFunctionToMetricKind.contains(index.arguments[1].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Second argument (distance function) of vector similarity index is not supported. Supported distance function are: {}", joinByComma(distanceFunctionToMetricKind));
    if (index.arguments[2].safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (dimensions) of vector similarity index must be > 0");
    if (has_seven_args)
    {
        if (!quantizationToScalarKind.contains(index.arguments[3].safeGet<String>()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Fourth argument (quantization) of vector similarity index is not supported. Supported quantizations are: {}", joinByComma(quantizationToScalarKind));

        /// More checks for binary quantization
        if (quantizationToScalarKind.at(index.arguments[3].safeGet<String>()) == unum::usearch::scalar_kind_t::b1x8_k)
        {
            if (distanceFunctionToMetricKind.at(index.arguments[1].safeGet<String>()) != unum::usearch::metric_kind_t::cos_k)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Binary quantization in vector similarity index can only be used with the cosine distance as distance function");
            if (index.arguments[2].safeGet<UInt64>() % 8 != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Binary quantization in vector similarity index requires that the dimension is a multiple of 8");
        }

        /// Call Usearch's own parameter validation method for HNSW-specific parameters
        UInt64 connectivity = index.arguments[4].safeGet<UInt64>();
        UInt64 expansion_add = index.arguments[5].safeGet<UInt64>();
        UInt64 expansion_search = default_expansion_search;
        unum::usearch::index_dense_config_t config(connectivity, expansion_add, expansion_search);
        if (auto error = config.validate(); error)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parameters passed to vector similarity index. Error: {}", error.release());
    }

    /// Validate LeaNN parameters (7th argument):
    if (has_seven_args)
    {
        if (index.arguments[6].getType() != Field::Types::String)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Seventh argument of vector similarity index (hub_pruning_ratio) must be of type String");

        String leann_arg = index.arguments[6].safeGet<String>();
        if (leann_arg != "enable_hub_pruning" && leann_arg != "true" && leann_arg != "false" && !leann_arg.empty())
        {
            if (leann_arg.find('=') != String::npos)
            {
                /// Validate key-value pairs
                size_t start = 0;
                while (start < leann_arg.size())
                {
                    size_t comma_pos = leann_arg.find(',', start);
                    String pair;
                    if (comma_pos == String::npos)
                    {
                        pair = leann_arg.substr(start);
                        start = leann_arg.size();
                    }
                    else
                    {
                        pair = leann_arg.substr(start, comma_pos - start);
                        start = comma_pos + 1;
                    }

                    size_t eq_pos = pair.find('=');
                    if (eq_pos == String::npos)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid key-value pair format in seventh argument: {}", pair);

                    String key = pair.substr(0, eq_pos);
                    String value = pair.substr(eq_pos + 1);

                    if (key == "ratio")
                    {
                        try
                        {
                            double ratio = std::stod(value);
                            if (ratio < 0.0 || ratio > 1.0)
                                throw Exception(ErrorCodes::INCORRECT_DATA, "hub_pruning_ratio must be between 0.0 and 1.0");
                        }
                        catch (const std::exception &)
                        {
                            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid numeric value for hub_pruning_ratio: {}", value);
                        }
                    }
                    else if (key == "sample_size")
                    {
                        try
                        {
                            size_t sample_size = std::stoull(value);
                            if (sample_size == 0)
                                throw Exception(ErrorCodes::INCORRECT_DATA, "sample_size must be greater than 0");
                        }
                        catch (const std::exception &)
                        {
                            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid numeric value for sample_size: {}", value);
                        }
                    }
                    else
                    {
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown parameter in seventh argument: {}", key);
                    }
                }
            }
            else
            {
                /// Check if it's a valid numeric value (backward compatibility)
                try
                {
                    double ratio = std::stod(leann_arg);
                    if (ratio < 0.0 || ratio > 1.0)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Seventh argument (hub_pruning_ratio) must be between 0.0 and 1.0");
                }
                catch (const std::exception &)
                {
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Seventh argument must be a numeric value between 0.0 and 1.0, key-value pairs (e.g., 'ratio=0.5,sample_size=200'), or 'true'/'enable_hub_pruning'/'false'");
                }
            }
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
