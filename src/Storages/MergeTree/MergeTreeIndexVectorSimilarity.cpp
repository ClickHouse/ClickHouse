#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>

#if USE_USEARCH

#include <usearch/index_plugins.hpp>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/filesystemHelpers.h>
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
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/castColumn.h>

#include <algorithm>
#include <filesystem>
#include <ranges>

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

std::set<String> methods()
{
    std::set<String> result = {"hnsw"};
#if USE_PDX
    result.insert("pdx");
#endif
    return result;
}

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

VectorSimilarityMethod methodFromString(const String & method)
{
    if (method == "hnsw")
        return VectorSimilarityMethod::HNSW;
    if (method == "pdx")
        return VectorSimilarityMethod::PDX;

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown vector similarity method {}", method);
}

String methodToString(VectorSimilarityMethod method)
{
    switch (method)
    {
        case VectorSimilarityMethod::HNSW:
            return "hnsw";
        case VectorSimilarityMethod::PDX:
            return "pdx";
    }
    std::unreachable();
}

#if USE_PDX
PDX::DistanceMetric getPdxDistanceMetric(unum::usearch::metric_kind_t metric_kind)
{
    switch (metric_kind)
    {
        case unum::usearch::metric_kind_t::l2sq_k:
            return PDX::DistanceMetric::L2SQ;
        case unum::usearch::metric_kind_t::cos_k:
            return PDX::DistanceMetric::COSINE;
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "PDX supports only L2Distance and cosineDistance");
    }
}

std::shared_ptr<PDX::IPDXIndex> createPDXIndex(UInt64 dimensions, unum::usearch::metric_kind_t metric_kind)
{
    if (dimensions > PDX::PDX_MAX_DIMS)
    {
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "PDX supports dimensions up to {}, got {}",
            PDX::PDX_MAX_DIMS,
            dimensions);
    }

    PDX::PDXIndexConfig config;
    config.num_dimensions = static_cast<uint32_t>(dimensions);
    config.distance_metric = getPdxDistanceMetric(metric_kind);
    config.n_threads = getNumberOfCPUCoresToUse();

    return std::make_shared<PDX::PDXIndexF32>(config);
}

void serializePDXIndex(std::shared_ptr<PDX::IPDXIndex> index, WriteBuffer & ostr)
{
    if (!index)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to serialize empty PDX index");

    auto tmp = createTemporaryFile(std::filesystem::temp_directory_path().string());
    index->Save(tmp->path());

    size_t bytes = std::filesystem::file_size(tmp->path());
    writeIntBinary(static_cast<UInt64>(bytes), ostr);

    ReadBufferFromFile in(tmp->path());
    copyData(in, ostr, bytes);
}

std::shared_ptr<PDX::IPDXIndex> deserializePDXIndex(
    ReadBuffer & istr,
    UInt64 dimensions,
    unum::usearch::metric_kind_t metric_kind)
{
    UInt64 bytes = 0;
    readIntBinary(bytes, istr);
    if (bytes == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized PDX index is empty");

    auto tmp = createTemporaryFile(std::filesystem::temp_directory_path().string());
    WriteBufferFromFile out(tmp->path());
    copyData(istr, out, static_cast<size_t>(bytes));
    out.finalize();

    auto index = createPDXIndex(dimensions, metric_kind);
    index->Restore(tmp->path());
    return index;
}
#endif
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
    VectorSimilarityMethod method_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : MergeTreeIndexGranuleVectorSimilarity(
        index_name_,
        method_,
        metric_kind_,
        scalar_kind_,
        usearch_hnsw_params_,
        nullptr
#if USE_PDX
        ,
        nullptr
#endif
    )
{
}

MergeTreeIndexGranuleVectorSimilarity::MergeTreeIndexGranuleVectorSimilarity(
    const String & index_name_,
    VectorSimilarityMethod method_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_,
    USearchIndexWithSerializationPtr hnsw_index_
#if USE_PDX
    ,
    std::shared_ptr<PDX::IPDXIndex> pdx_index_
#endif
    )
    : index_name(index_name_)
    , method(method_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
    , hnsw_index(std::move(hnsw_index_))
#if USE_PDX
    , pdx_index(std::move(pdx_index_))
#endif
{
}

bool MergeTreeIndexGranuleVectorSimilarity::empty() const
{
    if (method == VectorSimilarityMethod::HNSW)
        return !hnsw_index || hnsw_index->size() == 0;

#if USE_PDX
    return !pdx_index;
#else
    return true;
#endif
}

size_t MergeTreeIndexGranuleVectorSimilarity::memoryUsageBytes() const
{
    if (method == VectorSimilarityMethod::HNSW)
        return hnsw_index->memoryUsageBytes();

#if USE_PDX
    return pdx_index ? pdx_index->GetInMemorySizeInBytes() : 0;
#else
    return 0;
#endif
}

void MergeTreeIndexGranuleVectorSimilarity::serializeBinary(WriteBuffer & ostr) const
{
    LOG_TRACE(logger, "Start writing vector similarity index");

    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty vector similarity index {}", backQuote(index_name));

    writeIntBinary(FILE_FORMAT_VERSION, ostr);
    writeIntBinary(static_cast<UInt8>(method), ostr);

    if (method == VectorSimilarityMethod::HNSW)
    {
        /// Number of dimensions is required in the index constructor,
        /// so it must be written and read separately from the other part
        writeIntBinary(static_cast<UInt64>(hnsw_index->dimensions()), ostr);
        hnsw_index->serialize(ostr);
        auto statistics = hnsw_index->getStatistics();
        LOG_TRACE(logger, "Wrote vector similarity index (method={}): {}", methodToString(method), statistics.toString());
    }
#if USE_PDX
    else if (method == VectorSimilarityMethod::PDX)
    {
        writeIntBinary(static_cast<UInt64>(pdx_index->GetNumDimensions()), ostr);
        serializePDXIndex(pdx_index, ostr);
        LOG_TRACE(
            logger,
            "Wrote vector similarity index (method={}, dimensions={}, clusters={})",
            methodToString(method),
            pdx_index->GetNumDimensions(),
            pdx_index->GetNumClusters());
    }
#endif
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported vector similarity method {}", static_cast<UInt64>(static_cast<UInt8>(method)));
    }
}

void MergeTreeIndexGranuleVectorSimilarity::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    LOG_TRACE(logger, "Start loading vector similarity index");

    UInt64 file_version;
    readIntBinary(file_version, istr);
    if (file_version != 1 && file_version != FILE_FORMAT_VERSION)
        throw Exception(
            ErrorCodes::FORMAT_VERSION_TOO_OLD,
            "Vector similarity index could not be loaded because its version is too old (current version: {}, persisted version: {}). Please drop the index and create it again.",
            FILE_FORMAT_VERSION, file_version);
        /// More fancy error handling would be: Set a flag on the index that it failed to load. During usage return all granules, i.e.
        /// behave as if the index does not exist. Since format changes are expected to happen only rarely and it is "only" an index, keep it simple for now.

    auto persisted_method = VectorSimilarityMethod::HNSW;
    if (file_version >= 2)
    {
        UInt8 method_raw = 0;
        readIntBinary(method_raw, istr);
        if (method_raw > static_cast<UInt8>(VectorSimilarityMethod::PDX))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown vector similarity method tag {}", static_cast<UInt64>(method_raw));
        persisted_method = static_cast<VectorSimilarityMethod>(method_raw);
    }

    if (persisted_method != method)
    {
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Vector similarity index method mismatch (expected {}, got {}). Please drop the index and create it again.",
            methodToString(method),
            methodToString(persisted_method));
    }

    UInt64 dimensions;
    readIntBinary(dimensions, istr);
    if (method == VectorSimilarityMethod::HNSW)
    {
        hnsw_index = std::make_shared<USearchIndexWithSerialization>(dimensions, metric_kind, scalar_kind, usearch_hnsw_params);
        hnsw_index->deserialize(istr);
        auto statistics = hnsw_index->getStatistics();
        LOG_TRACE(logger, "Loaded vector similarity index (method={}): {}", methodToString(method), statistics.toString());
    }
#if USE_PDX
    else if (method == VectorSimilarityMethod::PDX)
    {
        pdx_index = deserializePDXIndex(istr, dimensions, metric_kind);
        LOG_TRACE(
            logger,
            "Loaded vector similarity index (method={}, dimensions={}, clusters={})",
            methodToString(method),
            pdx_index->GetNumDimensions(),
            pdx_index->GetNumClusters());
    }
#endif
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported vector similarity method {}", static_cast<UInt64>(static_cast<UInt8>(method)));
    }
}

MergeTreeIndexAggregatorVectorSimilarity::MergeTreeIndexAggregatorVectorSimilarity(
    const String & index_name_,
    const Block & index_sample_block_,
    VectorSimilarityMethod method_,
    UInt64 dimensions_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , method(method_)
    , dimensions(dimensions_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

bool MergeTreeIndexAggregatorVectorSimilarity::empty() const
{
    if (method == VectorSimilarityMethod::HNSW)
        return !hnsw_index || hnsw_index->size() == 0;

#if USE_PDX
    /// PDX index is materialized lazily in getGranuleAndReset(), so emptiness must be
    /// checked against accumulated embeddings, not against pdx_index pointer.
    return pdx_rows == 0 && pdx_embeddings.empty() && !pdx_index;
#else
    return true;
#endif
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSimilarity::getGranuleAndReset()
{
    if (method == VectorSimilarityMethod::HNSW)
    {
        auto granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(
            index_name,
            method,
            metric_kind,
            scalar_kind,
            usearch_hnsw_params,
            hnsw_index
#if USE_PDX
            ,
            nullptr
#endif
        );
        hnsw_index = nullptr;
        return granule;
    }

#if USE_PDX
    if (!pdx_index && !pdx_embeddings.empty())
    {
        pdx_index = createPDXIndex(dimensions, metric_kind);
        pdx_index->BuildIndex(pdx_embeddings.data(), pdx_rows);
    }

    auto granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(
        index_name,
        method,
        metric_kind,
        scalar_kind,
        usearch_hnsw_params,
        nullptr,
        pdx_index);
    pdx_index = nullptr;
    pdx_embeddings.clear();
    pdx_rows = 0;
    return granule;
#else
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PDX method is not available");
#endif
}

namespace
{

template <typename Column>
void updateImplHNSW(const ColumnArray * column_array, const ColumnArray::Offsets & column_array_offsets, USearchIndexWithSerializationPtr & hnsw_index, size_t dimensions, size_t rows)
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
    unum::usearch::index_limits_t limits(roundUpToPowerOfTwoOrZero(hnsw_index->size() + rows), max_thread_pool_size);
    hnsw_index->reserve(limits);

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

        size_t data_offset = (row == 0 ? 0 : column_array_offsets[row - 1]);
        const typename Column::ValueType & value = column_array_data_float_data[data_offset];
        unum::usearch::index_dense_t::add_result_t result;

        /// Note: add is thread-safe
        if constexpr (std::is_same_v<Column, ColumnBFloat16>)
        {
            /// bf16 was standardized with C++23 but libcxx does not support it yet.
            /// As a result, ClickHouse and usearch each emulate bf16 and we need to implement some ugly special handling for bf16 below.
            result = hnsw_index->add(key, reinterpret_cast<const unum::usearch::bf16_bits_t *>(&value.raw()));
        }
        else
        {
            static_assert(std::is_same_v<Column, ColumnFloat32> || std::is_same_v<Column, ColumnFloat64>);
            result = hnsw_index->add(key, &value);
        }

        if (!result)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Could not add data to vector similarity index. Error: {}", result.error.release());

        ProfileEvents::increment(ProfileEvents::USearchAddCount);
        ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, result.visited_members);
        ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, result.computed_distances);
    };


    size_t index_size = hnsw_index->size();
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

#if USE_PDX
template <typename Column>
void appendImplPDX(
    const ColumnArray * column_array,
    const ColumnArray::Offsets & column_array_offsets,
    std::vector<float> & pdx_embeddings,
    size_t dimensions,
    size_t rows)
{
    const auto & column_array_data = column_array->getData();
    const auto & column_array_data_float = typeid_cast<const Column &>(column_array_data);
    const auto & column_array_data_float_data = column_array_data_float.getData();

    for (size_t row = 0; row < rows - 1; ++row)
        if (column_array_offsets[row + 1] - column_array_offsets[row] != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column with vector similarity index must have equal length");

    pdx_embeddings.reserve(pdx_embeddings.size() + rows * dimensions);

    for (size_t row = 0; row < rows; ++row)
    {
        size_t begin = (row == 0 ? 0 : column_array_offsets[row - 1]);
        size_t end = column_array_offsets[row];
        if (end - begin != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column with vector similarity index must have equal length");

        for (size_t i = begin; i < end; ++i)
            pdx_embeddings.emplace_back(static_cast<float>(column_array_data_float_data[i]));
    }
}
#endif

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

    if (method == VectorSimilarityMethod::HNSW)
    {
        if (!hnsw_index)
            hnsw_index = std::make_shared<USearchIndexWithSerialization>(dimensions, metric_kind, scalar_kind, usearch_hnsw_params);

        /// We use Usearch's index_dense_t as index type which supports only 4 bio entries according to https://github.com/unum-cloud/usearch/tree/main/cpp
        if (hnsw_index->size() + rows > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Size of vector similarity index would exceed 4 billion entries");
    }
#if USE_PDX
    else if (method == VectorSimilarityMethod::PDX)
    {
        if (pdx_rows + rows > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Size of vector similarity index would exceed 4 billion entries");
    }
#else
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PDX method is not available");
    }
#endif

    const auto * data_type_array = typeid_cast<const DataTypeArray *>(block.getByName(index_column_name).type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float32|Float64|BFloat16)");

    const TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
    WhichDataType which(nested_type_index);
    if (method == VectorSimilarityMethod::HNSW)
    {
        if (which.isFloat32())
            updateImplHNSW<ColumnFloat32>(column_array, column_array_offsets, hnsw_index, dimensions, rows);
        else if (which.isFloat64())
            updateImplHNSW<ColumnFloat64>(column_array, column_array_offsets, hnsw_index, dimensions, rows);
        else if (which.isBFloat16())
            updateImplHNSW<ColumnBFloat16>(column_array, column_array_offsets, hnsw_index, dimensions, rows);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");
    }
#if USE_PDX
    else
    {
        if (which.isFloat32())
            appendImplPDX<ColumnFloat32>(column_array, column_array_offsets, pdx_embeddings, dimensions, rows);
        else if (which.isFloat64())
            appendImplPDX<ColumnFloat64>(column_array, column_array_offsets, pdx_embeddings, dimensions, rows);
        else if (which.isBFloat16())
            appendImplPDX<ColumnBFloat16>(column_array, column_array_offsets, pdx_embeddings, dimensions, rows);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");

        pdx_rows += rows;
        pdx_index = nullptr;
    }
#endif


    *pos += rows_read;
}

MergeTreeIndexConditionVectorSimilarity::MergeTreeIndexConditionVectorSimilarity(
    const std::optional<VectorSearchParameters> & parameters_,
    const String & index_column_,
    VectorSimilarityMethod method_,
    unum::usearch::metric_kind_t metric_kind_,
    ContextPtr context)
    : parameters(parameters_)
    , index_column(index_column_)
    , method(method_)
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

    size_t limit = parameters->limit;
    if (parameters->additional_filters_present || is_rescoring)
        /// Additional filters mean post-filtering which means that matches may be removed. To compensate, allow to fetch more rows by a factor.
        /// Similarly, if rescoring is on, fetch more neighbours from the index and pass them for the final re-ranking by ORDER BY ... LIMIT.
        limit = std::min(static_cast<size_t>(static_cast<double>(limit) * index_fetch_multiplier), max_limit);

    NearestNeighbours result;
    if (method == VectorSimilarityMethod::HNSW)
    {
        const USearchIndexWithSerializationPtr index = granule->hnsw_index;

        if (!index)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HNSW index is not initialized");

        if (parameters->reference_vector.size() != index->dimensions())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
                parameters->reference_vector.size(), index->dimensions());

        /// We want to run the search with the user-provided value for setting hnsw_candidate_list_size_for_search (aka. expansion_search).
        /// The way to do this in USearch is to call index_dense_gt::change_expansion_search. Unfortunately, this introduces a need to
        /// synchronize index access, see https://github.com/unum-cloud/usearch/issues/500. As a workaround, we extended USearch' search method
        /// to accept a custom expansion_add setting. The config value is only used on the fly, i.e. not persisted in the index.
        auto search_result = index->search(parameters->reference_vector.data(), limit, USearchIndex::any_thread(), false, expansion_search);
        if (!search_result)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Could not search in vector similarity index. Error: {}", search_result.error.release());

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
    }
#if USE_PDX
    else
    {
        if (!granule->pdx_index)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PDX index is not initialized");

        if (parameters->reference_vector.size() != granule->pdx_index->GetNumDimensions())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
                parameters->reference_vector.size(), granule->pdx_index->GetNumDimensions());

        std::vector<float> reference_vector(parameters->reference_vector.size());
        for (size_t i = 0; i < parameters->reference_vector.size(); ++i)
            reference_vector[i] = static_cast<float>(parameters->reference_vector[i]);

        size_t nprobe = std::min<size_t>(expansion_search, granule->pdx_index->GetNumClusters());
        if (nprobe == 0)
            nprobe = 1;
        granule->pdx_index->SetNProbe(static_cast<UInt32>(nprobe));

        auto search_result = granule->pdx_index->Search(reference_vector.data(), limit);
        std::sort(search_result.begin(), search_result.end(), [](const auto & lhs, const auto & rhs) { return lhs.distance < rhs.distance; });

        result.rows.reserve(search_result.size());
        if (parameters->return_distances)
            result.distances.emplace();

        for (const auto & candidate : search_result)
        {
            result.rows.emplace_back(candidate.index);
            if (result.distances)
                result.distances->emplace_back(candidate.distance);
        }
    }
#endif

    return result;
}

MergeTreeIndexVectorSimilarity::MergeTreeIndexVectorSimilarity(
    const IndexDescription & index_,
    VectorSimilarityMethod method_,
    UInt64 dimensions_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : IMergeTreeIndex(index_)
    , method(method_)
    , dimensions(dimensions_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSimilarity::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index.name, method, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarity::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarity>(index.name, index.sample_block, method, dimensions, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG::Node * /*predicate*/, ContextPtr /*context*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function not supported for vector similarity index");
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG::Node * /*predicate*/, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const
{
    const String & index_column = index.column_names[0];
    return std::make_shared<MergeTreeIndexConditionVectorSimilarity>(parameters, index_column, method, metric_kind, context);
}

MergeTreeIndexPtr vectorSimilarityIndexCreator(const IndexDescription & index)
{
    FieldVector args = getFieldsFromIndexArgumentsAST(index.arguments);
    auto method = methodFromString(args[0].safeGet<String>());
#if !USE_PDX
    if (method == VectorSimilarityMethod::PDX)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PDX support is not compiled");
#endif
    UInt64 dimensions = args[2].safeGet<UInt64>();

    /// Default parameters:
    unum::usearch::metric_kind_t metric_kind = distanceFunctionToMetricKind.at(args[1].safeGet<String>());
    unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::bf16_k;
    UsearchHnswParams usearch_hnsw_params;

    /// Optional parameters:
    const bool has_six_args = (args.size() == 6);
    if (has_six_args && method == VectorSimilarityMethod::HNSW)
    {
        scalar_kind = quantizationToScalarKind.at(args[3].safeGet<String>());
        usearch_hnsw_params = {.connectivity  = args[4].safeGet<UInt64>(),
                               .expansion_add = args[5].safeGet<UInt64>()};

        /// Special handling for binary quantization:
        if (scalar_kind == unum::usearch::scalar_kind_t::b1x8_k)
            metric_kind = unum::usearch::metric_kind_t::hamming_k;
    }
    else if (has_six_args)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "vector_similarity method '{}' accepts exactly three arguments", methodToString(method));
    }

    return std::make_shared<MergeTreeIndexVectorSimilarity>(index, method, dimensions, metric_kind, scalar_kind, usearch_hnsw_params);
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

    const auto available_methods = methods();
    const String method_name = args[0].safeGet<String>();

#if !USE_PDX
    if (method_name == "pdx")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PDX support is not compiled");
#endif

    /// Check that passed arguments are supported
    if (!available_methods.contains(method_name))
        throw Exception(ErrorCodes::INCORRECT_DATA, "First argument (method) of vector similarity index is not supported. Supported methods are: {}", joinByComma(available_methods));
    if (!distanceFunctionToMetricKind.contains(args[1].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Second argument (distance function) of vector similarity index is not supported. Supported distance function are: {}", joinByComma(distanceFunctionToMetricKind));
    if (args[2].safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (dimensions) of vector similarity index must be > 0");

    const auto method = methodFromString(method_name);

    if (method == VectorSimilarityMethod::PDX)
    {
        if (has_six_args)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "vector_similarity method 'pdx' accepts exactly three arguments");
    }
    else if (has_six_args)
    {
        if (!quantizationToScalarKind.contains(args[3].safeGet<String>()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Fourth argument (quantization) of vector similarity index is not supported. Supported quantizations are: {}", joinByComma(quantizationToScalarKind));

        /// More checks for binary quantization
        if (quantizationToScalarKind.at(args[3].safeGet<String>()) == unum::usearch::scalar_kind_t::b1x8_k)
        {
            if (distanceFunctionToMetricKind.at(args[1].safeGet<String>()) != unum::usearch::metric_kind_t::cos_k)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Binary quantization in vector similarity index can only be used with the cosine distance as distance function");
            if (args[2].safeGet<UInt64>() % 8 != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Binary quantization in vector similarity index requires that the dimension is a multiple of 8");
        }

        /// Call Usearch's own parameter validation method for HNSW-specific parameters
        UInt64 connectivity = args[4].safeGet<UInt64>();
        UInt64 expansion_add = args[5].safeGet<UInt64>();
        UInt64 expansion_search = default_expansion_search;
        unum::usearch::index_dense_config_t config(connectivity, expansion_add, expansion_search);
        if (auto error = config.validate(); error)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parameters passed to vector similarity index. Error: {}", error.release());
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
