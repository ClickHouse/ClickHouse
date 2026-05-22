#include <Storages/MergeTree/MergeTreeIndexVectorSpann.h>

#if USE_USEARCH

#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/IndicesDescription.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/formatReadable.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
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

#include <algorithm>
#include <cmath>
#include <limits>
#include <ranges>
#include <string_view>
#include <type_traits>

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
    extern const SettingsBool allow_experimental_vector_spann_index;
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
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

constexpr size_t vector_spann_centroid_assignment_top_k = 1;
constexpr size_t vector_spann_max_entries_per_posting_list = 65536;
constexpr float vector_spann_cosine_norm_epsilon = 1e-24f;

const std::set<String> spann_methods = {"spann"};

const std::unordered_map<String, unum::usearch::metric_kind_t> distanceFunctionToMetricKind = {
    {"L2Distance", unum::usearch::metric_kind_t::l2sq_k},
    {"cosineDistance", unum::usearch::metric_kind_t::cos_k},
    {"dotProduct", unum::usearch::metric_kind_t::ip_k}};

const std::unordered_map<String, unum::usearch::scalar_kind_t> quantizationToScalarKind = {
    {"f64", unum::usearch::scalar_kind_t::f64_k},
    {"f32", unum::usearch::scalar_kind_t::f32_k},
    {"f16", unum::usearch::scalar_kind_t::f16_k},
    {"bf16", unum::usearch::scalar_kind_t::bf16_k},
    {"i8", unum::usearch::scalar_kind_t::i8_k},
    {"b1", unum::usearch::scalar_kind_t::b1x8_k}};

Float32 distanceToQuery(unum::usearch::metric_kind_t metric_kind, const Float32 * query, const Float32 * vec, size_t dimensions)
{
    if (metric_kind == unum::usearch::metric_kind_t::l2sq_k)
    {
        Float64 acc = 0;
        for (size_t i = 0; i < dimensions; ++i)
        {
            Float64 d = static_cast<Float64>(query[i]) - static_cast<Float64>(vec[i]);
            acc += d * d;
        }
        return static_cast<Float32>(acc);
    }
    if (metric_kind == unum::usearch::metric_kind_t::cos_k)
    {
        Float64 dot = 0;
        Float64 nq = 0;
        Float64 nv = 0;
        for (size_t i = 0; i < dimensions; ++i)
        {
            Float64 q = query[i];
            Float64 v = vec[i];
            dot += q * v;
            nq += q * q;
            nv += v * v;
        }
        Float64 denom = std::sqrt(std::max(nq * nv, static_cast<Float64>(vector_spann_cosine_norm_epsilon)));
        return static_cast<Float32>(1.0 - dot / denom);
    }
    if (metric_kind == unum::usearch::metric_kind_t::ip_k)
    {
        Float64 dot = 0;
        for (size_t i = 0; i < dimensions; ++i)
            dot += static_cast<Float64>(query[i]) * static_cast<Float64>(vec[i]);
        return static_cast<Float32>(dot);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported metric kind for vector_spann index search");
}

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
                throw Exception(
                    error_code,
                    "Vector for vector_spann index ({}) must not contain non-finite values (NaN or Inf)",
                    context);
        }
        else
        {
            if (!std::isfinite(casted))
                throw Exception(
                    error_code,
                    "Vector for vector_spann index ({}) must not contain non-finite values (NaN or Inf)",
                    context);
        }

        if (scalar_kind == unum::usearch::scalar_kind_t::i8_k)
        {
            double v = static_cast<double>(vector[i]);
            magnitude_squared += v * v;
        }
    }

    if (scalar_kind == unum::usearch::scalar_kind_t::i8_k && magnitude_squared == 0.0)
        throw Exception(
            error_code,
            "Zero-magnitude vectors for vector_spann index ({}) are not supported with `i8` quantization",
            context);
}

size_t postingListSerializedBytes(size_t count, size_t dimensions)
{
    return sizeof(UInt32) + count * (sizeof(UInt64) + dimensions * sizeof(Float32));
}

void writePostingList(WriteBuffer & out, const std::vector<SpannPostingEntry> & list, size_t dimensions)
{
    writeBinaryLittleEndian(static_cast<UInt32>(list.size()), out);
    for (const auto & entry : list)
    {
        if (entry.vector.size() != dimensions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann posting vector size mismatch");
        writeBinaryLittleEndian(entry.row_id, out);
        for (size_t i = 0; i < dimensions; ++i)
            writeBinaryLittleEndian(entry.vector[i], out);
    }
}

void readPostingList(ReadBuffer & in, std::vector<SpannPostingEntry> & list, size_t dimensions)
{
    UInt32 count = 0;
    readBinaryLittleEndian(count, in);
    list.resize(count);
    for (UInt32 i = 0; i < count; ++i)
    {
        readBinaryLittleEndian(list[i].row_id, in);
        list[i].vector.resize(dimensions);
        for (size_t d = 0; d < dimensions; ++d)
            readBinaryLittleEndian(list[i].vector[d], in);
    }
}

}

MergeTreeIndexGranuleVectorSpann::MergeTreeIndexGranuleVectorSpann(const String & index_name_, const SpannParams & params_)
    : index_name(index_name_)
    , params(params_)
    , centroid_index(nullptr)
    , centroid_offsets{}
    , postings_by_centroid{}
{
}

MergeTreeIndexGranuleVectorSpann::MergeTreeIndexGranuleVectorSpann(
    const String & index_name_,
    const SpannParams & params_,
    USearchIndexWithSerializationPtr centroid_index_,
    std::vector<SpannCentroidOffset> centroid_offsets_,
    std::vector<std::vector<SpannPostingEntry>> postings_by_centroid_)
    : index_name(index_name_)
    , params(params_)
    , centroid_index(std::move(centroid_index_))
    , centroid_offsets(std::move(centroid_offsets_))
    , postings_by_centroid(std::move(postings_by_centroid_))
{
}

void MergeTreeIndexGranuleVectorSpann::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'vector_spann' must be serialized with multiple streams");
}

void MergeTreeIndexGranuleVectorSpann::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'vector_spann' must be deserialized with multiple streams");
}

void MergeTreeIndexGranuleVectorSpann::serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const
{
    auto * regular_stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    auto * posting_stream = streams.at(MergeTreeIndexSubstream::Type::SpannPostingLists);
    if (!regular_stream || !posting_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann index requires Regular and SpannPostingLists streams");

    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty vector_spann index {}", backQuote(index_name));

    const UInt64 centroid_count = centroid_index->size();
    if (centroid_offsets.size() != centroid_count || postings_by_centroid.size() != centroid_count)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann granule has inconsistent centroid metadata");

    for (UInt64 i = 0; i < centroid_count; ++i)
    {
        const UInt32 expected_len = static_cast<UInt32>(postingListSerializedBytes(postings_by_centroid[i].size(), params.dimensions));
        if (centroid_offsets[i].length != expected_len)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann centroid offset length mismatch at centroid {}", i);
    }

    writeIntBinary(FILE_FORMAT_VERSION, regular_stream->compressed_hashing);
    writeIntBinary(static_cast<UInt64>(params.dimensions), regular_stream->compressed_hashing);
    writeIntBinary(centroid_count, regular_stream->compressed_hashing);
    centroid_index->serialize(regular_stream->compressed_hashing);

    for (UInt64 i = 0; i < centroid_count; ++i)
    {
        writeIntBinary(centroid_offsets[i].offset, regular_stream->compressed_hashing);
        writeIntBinary(centroid_offsets[i].length, regular_stream->compressed_hashing);
    }

    for (UInt64 i = 0; i < centroid_count; ++i)
        writePostingList(posting_stream->compressed_hashing, postings_by_centroid[i], params.dimensions);

    auto statistics = centroid_index->getStatistics();
    LOG_TRACE(logger, "Wrote vector_spann index granule: {}", statistics.toString());
}

/// Loads all posting lists for this skip-index granule into memory (see loop over readPostingList below).
/// Paper-style selective IO would read only the posting ranges for centroids visited by the query; that needs API work:
/// calculateApproximateNearestNeighbors receives only `granule`, not MergeTreeIndexReader streams (see MergeTreeDataSelectExecutor::filterMarksUsingIndex).
void MergeTreeIndexGranuleVectorSpann::deserializeBinaryWithMultipleStreams(
    MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    auto * regular_stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    auto * posting_stream = streams.at(MergeTreeIndexSubstream::Type::SpannPostingLists);
    if (!regular_stream || !posting_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann index requires Regular and SpannPostingLists streams");

    ReadBuffer & regular_buf = *regular_stream->getDataBuffer();
    ReadBuffer & posting_buf = *posting_stream->getDataBuffer();

    UInt64 file_version = 0;
    readIntBinary(file_version, regular_buf);
    if (file_version != FILE_FORMAT_VERSION)
        throw Exception(
            ErrorCodes::FORMAT_VERSION_TOO_OLD,
            "vector_spann index could not be loaded because its version is too old (current version: {}, persisted version: {}). Please drop the index and create it again.",
            FILE_FORMAT_VERSION,
            file_version);

    UInt64 dimensions_on_disk = 0;
    readIntBinary(dimensions_on_disk, regular_buf);
    if (dimensions_on_disk != params.dimensions)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "vector_spann index dimensions mismatch on disk ({} vs {})",
            dimensions_on_disk,
            params.dimensions);

    UInt64 centroid_count = 0;
    readIntBinary(centroid_count, regular_buf);

    centroid_index = std::make_shared<USearchIndexWithSerialization>(
        static_cast<size_t>(params.dimensions), params.metric_kind, params.scalar_kind, params.usearch_hnsw_params);
    centroid_index->deserialize(regular_buf);

    if (centroid_index->size() != centroid_count)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "vector_spann centroid count mismatch after load (expected {}, have {})",
            centroid_count,
            centroid_index->size());

    centroid_offsets.resize(centroid_count);
    for (UInt64 i = 0; i < centroid_count; ++i)
    {
        readIntBinary(centroid_offsets[i].offset, regular_buf);
        readIntBinary(centroid_offsets[i].length, regular_buf);
    }

    postings_by_centroid.assign(centroid_count, {});
    for (UInt64 i = 0; i < centroid_count; ++i)
        readPostingList(posting_buf, postings_by_centroid[i], static_cast<size_t>(params.dimensions));

    LOG_TRACE(logger, "Loaded vector_spann index granule (format version {})", state.version);
}

size_t MergeTreeIndexGranuleVectorSpann::memoryUsageBytes() const
{
    size_t postings_bytes = 0;
    for (const auto & plist : postings_by_centroid)
        for (const auto & e : plist)
            postings_bytes += sizeof(SpannPostingEntry) + e.vector.capacity() * sizeof(Float32);
    return sizeof(*this) + centroid_offsets.size() * sizeof(SpannCentroidOffset) + postings_bytes
        + (centroid_index ? centroid_index->memoryUsageBytes() : 0);
}

MergeTreeIndexAggregatorVectorSpann::MergeTreeIndexAggregatorVectorSpann(
    const String & index_name_, const Block & index_sample_block_, const SpannParams & params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , params(params_)
{
}

std::vector<UInt64> MergeTreeIndexAggregatorVectorSpann::selectCentroidsRandom() const
{
    const size_t n = accumulated_vectors.size();
    if (n == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann: cannot select centroids from empty granule");

    size_t num_centroids = static_cast<size_t>(std::ceil(static_cast<double>(n) * static_cast<double>(params.centroid_ratio)));
    num_centroids = std::max<size_t>(1, num_centroids);
    num_centroids = std::min(num_centroids, n);

    std::vector<UInt64> all_ids(n);
    for (size_t i = 0; i < n; ++i)
        all_ids[i] = i;

    std::shuffle(all_ids.begin(), all_ids.end(), thread_local_rng);
    all_ids.resize(num_centroids);
    std::ranges::sort(all_ids);
    return all_ids;
}

USearchIndexWithSerializationPtr MergeTreeIndexAggregatorVectorSpann::buildCentroidIndex(const std::vector<UInt64> & centroid_row_ids) const
{
    auto centroid_index = std::make_shared<USearchIndexWithSerialization>(
        static_cast<size_t>(params.dimensions), params.metric_kind, params.scalar_kind, params.usearch_hnsw_params);

    size_t max_thread_pool_size = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
    if (max_thread_pool_size == 0)
        max_thread_pool_size = getNumberOfCPUCoresToUse();
    unum::usearch::index_limits_t lim(roundUpToPowerOfTwoOrZero(centroid_row_ids.size()), max_thread_pool_size);
    centroid_index->reserve(lim);

    auto & thread_pool = Context::getGlobalContextInstance()->getBuildVectorSimilarityIndexThreadPool();

    auto add_centroid = [&](unum::usearch::index_dense_t::vector_key_t key, UInt64 row_id_in_accumulated)
    {
        if (auto query_context = CurrentThread::tryGetQueryContext())
            if (auto query_status = query_context->getProcessListElementSafe())
                query_status->throwIfKilled();

        const Float32 * ptr = accumulated_vectors[row_id_in_accumulated].data();
        checkVectorIsSane(ptr, params.dimensions, params.scalar_kind, ErrorCodes::INCORRECT_DATA, "indexed vector");
        unum::usearch::index_dense_t::add_result_t result = centroid_index->add(key, ptr);
        if (!result)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Could not add centroid to vector_spann index. Error: {}", result.error.release());

        ProfileEvents::increment(ProfileEvents::USearchAddCount);
        ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, result.visited_members);
        ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, result.computed_distances);
    };

    ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::MERGETREE_VECTOR_SIM_INDEX);
    for (size_t i = 0; i < centroid_row_ids.size(); ++i)
    {
        const auto key = static_cast<unum::usearch::index_dense_t::vector_key_t>(i);
        const UInt64 row_id = centroid_row_ids[i];
        runner.enqueueAndKeepTrack([&add_centroid, key, row_id] { add_centroid(key, row_id); });
    }
    runner.waitForAllToFinishAndRethrowFirstError();

    return centroid_index;
}

std::pair<std::vector<SpannCentroidOffset>, std::vector<std::vector<SpannPostingEntry>>> MergeTreeIndexAggregatorVectorSpann::assignAndMakePostings(
    const USearchIndexWithSerializationPtr & centroid_index) const
{
    const size_t k = centroid_index->size();
    std::vector<std::vector<SpannPostingEntry>> postings_by_centroid(k);

    for (size_t i = 0; i < accumulated_vectors.size(); ++i)
    {
        const auto & vec = accumulated_vectors[i];
        auto search_result = centroid_index->search(
            vec.data(), vector_spann_centroid_assignment_top_k, unum::usearch::index_dense_t::any_thread(), false, default_expansion_search);
        if (!search_result)
            throw Exception(ErrorCodes::INCORRECT_DATA, "vector_spann centroid search failed: {}", search_result.error.release());

        if (search_result.size() != vector_spann_centroid_assignment_top_k)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann centroid search expected one neighbour");

        std::vector<unum::usearch::index_dense_t::vector_key_t> centroid_key_storage(vector_spann_centroid_assignment_top_k);
        search_result.dump_to(centroid_key_storage.data());
        const size_t centroid_key = static_cast<size_t>(centroid_key_storage[0]);
        if (centroid_key >= k)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann centroid key out of range");

        SpannPostingEntry entry;
        entry.row_id = accumulated_row_ids[i];
        entry.vector = vec;
        postings_by_centroid[centroid_key].push_back(std::move(entry));

        ProfileEvents::increment(ProfileEvents::USearchSearchCount);
        ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, search_result.visited_members);
        ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, search_result.computed_distances);
    }

    std::vector<SpannCentroidOffset> offsets(k);
    UInt64 pos = 0;
    const auto logger = getLogger("VectorSpannIndex");
    for (size_t i = 0; i < k; ++i)
    {
        offsets[i].offset = pos;
        if (postings_by_centroid[i].size() > vector_spann_max_entries_per_posting_list)
            LOG_WARNING(
                logger,
                "vector_spann posting list for centroid {} in index {} has {} entries (soft threshold {}); "
                "consider increasing centroid_ratio. Serialized size is still bounded by UInt32.",
                i,
                index_name,
                postings_by_centroid[i].size(),
                vector_spann_max_entries_per_posting_list);
        const size_t len = postingListSerializedBytes(postings_by_centroid[i].size(), params.dimensions);
        if (len > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::INCORRECT_DATA, "vector_spann posting list for centroid {} is too large", i);
        offsets[i].length = static_cast<UInt32>(len);
        pos += len;
    }

    return {std::move(offsets), std::move(postings_by_centroid)};
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSpann::getGranuleAndReset()
{
    if (accumulated_vectors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann aggregator has no data for granule {}", backQuote(index_name));

    const auto centroid_row_ids = selectCentroidsRandom();
    auto centroid_index = buildCentroidIndex(centroid_row_ids);
    auto offsets_and_postings = assignAndMakePostings(centroid_index);

    accumulated_vectors.clear();
    accumulated_row_ids.clear();

    return std::make_shared<MergeTreeIndexGranuleVectorSpann>(
        index_name,
        params,
        std::move(centroid_index),
        std::move(offsets_and_postings.first),
        std::move(offsets_and_postings.second));
}

void MergeTreeIndexAggregatorVectorSpann::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos,
            block.rows());

    const size_t rows_read = std::min(limit, block.rows() - *pos);
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array(Float32) column for vector_spann index");

    const auto & column_array_offsets = column_array->getOffsets();
    const size_t rows = column_array->size();
    if (rows == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Array is unexpectedly empty");

    const size_t dimensions_inserted = column_array_offsets[0];
    if (params.dimensions != dimensions_inserted)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Array values in column with vector_spann index have {} elements, expects {} elements",
            dimensions_inserted,
            params.dimensions);

    for (size_t row = 0; row + 1 < rows; ++row)
        if (column_array_offsets[row + 1] - column_array_offsets[row] != params.dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column with vector_spann index must have equal length");

    const auto * data_type_array = typeid_cast<const DataTypeArray *>(block.getByName(index_column_name).type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float32)");
    const TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
    WhichDataType which(nested_type_index);
    if (!which.isFloat32())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "vector_spann index supports only Array(Float32)");

    const auto & column_array_data = column_array->getData();
    const auto & column_float = typeid_cast<const ColumnFloat32 &>(column_array_data);
    const auto & column_float_data = column_float.getData();

    const size_t base_row_id = accumulated_vectors.size();
    for (size_t row = 0; row < rows; ++row)
    {
        accumulated_row_ids.push_back(base_row_id + row);
        const size_t start = row == 0 ? 0 : column_array_offsets[row - 1];
        const size_t end = column_array_offsets[row];
        std::vector<Float32> vec(column_float_data.begin() + start, column_float_data.begin() + end);
        checkVectorIsSane(vec.data(), params.dimensions, params.scalar_kind, ErrorCodes::INCORRECT_DATA, "indexed vector");
        accumulated_vectors.emplace_back(std::move(vec));
    }

    *pos += rows_read;
}

MergeTreeIndexConditionVectorSpann::MergeTreeIndexConditionVectorSpann(
    const std::optional<VectorSearchParameters> & parameters_,
    const String & index_column_,
    unum::usearch::metric_kind_t metric_kind_,
    const SpannParams & params_,
    ContextPtr context)
    : parameters(parameters_)
    , index_column(index_column_)
    , metric_kind(metric_kind_)
    , params(params_)
    , search_epsilon(0.0f)
    , index_fetch_multiplier(context->getSettingsRef()[Setting::vector_search_index_fetch_multiplier])
    , max_limit(context->getSettingsRef()[Setting::max_limit_for_vector_search_queries])
    , is_rescoring(context->getSettingsRef()[Setting::vector_search_with_rescoring])
    , max_posting_lists(std::max<size_t>(1, context->getSettingsRef()[Setting::hnsw_candidate_list_size_for_search]))
    , expansion_search(context->getSettingsRef()[Setting::hnsw_candidate_list_size_for_search])
{
    static constexpr auto MAX_INDEX_FETCH_MULTIPLIER = 1000.0;

    if (expansion_search == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'hnsw_candidate_list_size_for_search' must not be 0");

    if (!std::isfinite(index_fetch_multiplier)
        || index_fetch_multiplier <= 0.0 || index_fetch_multiplier > MAX_INDEX_FETCH_MULTIPLIER
        || (parameters && !std::isfinite(index_fetch_multiplier * static_cast<double>(parameters->limit))))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'vector_search_index_fetch_multiplier' must be greater than 0.0 and less than {}", MAX_INDEX_FETCH_MULTIPLIER);
}

bool MergeTreeIndexConditionVectorSpann::mayBeTrueOnGranule(MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for vector_spann indexes");
}

bool MergeTreeIndexConditionVectorSpann::alwaysUnknownOrTrue() const
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

/// Vector search path provides only the deserialized granule (no MergeTreeIndexReader / part streams here).
NearestNeighbours MergeTreeIndexConditionVectorSpann::calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule_) const
{
    if (!parameters)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected vector_search_parameters to be set");

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleVectorSpann>(granule_);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type for vector_spann");

    const auto & centroid_index = granule->centroid_index;
    if (!centroid_index || parameters->reference_vector.size() != params.dimensions)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
            parameters->reference_vector.size(),
            params.dimensions);

    checkVectorIsSane(
        parameters->reference_vector.data(),
        parameters->reference_vector.size(),
        params.scalar_kind,
        ErrorCodes::INCORRECT_QUERY,
        "reference vector in the SELECT query");

    std::vector<Float32> query_float(params.dimensions);
    for (size_t i = 0; i < params.dimensions; ++i)
        query_float[i] = static_cast<Float32>(parameters->reference_vector[i]);

    checkVectorIsSane(
        query_float.data(),
        query_float.size(),
        params.scalar_kind,
        ErrorCodes::INCORRECT_QUERY,
        "reference vector in the SELECT query after conversion to Float32");

    auto search_centroids = centroid_index->search(
        query_float.data(), max_posting_lists, unum::usearch::index_dense_t::any_thread(), false, expansion_search);
    if (!search_centroids)
        throw Exception(ErrorCodes::INCORRECT_DATA, "vector_spann centroid search failed: {}", search_centroids.error.release());

    struct Candidate
    {
        UInt64 row_id = 0;
        Float32 distance = 0;
    };
    std::vector<Candidate> candidates;
    candidates.reserve(search_centroids.size() * 4);

    std::vector<unum::usearch::index_dense_t::vector_key_t> centroid_keys(search_centroids.size());
    if (!centroid_keys.empty())
        search_centroids.dump_to(centroid_keys.data());

    for (size_t ci = 0; ci < search_centroids.size(); ++ci)
    {
        const size_t centroid_idx = static_cast<size_t>(centroid_keys[ci]);
        if (centroid_idx >= granule->postings_by_centroid.size())
            continue;
        for (const auto & entry : granule->postings_by_centroid[centroid_idx])
        {
            Candidate c;
            c.row_id = entry.row_id;
            c.distance = distanceToQuery(metric_kind, query_float.data(), entry.vector.data(), params.dimensions);
            candidates.push_back(c);
        }
    }

    size_t limit = parameters->limit;
    if (parameters->additional_filters_present || is_rescoring)
        limit = std::min(static_cast<size_t>(static_cast<double>(limit) * index_fetch_multiplier), max_limit);

    const bool higher_is_better = (metric_kind == unum::usearch::metric_kind_t::ip_k);
    auto compare = [higher_is_better](const Candidate & a, const Candidate & b)
    {
        return higher_is_better ? (a.distance > b.distance) : (a.distance < b.distance);
    };
    if (candidates.size() <= limit)
    {
        std::ranges::sort(candidates, compare);
    }
    else
    {
        std::partial_sort(
            candidates.begin(),
            candidates.begin() + static_cast<std::ptrdiff_t>(limit),
            candidates.end(),
            compare);
        candidates.resize(limit);
    }

    NearestNeighbours result;
    result.rows.resize(std::min(limit, candidates.size()));
    for (size_t i = 0; i < result.rows.size(); ++i)
        result.rows[i] = candidates[i].row_id;

    if (parameters->return_distances)
    {
        result.distances = std::vector<float>(result.rows.size());
        for (size_t i = 0; i < result.rows.size(); ++i)
            result.distances->at(i) = candidates[i].distance;
    }

    ProfileEvents::increment(ProfileEvents::USearchSearchCount);
    ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, search_centroids.visited_members);
    ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, search_centroids.computed_distances);

    (void)search_epsilon;
    return result;
}

MergeTreeIndexVectorSpann::MergeTreeIndexVectorSpann(const IndexDescription & index_, SpannParams params_)
    : IMergeTreeIndex(index_)
    , params(std::move(params_))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSpann::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleVectorSpann>(index.name, params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSpann::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorVectorSpann>(index.name, index.sample_block, params);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSpann::createIndexCondition(const ActionsDAG::Node *, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function not supported for vector_spann index");
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSpann::createIndexCondition(
    const ActionsDAG::Node *, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const
{
    return std::make_shared<MergeTreeIndexConditionVectorSpann>(parameters, index.column_names[0], params.metric_kind, params, context);
}

MergeTreeIndexSubstreams MergeTreeIndexVectorSpann::getSubstreams() const
{
    return {
        {MergeTreeIndexSubstream::Type::Regular, "", ".idx"},
        {MergeTreeIndexSubstream::Type::SpannPostingLists, ".pl", ".idx"},
    };
}

MergeTreeIndexFormat MergeTreeIndexVectorSpann::getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, path_prefix, ".idx"))
        return {1, getSubstreams()};
    return {0, {}};
}

MergeTreeIndexPtr spannIndexCreator(const IndexDescription & index)
{
    FieldVector args = getFieldsFromIndexArgumentsAST(index.arguments);
    SpannParams spann_params;
    spann_params.dimensions = args[2].safeGet<UInt64>();

    spann_params.metric_kind = distanceFunctionToMetricKind.at(args[1].safeGet<String>());
    spann_params.scalar_kind = unum::usearch::scalar_kind_t::bf16_k;
    spann_params.usearch_hnsw_params = {};

    const bool has_seven_args = (args.size() == 7);
    if (has_seven_args)
    {
        spann_params.scalar_kind = quantizationToScalarKind.at(args[3].safeGet<String>());
        spann_params.usearch_hnsw_params.connectivity = args[4].safeGet<UInt64>();
        spann_params.usearch_hnsw_params.expansion_add = args[5].safeGet<UInt64>();
        if (args[6].getType() != Field::Types::Float64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Seventh argument of vector_spann index (centroid_ratio) must be Float");
        spann_params.centroid_ratio = static_cast<float>(args[6].safeGet<Float64>());
    }

    return std::make_shared<MergeTreeIndexVectorSpann>(index, std::move(spann_params));
}

void spannIndexValidator(const IndexDescription & index, bool attach)
{
    if (!attach)
    {
        ContextPtr context = CurrentThread::tryGetQueryContext();
        if (!context)
            context = Context::getGlobalContextInstance();

        if (!context || !context->getSettingsRef()[Setting::allow_experimental_vector_spann_index])
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "vector_spann index is an experimental feature. "
                "Set setting `allow_experimental_vector_spann_index = 1` to enable it.");
        }
    }

    FieldVector args = getFieldsFromIndexArgumentsAST(index.arguments);
    const bool has_three_args = (args.size() == 3);
    const bool has_seven_args = (args.size() == 7);

    if (!has_three_args && !has_seven_args)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "vector_spann index must have three or seven arguments");

    if (args[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "First argument of vector_spann index (method) must be of type String");
    if (args[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector_spann index (metric) must be of type String");
    if (args[2].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector_spann index (dimensions) must be of type UInt64");

    if (has_seven_args)
    {
        if (args[3].getType() != Field::Types::String)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fourth argument of vector_spann index (quantization) must be of type String");
        if (args[4].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fifth argument of vector_spann index (hnsw_max_connections_per_layer) must be of type UInt64");
        if (args[5].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Sixth argument of vector_spann index (hnsw_candidate_list_size_for_construction) must be of type UInt64");
        if (args[6].getType() != Field::Types::Float64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Seventh argument of vector_spann index (centroid_ratio) must be Float");
    }

    if (!spann_methods.contains(args[0].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "First argument (method) of vector_spann index is not supported. Supported methods are: spann");
    if (!distanceFunctionToMetricKind.contains(args[1].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Second argument (distance function) of vector_spann index is not supported");
    if (args[2].safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (dimensions) of vector_spann index must be > 0");

    if (has_seven_args)
    {
        if (!quantizationToScalarKind.contains(args[3].safeGet<String>()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Fourth argument (quantization) of vector_spann index is not supported");

        if (quantizationToScalarKind.at(args[3].safeGet<String>()) == unum::usearch::scalar_kind_t::b1x8_k)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Binary quantization (`b1`) is not supported in vector_spann: posting lists store Float32 vectors and Hamming-distance reranking over Float32 vectors is not meaningful");
        }

        const UInt64 connectivity = args[4].safeGet<UInt64>();
        const UInt64 expansion_add = args[5].safeGet<UInt64>();
        unum::usearch::index_dense_config_t config(connectivity, expansion_add, default_expansion_search);
        if (auto error = config.validate(); error)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parameters passed to vector_spann index. Error: {}", error.release());

        const Float64 ratio = args[6].safeGet<Float64>();
        if (!std::isfinite(ratio) || ratio <= 0.0 || ratio > 1.0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Seventh argument (centroid_ratio) of vector_spann index must be in (0, 1]");
    }

    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "vector_spann index must be created on a single column");

    const DataTypePtr data_type = index.sample_block.getDataTypes()[0];
    const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "vector_spann index can only be created on columns of type Array(Float32)");
    const TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
    WhichDataType which(nested_type_index);
    if (!which.isFloat32())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "vector_spann index can only be created on columns of type Array(Float32)");
}

}

#endif
