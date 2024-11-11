#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>

#if USE_USEARCH

#include <Columns/ColumnArray.h>
#include <Common/BitHelpers.h>
#include <Common/formatReadable.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>

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

namespace Setting
{
    extern const SettingsUInt64 hnsw_candidate_list_size_for_search;
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
    {"i8", unum::usearch::scalar_kind_t::i8_k}};
/// Usearch provides more quantizations but ^^ above ones seem the only ones comprehensively supported across all distance functions.

/// The vector similarity index implements scalar quantization on top of Usearch. This is the target type (currently, only i8 is supported).
using QuantizedValue = unum::usearch::i8_t;

/// The maximum number of dimensions for scalar quantization. The purpose is to be able to allocate space for the result row on the stack
/// (std::array) instead of the heap (std::vector). The value can be chosen randomly as long as the stack doesn't overflow.
constexpr size_t MAX_DIMENSIONS_FOR_SCALAR_QUANTIZATION = 3000;

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
        String joined_keys;
        for (const auto & [k, _] : t)
        {
            if (!joined_keys.empty())
                joined_keys += ", ";
            joined_keys += k;
        }
        return joined_keys;
    }
    /// TODO once our libcxx is recent enough, replace above by
    ///      return fmt::format("{}", fmt::join(std::views::keys(t)), ", "));
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
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not create vector similarity index. Error: {}", String(result.error.release()));
    swap(result.index);
}

void USearchIndexWithSerialization::serialize(WriteBuffer & ostr) const
{
    auto callback = [&ostr](void * from, size_t n)
    {
        ostr.write(reinterpret_cast<const char *>(from), n);
        return true;
    };

    if (auto result = Base::save_to_stream(callback); !result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not save vector similarity index. Error: {}", String(result.error.release()));
}

void USearchIndexWithSerialization::deserialize(ReadBuffer & istr)
{
    auto callback = [&istr](void * from, size_t n)
    {
        istr.readStrict(reinterpret_cast<char *>(from), n);
        return true;
    };

    if (auto result = Base::load_from_stream(callback); !result)
        /// See the comment in MergeTreeIndexGranuleVectorSimilarity::deserializeBinary why we throw here
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not load vector similarity index. Please drop the index and create it again. Error: {}", String(result.error.release()));

    try_reserve(limits());
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

    writeIntBinary(index->scalar_quantization_codebooks ? static_cast<UInt64>(1) : static_cast<UInt64>(0), ostr);
    if (index->scalar_quantization_codebooks)
    {
        for (const auto codebook : *(index->scalar_quantization_codebooks))
        {
            writeFloatBinary(codebook.min, ostr);
            writeFloatBinary(codebook.max, ostr);
        }
    }

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

    UInt64 has_scalar_quantization_codebooks;
    readIntBinary(has_scalar_quantization_codebooks, istr);
    if (has_scalar_quantization_codebooks)
    {
        index->scalar_quantization_codebooks = std::make_optional<ScalarQuantizationCodebooks>();
        for (size_t dimension = 0; dimension < dimensions; ++dimension)
        {
            Float64 min;
            Float64 max;
            readFloatBinary(min, istr);
            readFloatBinary(max, istr);
            index->scalar_quantization_codebooks->push_back({min, max});
        }
    }

    auto statistics = index->getStatistics();
    LOG_TRACE(logger, "Loaded vector similarity index: {}", statistics.toString());
}

MergeTreeIndexAggregatorVectorSimilarity::MergeTreeIndexAggregatorVectorSimilarity(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_,
    Float64 scalar_quantization_quantile_,
    size_t scalar_quantization_buffer_size_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
    , scalar_quantization_quantile(scalar_quantization_quantile_)
    , scalar_quantization_buffer_size(scalar_quantization_buffer_size_)
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

template <typename Value>
ScalarQuantizationCodebook calculateCodebook(std::vector<Value> & values, Float64 quantile)
{
    if (values.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "values is empty");

    std::ranges::sort(values);

    size_t minimum_element_index = static_cast<size_t>(values.size() * (1.0 - quantile));
    size_t maximum_element_index = std::min(static_cast<size_t>(values.size() * quantile), values.size() - 1);

    return {values[minimum_element_index], values[maximum_element_index]};
}

template <typename Value>
void quantize(
    const Value * values, size_t dimensions, const ScalarQuantizationCodebooks & codebooks,
    std::array<QuantizedValue, MAX_DIMENSIONS_FOR_SCALAR_QUANTIZATION> & quantized_vector)
{
    /// Does a similar calculation as in Usearch's cast_to_i8_gt::try_(byte_t const* input, std::size_t dim, byte_t* output)

    /// For some reason, USearch does not map into range [-std::numeric_limits<Int8>, std::numeric_limits<Int8>]
    /// aka. [-128, 127], it maps into [-127, 127]. Do the same here.
    constexpr QuantizedValue i8_min = -127;
    constexpr QuantizedValue i8_max = 127;

    Float64 magnitude = 0.0;
    for (size_t dimension = 0; dimension != dimensions; ++dimension)
    {
        Float64 value = static_cast<Float64>(*(values + dimension));
        magnitude += value * value;
    }
    magnitude = std::sqrt(magnitude);

    if (magnitude == 0.0)
    {
        for (std::size_t dimension = 0; dimension != dimensions; ++dimension)
            quantized_vector[dimension] = 0;
        return;
    }

    for (std::size_t dimension = 0; dimension != dimensions; ++dimension)
    {
        Float64 value = static_cast<Float64>(*(values + dimension));

        const ScalarQuantizationCodebook & codebook = codebooks[dimension];
        if (value < codebook.min)
        {
            quantized_vector[dimension] = i8_min;
            continue;
        }
        if (value > codebook.max)
        {
            quantized_vector[dimension] = i8_max;
            continue;
        }

        quantized_vector[dimension] = static_cast<QuantizedValue>(std::clamp(value * i8_max / magnitude, static_cast<Float64>(i8_min), static_cast<Float64>(i8_max)));

    }

    /// for (size_t dimension = 0; dimension < dimensions; ++dimension)
    /// {
    ///     const ScalarQuantizationCodebook & codebook = codebooks[dimension];
    ///     Float64 value = static_cast<Float64>(*(values + dimension));
    ///     LOG_TRACE(getLogger("Vector Similarity Index"), "{}: {} --> {} (cb: [{}, {}])", dimension, value, quantized_vector[dimension], codebook.min, codebook.max);
    /// }
}

template <typename Column>
void updateImpl(
    const ColumnArray * column_array, const ColumnArray::Offsets & column_array_offsets, USearchIndexWithSerializationPtr & index,
    size_t dimensions, size_t rows,
    Float64 scalar_quantization_quantile, size_t scalar_quantization_buffer_size)
{
    const auto & column_array_data = column_array->getData();
    const auto & column_array_data_float = typeid_cast<const Column &>(column_array_data);
    const auto & column_array_data_float_data = column_array_data_float.getData();

    /// Check all sizes are the same
    for (size_t row = 0; row < rows - 1; ++row)
        if (column_array_offsets[row + 1] - column_array_offsets[row] != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column with vector similarity index must have equal length");

    /// ------------------
    /// "Quantization" in Usearch means mere downsampling. We implement scalar quantization by ourselves.
    /// The math only works for i8 and cosine distance.
    /// --> compute for every dimension the quantiles and store them as "codebook" in the index.
    if (index->scalar_kind() == unum::usearch::scalar_kind_t::i8_k
        && index->metric_kind() == unum::usearch::metric_kind_t::cos_k
        && scalar_quantization_buffer_size != 0 && dimensions < MAX_DIMENSIONS_FOR_SCALAR_QUANTIZATION)
    {
        const size_t buffer_size = std::min(rows, scalar_quantization_buffer_size);
        /// Note: This function (update) can theoretically be called in a chunked fashion but this is currently not done, i.e. update is
        /// called exactly once per index granule. This simplifies the code, so we make this assumption for now (otherwise, we'd need to
        /// integrate with getGranuleAndReset which "finalizes" the insert of rows).

        using ColumnValue = std::conditional_t<std::is_same_v<Column,ColumnFloat32>, Float32, Float64>;
        std::vector<std::vector<ColumnValue>> values_per_dimension;

        values_per_dimension.resize(dimensions);
        for (auto & values : values_per_dimension)
            values.resize(buffer_size);

        /// Row-to-column conversion, needed because calculateCodebook sorts along each dimension
        for (size_t i = 0; i < buffer_size * dimensions; ++i)
        {
            ColumnValue value = column_array_data_float_data[i];
            size_t x = i % dimensions;
            size_t y = i / dimensions;
            values_per_dimension[x][y] = value;
        }

        index->scalar_quantization_codebooks = std::make_optional<ScalarQuantizationCodebooks>();
        for (size_t dimension = 0; dimension < dimensions; ++dimension)
        {
            ScalarQuantizationCodebook codebook = calculateCodebook(values_per_dimension[dimension], scalar_quantization_quantile);
            /// Invalid codebook that would lead to division-by-0 during quantizaiton. May happen if buffer size is too small or the data
            /// distribution is too weird. Continue without quantization.
            if (codebook.min == codebook.max)
            {
                index->scalar_quantization_codebooks = std::nullopt;
                break;
            }
            index->scalar_quantization_codebooks->push_back(codebook);
        }
    }
    /// ------------------

    /// Reserving space is mandatory
    size_t max_thread_pool_size = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
    if (max_thread_pool_size == 0)
        max_thread_pool_size = getNumberOfCPUCoresToUse();
    unum::usearch::index_limits_t limits(roundUpToPowerOfTwoOrZero(index->size() + rows), max_thread_pool_size);
    index->reserve(limits);

    /// Vector index creation is slooooow. Add the new rows in parallel. The threadpool is global to avoid oversubscription when multiple
    /// indexes are build simultaneously (e.g. multiple merges run at the same time).
    auto & thread_pool = Context::getGlobalContextInstance()->getBuildVectorSimilarityIndexThreadPool();

    auto add_vector_to_index = [&](USearchIndex::vector_key_t key, size_t row, ThreadGroupPtr thread_group)
    {
        SCOPE_EXIT_SAFE(
            if (thread_group)
                CurrentThread::detachFromGroupIfNotDetached();
        );

        if (thread_group)
            CurrentThread::attachToGroupIfDetached(thread_group);

        USearchIndexWithSerialization::add_result_t add_result;

        if (index->scalar_quantization_codebooks)
        {
            const ScalarQuantizationCodebooks & codebooks = *(index->scalar_quantization_codebooks);
            std::array<QuantizedValue, MAX_DIMENSIONS_FOR_SCALAR_QUANTIZATION> quantized_vector;
            quantize(&column_array_data_float_data[column_array_offsets[row - 1]], dimensions, codebooks, quantized_vector);
            add_result = index->add(key, quantized_vector.data());
        }
        else
        {
            add_result = index->add(key, &column_array_data_float_data[column_array_offsets[row - 1]]);
        }

        if (!add_result)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Could not add data to vector similarity index. Error: {}", String(add_result.error.release()));

        ProfileEvents::increment(ProfileEvents::USearchAddCount);
        ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, add_result.visited_members);
        ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, add_result.computed_distances);
    };

    const size_t index_size = index->size();
    for (size_t row = 0; row < rows; ++row)
    {
        auto key = static_cast<USearchIndex::vector_key_t>(index_size + row);
        auto task = [&add_vector_to_index, key, row, thread_group = CurrentThread::getGroup()] { add_vector_to_index(key, row, thread_group); };
        thread_pool.scheduleOrThrowOnError(task);
    }

    thread_pool.wait();
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array(Float*) column");

    if (column_array->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Array is unexpectedly empty");

    /// The vector similarity algorithm naturally assumes that the indexed vectors have dimension >= 1. This condition is violated if empty arrays
    /// are INSERTed into an vector-similarity-indexed column or if no value was specified at all in which case the arrays take on their default
    /// values which is also empty.
    if (column_array->isDefaultAt(0))
        throw Exception(ErrorCodes::INCORRECT_DATA, "The arrays in column '{}' must not be empty. Did you try to INSERT default values?", index_column_name);

    const size_t rows = column_array->size();

    const auto & column_array_offsets = column_array->getOffsets();
    const size_t dimensions = column_array_offsets[0];

    if (!index)
        index = std::make_shared<USearchIndexWithSerialization>(dimensions, metric_kind, scalar_kind, usearch_hnsw_params);

    /// Also check that previously inserted blocks have the same size as this block.
    /// Note that this guarantees consistency of dimension only within parts. We are unable to detect inconsistent dimensions across
    /// parts - for this, a little help from the user is needed, e.g. CONSTRAINT cnstr CHECK length(array) = 42.
    if (index->dimensions() != dimensions)
        throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column with vector similarity index must have equal length");

    /// We use Usearch's index_dense_t as index type which supports only 4 bio entries according to https://github.com/unum-cloud/usearch/tree/main/cpp
    if (index->size() + rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Size of vector similarity index would exceed 4 billion entries");

    const auto * data_type_array = typeid_cast<const DataTypeArray *>(block.getByName(index_column_name).type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");
    const TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();

    if (WhichDataType(nested_type_index).isFloat32())
        updateImpl<ColumnFloat32>(column_array, column_array_offsets, index, dimensions, rows, scalar_quantization_quantile, scalar_quantization_buffer_size);
    else if (WhichDataType(nested_type_index).isFloat64())
        updateImpl<ColumnFloat64>(column_array, column_array_offsets, index, dimensions, rows, scalar_quantization_quantile, scalar_quantization_buffer_size);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");

    *pos += rows_read;
}

MergeTreeIndexConditionVectorSimilarity::MergeTreeIndexConditionVectorSimilarity(
    const IndexDescription & /*index_description*/,
    const SelectQueryInfo & query,
    unum::usearch::metric_kind_t metric_kind_,
    ContextPtr context)
    : vector_similarity_condition(query, context)
    , metric_kind(metric_kind_)
    , expansion_search(context->getSettingsRef()[Setting::hnsw_candidate_list_size_for_search])
{
    if (expansion_search == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'hnsw_candidate_list_size_for_search' must not be 0");

}

bool MergeTreeIndexConditionVectorSimilarity::mayBeTrueOnGranule(MergeTreeIndexGranulePtr) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for vector similarity indexes");
}

bool MergeTreeIndexConditionVectorSimilarity::alwaysUnknownOrTrue() const
{
    String index_distance_function;
    switch (metric_kind)
    {
        case unum::usearch::metric_kind_t::l2sq_k: index_distance_function = "L2Distance"; break;
        case unum::usearch::metric_kind_t::cos_k:  index_distance_function = "cosineDistance"; break;
        default: std::unreachable();
    }
    return vector_similarity_condition.alwaysUnknownOrTrue(index_distance_function);
}

std::vector<UInt64> MergeTreeIndexConditionVectorSimilarity::calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule_) const
{
    const UInt64 limit = vector_similarity_condition.getLimit();

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleVectorSimilarity>(granule_);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    const USearchIndexWithSerializationPtr index = granule->index;

    if (vector_similarity_condition.getDimensions() != index->dimensions())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the space in the request ({}) does not match the dimension in the index ({})",
            vector_similarity_condition.getDimensions(), index->dimensions());

    const std::vector<Float64> reference_vector = vector_similarity_condition.getReferenceVector();

    /// We want to run the search with the user-provided value for setting hnsw_candidate_list_size_for_search (aka. expansion_search).
    /// The way to do this in USearch is to call index_dense_gt::change_expansion_search. Unfortunately, this introduces a need to
    /// synchronize index access, see https://github.com/unum-cloud/usearch/issues/500. As a workaround, we extended USearch' search method
    /// to accept a custom expansion_add setting. The config value is only used on the fly, i.e. not persisted in the index.

    std::vector<USearchIndex::vector_key_t> neighbors; /// indexes of vectors which were closest to the reference vector

    if (index->scalar_quantization_codebooks)
    {
        const ScalarQuantizationCodebooks & codebooks = *(index->scalar_quantization_codebooks);
        std::array<QuantizedValue, MAX_DIMENSIONS_FOR_SCALAR_QUANTIZATION> quantized_vector;
        quantize(reference_vector.data(), index->dimensions(), codebooks, quantized_vector);
        auto search_result = index->search(quantized_vector.data(), limit, USearchIndex::any_thread(), false, expansion_search);
        if (!search_result)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Could not search in vector similarity index. Error: {}", search_result.error.release());
        neighbors.resize(search_result.size());
        search_result.dump_to(neighbors.data());

        ProfileEvents::increment(ProfileEvents::USearchSearchCount);
        ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, search_result.visited_members);
        ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, search_result.computed_distances);
    }
    else
    {
        auto search_result = index->search(reference_vector.data(), limit, USearchIndex::any_thread(), false, expansion_search);
        if (!search_result)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Could not search in vector similarity index. Error: {}", search_result.error.release());
        neighbors.resize(search_result.size());
        search_result.dump_to(neighbors.data());

        ProfileEvents::increment(ProfileEvents::USearchSearchCount);
        ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, search_result.visited_members);
        ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, search_result.computed_distances);
    }

    std::sort(neighbors.begin(), neighbors.end());

    /// Duplicates should in theory not be possible but who knows ...
    const bool has_duplicates = std::adjacent_find(neighbors.begin(), neighbors.end()) != neighbors.end();
    if (has_duplicates)
#ifndef NDEBUG
        throw Exception(ErrorCodes::INCORRECT_DATA, "Usearch returned duplicate row numbers");
#else
        neighbors.erase(std::unique(neighbors.begin(), neighbors.end()), neighbors.end());
#endif

    return neighbors;
}

MergeTreeIndexVectorSimilarity::MergeTreeIndexVectorSimilarity(
    const IndexDescription & index_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : IMergeTreeIndex(index_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSimilarity::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index.name, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarity::createIndexAggregator(const MergeTreeWriterSettings & settings) const
{
    Float64 scalar_quantization_quantile = settings.scalar_quantization_quantile_for_vector_similarity_index;
    size_t scalar_quantization_buffer_size = settings.scalar_quantization_buffer_size_for_vector_similarity_index;

    if (scalar_quantization_quantile < 0.5 || scalar_quantization_quantile > 1.0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'scalar_quantization_quantile_for_vector_similarity_index' must be in [0.5, 1.0]");

    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarity>(index.name, index.sample_block, metric_kind, scalar_kind, usearch_hnsw_params, scalar_quantization_quantile, scalar_quantization_buffer_size);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionVectorSimilarity>(index, query, metric_kind, context);
};

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG *, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Vector similarity index cannot be created with ActionsDAG");
}

MergeTreeIndexPtr vectorSimilarityIndexCreator(const IndexDescription & index)
{
    /// Default parameters:
    unum::usearch::metric_kind_t metric_kind = distanceFunctionToMetricKind.at(index.arguments[1].safeGet<String>());
    unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::bf16_k;
    UsearchHnswParams usearch_hnsw_params;

    /// Optional parameters:
    const bool has_five_args = (index.arguments.size() == 5);
    if (has_five_args)
    {
        scalar_kind = quantizationToScalarKind.at(index.arguments[2].safeGet<String>());
        usearch_hnsw_params = {.connectivity  = index.arguments[3].safeGet<UInt64>(),
                               .expansion_add = index.arguments[4].safeGet<UInt64>()};
    }

    return std::make_shared<MergeTreeIndexVectorSimilarity>(index, metric_kind, scalar_kind, usearch_hnsw_params);
}

void vectorSimilarityIndexValidator(const IndexDescription & index, bool /* attach */)
{
    const bool has_two_args = (index.arguments.size() == 2);
    const bool has_five_args = (index.arguments.size() == 5);
    const bool has_six_args = (index.arguments.size() == 6); /// Legacy index creation syntax before #70616. Supported only to be able to load old tables, can be removed mid-2025.
                                                             /// The 6th argument (ef_search) is ignored.

    /// Check number and type of arguments
    if (!has_two_args && !has_five_args && !has_six_args)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index must have two or five arguments");
    if (index.arguments[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "First argument of vector similarity index (method) must be of type String");
    if (index.arguments[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (has_five_args || has_six_args)
    {
        if (index.arguments[2].getType() != Field::Types::String)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (quantization) must be of type String");
        if (index.arguments[3].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fourth argument of vector similarity index (hnsw_max_connections_per_layer) must be of type UInt64");
        if (index.arguments[4].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fifth argument of vector similarity index (hnsw_candidate_list_size_for_construction) must be of type UInt64");
    }

    /// Check that passed arguments are supported
    if (!methods.contains(index.arguments[0].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "First argument (method) of vector similarity index is not supported. Supported methods are: {}", joinByComma(methods));
    if (!distanceFunctionToMetricKind.contains(index.arguments[1].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Second argument (distance function) of vector similarity index is not supported. Supported distance function are: {}", joinByComma(distanceFunctionToMetricKind));
    if (has_five_args)
    {
        if (!quantizationToScalarKind.contains(index.arguments[2].safeGet<String>()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (quantization) of vector similarity index is not supported. Supported quantizations are: {}", joinByComma(quantizationToScalarKind));

        /// Call Usearch's own parameter validation method for HNSW-specific parameters
        UInt64 connectivity = index.arguments[3].safeGet<UInt64>();
        UInt64 expansion_add = index.arguments[4].safeGet<UInt64>();
        UInt64 expansion_search = default_expansion_search;

        unum::usearch::index_dense_config_t config(connectivity, expansion_add, expansion_search);
        if (auto error = config.validate(); error)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parameters passed to vector similarity index. Error: {}", String(error.release()));
    }

    /// Check that the index is created on a single column
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Vector similarity indexes must be created on a single column");

    /// Check that the data type is Array(Float*)
    DataTypePtr data_type = index.sample_block.getDataTypes()[0];
    const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector similarity indexes can only be created on columns of type Array(Float*)");
    TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
    if (!WhichDataType(nested_type_index).isFloat())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector similarity indexes can only be created on columns of type Array(Float*)");
}

}

#endif
