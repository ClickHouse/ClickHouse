#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>

#if USE_USEARCH

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/BitHelpers.h>
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
    {"i8", unum::usearch::scalar_kind_t::i8_k}};
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
        ostr.write(reinterpret_cast<const char *>(from), n);
        return true;
    };

    if (auto result = Base::save_to_stream(callback); !result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not save vector similarity index. Error: {}", result.error.release());
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
    size_t max_thread_pool_size = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::max_build_vector_similarity_index_thread_pool_size];
    if (max_thread_pool_size == 0)
        max_thread_pool_size = getNumberOfCPUCoresToUse();
    unum::usearch::index_limits_t limits(roundUpToPowerOfTwoOrZero(index->size() + rows), max_thread_pool_size);
    index->reserve(limits);

    /// Vector index creation is slooooow. Add the new rows in parallel. The threadpool is global to avoid oversubscription when multiple
    /// indexes are build simultaneously (e.g. multiple merges run at the same time).
    auto & thread_pool = Context::getGlobalContextInstance()->getBuildVectorSimilarityIndexThreadPool();

    ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, "VectorSimIndex");
    auto add_vector_to_index = [&](USearchIndex::vector_key_t key, size_t row)
    {
        /// add is thread-safe
        auto result = index->add(key, &column_array_data_float_data[column_array_offsets[row - 1]]);
        if (!result)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Could not add data to vector similarity index. Error: {}", result.error.release());
        }

        ProfileEvents::increment(ProfileEvents::USearchAddCount);
        ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, result.visited_members);
        ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, result.computed_distances);
    };

    size_t index_size = index->size();

    for (size_t row = 0; row < rows; ++row)
    {
        auto key = static_cast<USearchIndex::vector_key_t>(index_size + row);
        runner([&add_vector_to_index, key, row] { add_vector_to_index(key, row); });
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array(Float*) column");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected data type Array(Float*)");

    const TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
    if (WhichDataType(nested_type_index).isFloat32())
        updateImpl<ColumnFloat32>(column_array, column_array_offsets, index, dimensions, rows);
    else if (WhichDataType(nested_type_index).isFloat64())
        updateImpl<ColumnFloat64>(column_array, column_array_offsets, index, dimensions, rows);
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
    if (!parameters)
        return true;

    /// The vector similarity index was build on a specific column.
    /// It can only be used if the ORDER BY clause in the SELECT query is against the same column.
    if (parameters->column != index_column)
        return true;

    /// The vector similarity index was build for a specific distance function.
    /// It can only be used if the ORDER BY clause in the SELECT query uses the same distance function.
    if ((parameters->distance_function == "L2Distance" && metric_kind != unum::usearch::metric_kind_t::l2sq_k)
        || (parameters->distance_function == "cosineDistance" && metric_kind != unum::usearch::metric_kind_t::cos_k))
            return true;

    return false;
}

std::vector<UInt64> MergeTreeIndexConditionVectorSimilarity::calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr granule_) const
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

    /// We want to run the search with the user-provided value for setting hnsw_candidate_list_size_for_search (aka. expansion_search).
    /// The way to do this in USearch is to call index_dense_gt::change_expansion_search. Unfortunately, this introduces a need to
    /// synchronize index access, see https://github.com/unum-cloud/usearch/issues/500. As a workaround, we extended USearch' search method
    /// to accept a custom expansion_add setting. The config value is only used on the fly, i.e. not persisted in the index.

    auto search_result = index->search(parameters->reference_vector.data(), parameters->limit, USearchIndex::any_thread(), false, expansion_search);
    if (!search_result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not search in vector similarity index. Error: {}", search_result.error.release());

    std::vector<USearchIndex::vector_key_t> neighbors(search_result.size()); /// indexes of vectors which were closest to the reference vector
    search_result.dump_to(neighbors.data());

    std::sort(neighbors.begin(), neighbors.end());

    /// Duplicates should in theory not be possible but who knows ...
    const bool has_duplicates = std::adjacent_find(neighbors.begin(), neighbors.end()) != neighbors.end();
    if (has_duplicates)
#ifndef NDEBUG
        throw Exception(ErrorCodes::INCORRECT_DATA, "Usearch returned duplicate row numbers");
#else
        neighbors.erase(std::unique(neighbors.begin(), neighbors.end()), neighbors.end());
#endif

    ProfileEvents::increment(ProfileEvents::USearchSearchCount);
    ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, search_result.visited_members);
    ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, search_result.computed_distances);

    return neighbors;
}

MergeTreeIndexVectorSimilarity::MergeTreeIndexVectorSimilarity(
    const IndexDescription & index_,
    UInt64 dimensions_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : IMergeTreeIndex(index_)
    , dimensions(dimensions_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSimilarity::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index.name, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarity::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarity>(index.name, index.sample_block, dimensions, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG * /*filter_actions_dag*/, ContextPtr /*context*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function not supported for vector similarity index");
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG * /*filter_actions_dag*/, ContextPtr context, const std::optional<VectorSearchParameters> & parameters) const
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

    /// Optional parameters:
    const bool has_six_args = (index.arguments.size() == 6);
    if (has_six_args)
    {
        scalar_kind = quantizationToScalarKind.at(index.arguments[3].safeGet<String>());
        usearch_hnsw_params = {.connectivity  = index.arguments[4].safeGet<UInt64>(),
                               .expansion_add = index.arguments[5].safeGet<UInt64>()};
    }

    return std::make_shared<MergeTreeIndexVectorSimilarity>(index, dimensions, metric_kind, scalar_kind, usearch_hnsw_params);
}

void vectorSimilarityIndexValidator(const IndexDescription & index, bool /* attach */)
{
    const bool has_three_args = (index.arguments.size() == 3);
    const bool has_six_args = (index.arguments.size() == 6);

    /// Check number and type of arguments
    if (!has_three_args && !has_six_args)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index must have three or six arguments");
    if (index.arguments[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "First argument of vector similarity index (method) must be of type String");
    if (index.arguments[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (index.arguments[2].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (dimensions) must be of type UInt64");
    if (has_six_args)
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
    if (has_six_args)
    {
        if (!quantizationToScalarKind.contains(index.arguments[3].safeGet<String>()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Fourth argument (quantization) of vector similarity index is not supported. Supported quantizations are: {}", joinByComma(quantizationToScalarKind));

        /// Call Usearch's own parameter validation method for HNSW-specific parameters
        UInt64 connectivity = index.arguments[4].safeGet<UInt64>();
        UInt64 expansion_add = index.arguments[5].safeGet<UInt64>();
        UInt64 expansion_search = default_expansion_search;
        unum::usearch::index_dense_config_t config(connectivity, expansion_add, expansion_search);
        if (auto error = config.validate(); error)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parameters passed to vector similarity index. Error: {}", error.release());
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
    if (!WhichDataType(nested_type_index).isNativeFloat())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector similarity indexes can only be created on columns of type Array(Float*)");
}

}

#endif
