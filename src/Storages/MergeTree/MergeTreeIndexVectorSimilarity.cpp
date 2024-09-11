#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>

#if USE_USEARCH

#include <Columns/ColumnArray.h>
#include <Common/BitHelpers.h>
#include <Common/ThreadPool.h>
#include <Common/formatReadable.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
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

namespace CurrentMetrics
{
extern const Metric UsearchUpdateThreads;
extern const Metric UsearchUpdateThreadsActive;
extern const Metric UsearchUpdateThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
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

    unum::usearch::index_dense_config_t config(usearch_hnsw_params.m, usearch_hnsw_params.ef_construction, usearch_hnsw_params.ef_search);
    config.enable_key_lookups = false; /// we don't do row-to-vector lookups

    if (auto result = USearchIndex::make(metric, config); !result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not create vector similarity index. Error: {}", String(result.error.release()));
    else
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

    if (!try_reserve(limits()))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for usearch index");
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
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : MergeTreeIndexGranuleVectorSimilarity(index_name_, index_sample_block_, metric_kind_, scalar_kind_, usearch_hnsw_params_, nullptr)
{
}

MergeTreeIndexGranuleVectorSimilarity::MergeTreeIndexGranuleVectorSimilarity(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_,
    USearchIndexWithSerializationPtr index_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
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

    UInt64 dimension;
    readIntBinary(dimension, istr);
    index = std::make_shared<USearchIndexWithSerialization>(dimension, metric_kind, scalar_kind, usearch_hnsw_params);

    index->deserialize(istr);

    auto statistics = index->getStatistics();
    LOG_TRACE(logger, "Loaded vector similarity index: {}", statistics.toString());
}

MergeTreeIndexAggregatorVectorSimilarity::MergeTreeIndexAggregatorVectorSimilarity(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSimilarity::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index_name, index_sample_block, metric_kind, scalar_kind, usearch_hnsw_params, index);
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
    if (!index->try_reserve(roundUpToPowerOfTwoOrZero(index->size() + rows)))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for vector similarity index");

    size_t max_threads = Context::getGlobalContextInstance()->getSettingsRef().max_threads;
    max_threads = max_threads > 0 ? max_threads : getNumberOfPhysicalCPUCores();

    auto thread_pool = std::make_unique<ThreadPool>(
        CurrentMetrics::UsearchUpdateThreads,
        CurrentMetrics::UsearchUpdateThreadsActive,
        CurrentMetrics::UsearchUpdateThreadsScheduled,
        max_threads);

    auto add_vector_to_index = [&](USearchIndex::vector_key_t key, size_t row, ThreadGroupPtr thread_group)
    {
        SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););

        if (thread_group)
            CurrentThread::attachToGroupIfDetached(thread_group);

        if (auto result = index->add(key, &column_array_data_float_data[column_array_offsets[row - 1]]); !result)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Could not add data to vector similarity index. Error: {}", String(result.error.release()));
        else
        {
            ProfileEvents::increment(ProfileEvents::USearchAddCount);
            ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, result.visited_members);
            ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, result.computed_distances);
        }
    };

    size_t current_index_size = index->size();

    for (size_t row = 0; row < rows; ++row)
    {
        auto key = static_cast<USearchIndex::vector_key_t>(current_index_size + row);
        auto task = [group = CurrentThread::getGroup(), &add_vector_to_index, key, row] { add_vector_to_index(key, row, group); };
        thread_pool->scheduleOrThrowOnError(task);
    }
    thread_pool->wait();
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected block with single column");

    const String & index_column_name = index_sample_block.getByPosition(0).name;
    const ColumnPtr & index_column = block.getByName(index_column_name).column;
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

    DataTypePtr data_type = block.getDataTypes()[0];
    const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
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
    const IndexDescription & /*index_description*/,
    const SelectQueryInfo & query,
    unum::usearch::metric_kind_t metric_kind_,
    ContextPtr context)
    : vector_similarity_condition(query, context)
    , metric_kind(metric_kind_)
{
}

bool MergeTreeIndexConditionVectorSimilarity::mayBeTrueOnGranule(MergeTreeIndexGranulePtr) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for ANN skip indexes");
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

std::vector<size_t> MergeTreeIndexConditionVectorSimilarity::getUsefulRanges(MergeTreeIndexGranulePtr granule_) const
{
    const UInt64 limit = vector_similarity_condition.getLimit();
    const UInt64 index_granularity = vector_similarity_condition.getIndexGranularity();

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleVectorSimilarity>(granule_);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    const USearchIndexWithSerializationPtr index = granule->index;

    if (vector_similarity_condition.getDimensions() != index->dimensions())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the space in the request ({}) "
            "does not match the dimension in the index ({})",
            vector_similarity_condition.getDimensions(), index->dimensions());

    const std::vector<Float64> reference_vector = vector_similarity_condition.getReferenceVector();

    auto search_result = index->search(reference_vector.data(), limit);
    if (!search_result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Could not search in vector similarity index. Error: {}", String(search_result.error.release()));

    ProfileEvents::increment(ProfileEvents::USearchSearchCount);
    ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, search_result.visited_members);
    ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, search_result.computed_distances);

    std::vector<USearchIndex::vector_key_t> neighbors(search_result.size()); /// indexes of vectors which were closest to the reference vector
    search_result.dump_to(neighbors.data());

    std::vector<size_t> granules;
    granules.reserve(neighbors.size());
    for (auto neighbor : neighbors)
        granules.push_back(neighbor / index_granularity);

    /// make unique
    std::sort(granules.begin(), granules.end());
    granules.erase(std::unique(granules.begin(), granules.end()), granules.end());

    return granules;
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
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index.name, index.sample_block, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarity::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarity>(index.name, index.sample_block, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionVectorSimilarity>(index, query, metric_kind, context);
};

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAG *, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeTreeIndexAnnoy cannot be created with ActionsDAG");
}

MergeTreeIndexPtr vectorSimilarityIndexCreator(const IndexDescription & index)
{
    /// Default parameters:
    unum::usearch::metric_kind_t metric_kind = distanceFunctionToMetricKind.at(index.arguments[1].safeGet<String>());
    unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::bf16_k;
    UsearchHnswParams usearch_hnsw_params;

    /// Optional parameters:
    const bool has_six_args = (index.arguments.size() == 6);
    if (has_six_args)
    {
        scalar_kind = quantizationToScalarKind.at(index.arguments[2].safeGet<String>());
        usearch_hnsw_params = {.m               = index.arguments[3].safeGet<UInt64>(),
                               .ef_construction = index.arguments[4].safeGet<UInt64>(),
                               .ef_search       = index.arguments[5].safeGet<UInt64>()};
    }

    return std::make_shared<MergeTreeIndexVectorSimilarity>(index, metric_kind, scalar_kind, usearch_hnsw_params);
}

void vectorSimilarityIndexValidator(const IndexDescription & index, bool /* attach */)
{
    const bool has_two_args = (index.arguments.size() == 2);
    const bool has_six_args = (index.arguments.size() == 6);

    /// Check number and type of arguments
    if (!has_two_args && !has_six_args)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector similarity index must have two or six arguments");
    if (index.arguments[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "First argument of vector similarity index (method) must be of type String");
    if (index.arguments[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector similarity index (metric) must be of type String");
    if (has_six_args)
    {
        if (index.arguments[2].getType() != Field::Types::String)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector similarity index (quantization) must be of type String");
        if (index.arguments[3].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fourth argument of vector similarity index (M) must be of type UInt64");
        if (index.arguments[4].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fifth argument of vector similarity index (ef_construction) must be of type UInt64");
        if (index.arguments[5].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Sixth argument of vector similarity index (ef_search) must be of type UInt64");
    }

    /// Check that passed arguments are supported
    if (!methods.contains(index.arguments[0].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "First argument (method) of vector similarity index is not supported. Supported methods are: {}", joinByComma(methods));
    if (!distanceFunctionToMetricKind.contains(index.arguments[1].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Second argument (distance function) of vector similarity index is not supported. Supported distance function are: {}", joinByComma(distanceFunctionToMetricKind));
    if (has_six_args)
    {
        if (!quantizationToScalarKind.contains(index.arguments[2].safeGet<String>()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (quantization) of vector similarity index is not supported. Supported quantizations are: {}", joinByComma(quantizationToScalarKind));

        /// Call Usearch's own parameter validation method for HNSW-specific parameters
        UInt64 m = index.arguments[3].safeGet<UInt64>();
        UInt64 ef_construction = index.arguments[4].safeGet<UInt64>();
        UInt64 ef_search = index.arguments[5].safeGet<UInt64>();

        unum::usearch::index_dense_config_t config(m, ef_construction, ef_search);

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
