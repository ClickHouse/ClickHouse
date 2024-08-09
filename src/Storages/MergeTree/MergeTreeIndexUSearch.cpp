#include <Storages/MergeTree/MergeTreeIndexUSearch.h>

#if USE_USEARCH

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpass-failed"

#include <Columns/ColumnArray.h>
#include <Common/BitHelpers.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
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

/// Maps from user-facing name to internal name
std::unordered_map<String, unum::usearch::metric_kind_t> distanceFunctionToMetricKind = {
    {"L2Distance", unum::usearch::metric_kind_t::l2sq_k},
    {"cosineDistance", unum::usearch::metric_kind_t::cos_k}};

/// Maps from user-facing name to internal name
std::unordered_map<String, unum::usearch::scalar_kind_t> quantizationToScalarKind = {
    {"f64", unum::usearch::scalar_kind_t::f64_k},
    {"f32", unum::usearch::scalar_kind_t::f32_k},
    {"f16", unum::usearch::scalar_kind_t::f16_k},
    {"i8", unum::usearch::scalar_kind_t::i8_k}};

template <typename T>
String keysAsString(const T & t)
{
    String result;
    for (const auto & [k, _] : t)
    {
        if (!result.empty())
            result += ", ";
        result += k;
    }
    return result;
}

}

USearchIndexWithSerialization::USearchIndexWithSerialization(
    size_t dimensions,
    unum::usearch::metric_kind_t metric_kind,
    unum::usearch::scalar_kind_t scalar_kind)
    : Base(Base::make(unum::usearch::metric_punned_t(dimensions, metric_kind, scalar_kind)))
{
}

void USearchIndexWithSerialization::serialize(WriteBuffer & ostr) const
{
    auto callback = [&ostr](void * from, size_t n)
    {
        ostr.write(reinterpret_cast<const char *>(from), n);
        return true;
    };

    auto result = Base::save_to_stream(callback);
    if (result.error)
        throw Exception::createRuntime(ErrorCodes::INCORRECT_DATA, "Could not save USearch index, error: " + String(result.error.release()));
}

void USearchIndexWithSerialization::deserialize(ReadBuffer & istr)
{
    auto callback = [&istr](void * from, size_t n)
    {
        istr.readStrict(reinterpret_cast<char *>(from), n);
        return true;
    };

    auto result = Base::load_from_stream(callback);
    if (result.error)
        /// See the comment in MergeTreeIndexGranuleVectorSimilarity::deserializeBinary why we throw here
        throw Exception::createRuntime(ErrorCodes::INCORRECT_DATA, "Could not load USearch index, error: " + String(result.error.release()) + " Please drop the index and create it again.");
}

USearchIndexWithSerialization::Statistics USearchIndexWithSerialization::getStatistics() const
{
    Statistics statistics = {
        .max_level = max_level(),
        .connectivity = connectivity(),
        .size = size(),                         /// number of vectors
        .capacity = capacity(),                 /// number of vectors reserved
        .memory_usage = memory_usage(),         /// in bytes, the value is not exact
        .bytes_per_vector = bytes_per_vector(),
        .scalar_words = scalar_words(),
        .statistics = stats()};
    return statistics;
}

MergeTreeIndexGranuleUSearch::MergeTreeIndexGranuleUSearch(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_)
    : MergeTreeIndexGranuleUSearch(index_name_, index_sample_block_, metric_kind_, scalar_kind_, nullptr)
{
}

MergeTreeIndexGranuleUSearch::MergeTreeIndexGranuleUSearch(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    USearchIndexWithSerializationPtr index_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , index(std::move(index_))
{
}

void MergeTreeIndexGranuleUSearch::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty minmax index {}", backQuote(index_name));

    writeIntBinary(FILE_FORMAT_VERSION, ostr);

    /// Number of dimensions is required in the index constructor,
    /// so it must be written and read separately from the other part
    writeIntBinary(static_cast<UInt64>(index->dimensions()), ostr); // write dimension
                                                                    //
    index->serialize(ostr);

    auto statistics = index->getStatistics();
    LOG_TRACE(logger, "Wrote USearch index: max_level = {}, connectivity = {}, size = {}, capacity = {}, memory_usage = {}",
                      statistics.max_level, statistics.connectivity, statistics.size, statistics.capacity, ReadableSize(statistics.memory_usage));
}

void MergeTreeIndexGranuleUSearch::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt64 file_version;
    readIntBinary(file_version, istr);
    if (file_version != FILE_FORMAT_VERSION)
        throw Exception(
            ErrorCodes::FORMAT_VERSION_TOO_OLD,
            "USearch index could not be loaded because its version is too old (current version: {}, persisted version: {}). Please drop the index and create it again.",
            FILE_FORMAT_VERSION, file_version);
        /// More fancy error handling would be: Set a flag on the index that it failed to load. During usage return all granules, i.e.
        /// behave as if the index does not exist. Since format changes are expected to happen only rarely and it is "only" an index, keep it simple for now.

    UInt64 dimension;
    readIntBinary(dimension, istr);
    index = std::make_shared<USearchIndexWithSerialization>(dimension, metric_kind, scalar_kind);

    index->deserialize(istr);

    auto statistics = index->getStatistics();
    LOG_TRACE(logger, "Loaded USearch index: max_level = {}, connectivity = {}, size = {}, capacity = {}, memory_usage = {}",
                      statistics.max_level, statistics.connectivity, statistics.size, statistics.capacity, ReadableSize(statistics.memory_usage));
}

MergeTreeIndexAggregatorUSearch::MergeTreeIndexAggregatorUSearch(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorUSearch::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleUSearch>(index_name, index_sample_block, metric_kind, scalar_kind, index);
    index = nullptr;
    return granule;
}

void MergeTreeIndexAggregatorUSearch::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos,
            block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (rows_read == 0)
        return;

    if (rows_read > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Index granularity is too big: more than {} rows per index granule.", std::numeric_limits<UInt32>::max());

    if (index_sample_block.columns() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected block with single column");

    const String & index_column_name = index_sample_block.getByPosition(0).name;
    ColumnPtr column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);

    if (const auto & column_array = typeid_cast<const ColumnArray *>(column_cut.get()))
    {
        const auto & column_array_data = column_array->getData();
        const auto & column_array_data_float = typeid_cast<const ColumnFloat32 &>(column_array_data);
        const auto & column_array_data_float_data = column_array_data_float.getData();

        const auto & column_array_offsets = column_array->getOffsets();
        const size_t num_rows = column_array_offsets.size();

        if (column_array->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array is unexpectedly empty");

        /// The Usearch algorithm naturally assumes that the indexed vectors have dimension >= 1. This condition is violated if empty arrays
        /// are INSERTed into an Usearch-indexed column or if no value was specified at all in which case the arrays take on their default
        /// values which is also empty.
        if (column_array->isDefaultAt(0))
            throw Exception(ErrorCodes::INCORRECT_DATA, "The arrays in column '{}' must not be empty. Did you try to INSERT default values?", index_column_name);

        /// Check all sizes are the same
        const size_t dimensions = column_array_offsets[0];
        for (size_t i = 0; i < num_rows - 1; ++i)
            if (column_array_offsets[i + 1] - column_array_offsets[i] != dimensions)
                throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column '{}' must have equal length", index_column_name);

        /// Also check that previously inserted blocks have the same size as this block.
        /// Note that this guarantees consistency of dimension only within parts. We are unable to detect inconsistent dimensions across
        /// parts - for this, a little help from the user is needed, e.g. CONSTRAINT cnstr CHECK length(array) = 42.
        if (index && index->dimensions() != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column '{}' must have equal length", index_column_name);

        if (!index)
            index = std::make_shared<USearchIndexWithSerialization>(dimensions, metric_kind, scalar_kind);

        /// Reserving space is mandatory
        if (!index->reserve(roundUpToPowerOfTwoOrZero(index->size() + num_rows)))
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for usearch index");

        for (size_t current_row = 0; current_row < num_rows; ++current_row)
        {
            auto rc = index->add(static_cast<UInt32>(index->size()), &column_array_data_float_data[column_array_offsets[current_row - 1]]);
            if (!rc)
                throw Exception::createRuntime(ErrorCodes::INCORRECT_DATA, "Could not add data to USearch index, error: " + String(rc.error.release()));

            ProfileEvents::increment(ProfileEvents::USearchAddCount);
            ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, rc.visited_members);
            ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, rc.computed_distances);
        }
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array(Float32) column");

    *pos += rows_read;
}

MergeTreeIndexConditionUSearch::MergeTreeIndexConditionUSearch(
    const IndexDescription & /*index_description*/,
    const SelectQueryInfo & query,
    unum::usearch::metric_kind_t metric_kind_,
    ContextPtr context)
    : vector_similarity_condition(query, context)
    , metric_kind(metric_kind_)
{
}

bool MergeTreeIndexConditionUSearch::mayBeTrueOnGranule(MergeTreeIndexGranulePtr) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for ANN skip indexes");
}

bool MergeTreeIndexConditionUSearch::alwaysUnknownOrTrue() const
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

std::vector<size_t> MergeTreeIndexConditionUSearch::getUsefulRanges(MergeTreeIndexGranulePtr granule_) const
{
    const UInt64 limit = vector_similarity_condition.getLimit();
    const UInt64 index_granularity = vector_similarity_condition.getIndexGranularity();

    const std::vector<float> reference_vector = vector_similarity_condition.getReferenceVector();

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleUSearch>(granule_);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    const USearchIndexWithSerializationPtr index = granule->index;

    if (vector_similarity_condition.getDimensions() != index->dimensions())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the space in the request ({}) "
            "does not match the dimension in the index ({})",
            vector_similarity_condition.getDimensions(), index->dimensions());

    auto result = index->search(reference_vector.data(), limit);
    if (result.error)
        throw Exception::createRuntime(ErrorCodes::INCORRECT_DATA, "Could not search in USearch index, error: " + String(result.error.release()));

    ProfileEvents::increment(ProfileEvents::USearchSearchCount);
    ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, result.visited_members);
    ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, result.computed_distances);

    std::vector<USearchIndex::key_t> neighbors(result.size()); /// indexes of dots which were closest to the reference vector
    std::vector<USearchIndex::distance_t> distances(result.size());
    result.dump_to(neighbors.data(), distances.data());

    std::vector<size_t> granules;
    granules.reserve(neighbors.size());
    for (auto neighbor : neighbors)
        granules.push_back(neighbor / index_granularity);

    /// make unique
    std::sort(granules.begin(), granules.end());
    granules.erase(std::unique(granules.begin(), granules.end()), granules.end());

    return granules;
}

MergeTreeIndexUSearch::MergeTreeIndexUSearch(const IndexDescription & index_, unum::usearch::metric_kind_t metric_kind_, unum::usearch::scalar_kind_t scalar_kind_)
    : IMergeTreeIndex(index_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexUSearch::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleUSearch>(index.name, index.sample_block, metric_kind, scalar_kind);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexUSearch::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorUSearch>(index.name, index.sample_block, metric_kind, scalar_kind);
}

MergeTreeIndexConditionPtr MergeTreeIndexUSearch::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionUSearch>(index, query, metric_kind, context);
};

MergeTreeIndexConditionPtr MergeTreeIndexUSearch::createIndexCondition(const ActionsDAG *, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeTreeIndexAnnoy cannot be created with ActionsDAG");
}

MergeTreeIndexPtr usearchIndexCreator(const IndexDescription & index)
{
    static constexpr auto default_metric_kind = unum::usearch::metric_kind_t::l2sq_k;
    auto metric_kind = default_metric_kind;
    if (!index.arguments.empty())
        metric_kind = distanceFunctionToMetricKind.at(index.arguments[0].safeGet<String>());

    static constexpr auto default_scalar_kind = unum::usearch::scalar_kind_t::f16_k;
    auto scalar_kind = default_scalar_kind;
    if (index.arguments.size() > 1)
        scalar_kind = quantizationToScalarKind.at(index.arguments[1].safeGet<String>());

    return std::make_shared<MergeTreeIndexUSearch>(index, metric_kind, scalar_kind);
}

void usearchIndexValidator(const IndexDescription & index, bool /* attach */)
{
    /// Check number and type of USearch index arguments:

    if (index.arguments.size() > 2)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "USearch index must not have more than one parameters");

    if (!index.arguments.empty() && index.arguments[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "First argument of USearch index (distance function) must be of type String");
    if (index.arguments.size() > 1 && index.arguments[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of USearch index (scalar type) must be of type String");

    /// Check that the index is created on a single column

    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "USearch indexes must be created on a single column");

    /// Check that a supported metric was passed as first argument

    if (!index.arguments.empty() && !distanceFunctionToMetricKind.contains(index.arguments[0].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unrecognized metric kind (first argument) for vector index. Supported kinds are: {}", keysAsString(distanceFunctionToMetricKind));

    /// Check that a supported kind was passed as a second argument

    if (index.arguments.size() > 1 && !quantizationToScalarKind.contains(index.arguments[1].safeGet<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unrecognized scalar kind (second argument) for vector index. Supported kinds are: {}", keysAsString(quantizationToScalarKind));

    /// Check data type of indexed column:

    DataTypePtr data_type = index.sample_block.getDataTypes()[0];
    if (const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
        if (!WhichDataType(nested_type_index).isFloat32())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "USearch can only be created on columns of type Array(Float32)");
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "USearch can only be created on columns of type Array(Float32)");
}

}

#endif
