#ifdef ENABLE_USEARCH

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpass-failed"

#include <Storages/MergeTree/MergeTreeIndexUSearch.h>

#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
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
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

std::unordered_map<String, unum::usearch::scalar_kind_t> nameToScalarKind = {
    {"f64", unum::usearch::scalar_kind_t::f64_k},
    {"f32", unum::usearch::scalar_kind_t::f32_k},
    {"f16", unum::usearch::scalar_kind_t::f16_k},
    {"i8", unum::usearch::scalar_kind_t::i8_k}};

}

template <unum::usearch::metric_kind_t Metric>
USearchIndexWithSerialization<Metric>::USearchIndexWithSerialization(size_t dimensions, unum::usearch::scalar_kind_t scalar_kind)
    : Base(Base::make(unum::usearch::metric_punned_t(dimensions, Metric, scalar_kind)))
{
}

template <unum::usearch::metric_kind_t Metric>
void USearchIndexWithSerialization<Metric>::serialize(WriteBuffer & ostr) const
{
    auto callback = [&ostr](void * from, size_t n)
    {
        ostr.write(reinterpret_cast<const char *>(from), n);
        return true;
    };

    Base::save_to_stream(callback);
}

template <unum::usearch::metric_kind_t Metric>
void USearchIndexWithSerialization<Metric>::deserialize(ReadBuffer & istr)
{
    auto callback = [&istr](void * from, size_t n)
    {
        istr.readStrict(reinterpret_cast<char *>(from), n);
        return true;
    };

    Base::load_from_stream(callback);
}

template <unum::usearch::metric_kind_t Metric>
size_t USearchIndexWithSerialization<Metric>::getDimensions() const
{
    return Base::dimensions();
}

template <unum::usearch::metric_kind_t Metric>
MergeTreeIndexGranuleUSearch<Metric>::MergeTreeIndexGranuleUSearch(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::scalar_kind_t scalar_kind_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , scalar_kind(scalar_kind_)
    , index(nullptr)
{
}

template <unum::usearch::metric_kind_t Metric>
MergeTreeIndexGranuleUSearch<Metric>::MergeTreeIndexGranuleUSearch(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::scalar_kind_t scalar_kind_,
    USearchIndexWithSerializationPtr<Metric> index_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , scalar_kind(scalar_kind_)
    , index(std::move(index_))
{
}

template <unum::usearch::metric_kind_t Metric>
void MergeTreeIndexGranuleUSearch<Metric>::serializeBinary(WriteBuffer & ostr) const
{
    /// Number of dimensions is required in the index constructor,
    /// so it must be written and read separately from the other part
    writeIntBinary(static_cast<UInt64>(index->getDimensions()), ostr); // write dimension
    index->serialize(ostr);
}

template <unum::usearch::metric_kind_t Metric>
void MergeTreeIndexGranuleUSearch<Metric>::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt64 dimension;
    readIntBinary(dimension, istr);
    index = std::make_shared<USearchIndexWithSerialization<Metric>>(dimension, scalar_kind);
    index->deserialize(istr);
}

template <unum::usearch::metric_kind_t Metric>
MergeTreeIndexAggregatorUSearch<Metric>::MergeTreeIndexAggregatorUSearch(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::scalar_kind_t scalar_kind_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , scalar_kind(scalar_kind_)
{
}

template <unum::usearch::metric_kind_t Metric>
MergeTreeIndexGranulePtr MergeTreeIndexAggregatorUSearch<Metric>::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleUSearch<Metric>>(index_name, index_sample_block, scalar_kind, index);
    index = nullptr;
    return granule;
}

template <unum::usearch::metric_kind_t Metric>
void MergeTreeIndexAggregatorUSearch<Metric>::update(const Block & block, size_t * pos, size_t limit)
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

    if (rows_read > std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Index granularity is too big: more than 4B rows per index granule.");

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
        size_t dimension = column_array_offsets[0];
        for (size_t i = 0; i < num_rows - 1; ++i)
            if (column_array_offsets[i + 1] - column_array_offsets[i] != dimension)
                throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column '{}' must have equal length", index_column_name);

        /// Also check that previously inserted blocks have the same size as this block.
        /// Note that this guarantees consistency of dimension only within parts. We are unable to detect inconsistent dimensions across
        /// parts - for this, a little help from the user is needed, e.g. CONSTRAINT cnstr CHECK length(array) = 42.
        if (index && index->getDimensions() != dimension)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column '{}' must have equal length", index_column_name);

        if (!index)
            index = std::make_shared<USearchIndexWithSerialization<Metric>>(dimension, scalar_kind);

        /// Add all rows of block
        if (!index->reserve(unum::usearch::ceil2(index->size() + num_rows)))
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for usearch index");

        for (size_t current_row = 0; current_row < num_rows; ++current_row)
        {
            auto rc = index->add(static_cast<uint32_t>(index->size()), &column_array_data_float_data[column_array_offsets[current_row - 1]]);
            if (!rc)
                throw Exception::createRuntime(ErrorCodes::INCORRECT_DATA, rc.error.release());

            ProfileEvents::increment(ProfileEvents::USearchAddCount);
            ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, rc.visited_members);
            ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, rc.computed_distances);
        }
    }
    else if (const auto & column_tuple = typeid_cast<const ColumnTuple *>(column_cut.get()))
    {
        const auto & column_tuple_columns = column_tuple->getColumns();
        std::vector<std::vector<Float32>> data(column_tuple->size(), std::vector<Float32>());
        for (const auto & column : column_tuple_columns)
        {
            const auto & pod_array = typeid_cast<const ColumnFloat32 *>(column.get())->getData();
            for (size_t i = 0; i < pod_array.size(); ++i)
                data[i].push_back(pod_array[i]);
        }

        if (data.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tuple has 0 rows, {} rows expected", rows_read);

        if (!index)
            index = std::make_shared<USearchIndexWithSerialization<Metric>>(data[0].size(), scalar_kind);

        if (!index->reserve(unum::usearch::ceil2(index->size() + data.size())))
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for usearch index");

        for (const auto & item : data)
        {
            auto rc = index->add(static_cast<uint32_t>(index->size()), item.data());
            if (!rc)
                throw Exception::createRuntime(ErrorCodes::INCORRECT_DATA, rc.error.release());

            ProfileEvents::increment(ProfileEvents::USearchAddCount);
            ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, rc.visited_members);
            ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, rc.computed_distances);
        }
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array or Tuple column");

    *pos += rows_read;
}

MergeTreeIndexConditionUSearch::MergeTreeIndexConditionUSearch(
    const IndexDescription & /*index_description*/,
    const SelectQueryInfo & query,
    const String & distance_function_,
    ContextPtr context)
    : ann_condition(query, context)
    , distance_function(distance_function_)
{
}

bool MergeTreeIndexConditionUSearch::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /*idx_granule*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for ANN skip indexes");
}

bool MergeTreeIndexConditionUSearch::alwaysUnknownOrTrue() const
{
    return ann_condition.alwaysUnknownOrTrue(distance_function);
}

std::vector<size_t> MergeTreeIndexConditionUSearch::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    if (distance_function == DISTANCE_FUNCTION_L2)
        return getUsefulRangesImpl<unum::usearch::metric_kind_t::l2sq_k>(idx_granule);
    else if (distance_function == DISTANCE_FUNCTION_COSINE)
        return getUsefulRangesImpl<unum::usearch::metric_kind_t::cos_k>(idx_granule);
    std::unreachable();
}

template <unum::usearch::metric_kind_t Metric>
std::vector<size_t> MergeTreeIndexConditionUSearch::getUsefulRangesImpl(MergeTreeIndexGranulePtr idx_granule) const
{
    const UInt64 limit = ann_condition.getLimit();
    const UInt64 index_granularity = ann_condition.getIndexGranularity();
    const std::optional<float> comparison_distance = ann_condition.getQueryType() == ApproximateNearestNeighborInformation::Type::Where
        ? std::optional<float>(ann_condition.getComparisonDistanceForWhereQuery())
        : std::nullopt;

    if (comparison_distance && comparison_distance.value() < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to optimize query with where without distance");

    const std::vector<float> reference_vector = ann_condition.getReferenceVector();

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleUSearch<Metric>>(idx_granule);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    const USearchIndexWithSerializationPtr<Metric> index = granule->index;

    if (ann_condition.getDimensions() != index->dimensions())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the space in the request ({}) "
            "does not match the dimension in the index ({})",
            ann_condition.getDimensions(), index->dimensions());

    auto result = index->search(reference_vector.data(), limit);

    ProfileEvents::increment(ProfileEvents::USearchSearchCount);
    ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, result.visited_members);
    ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, result.computed_distances);

    std::vector<UInt32> neighbors(result.size()); /// indexes of dots which were closest to the reference vector
    std::vector<Float32> distances(result.size());
    result.dump_to(neighbors.data(), distances.data());

    std::vector<size_t> granules;
    granules.reserve(neighbors.size());
    for (size_t i = 0; i < neighbors.size(); ++i)
    {
        if (comparison_distance && distances[i] > comparison_distance)
            continue;
        granules.push_back(neighbors[i] / index_granularity);
    }

    /// make unique
    std::sort(granules.begin(), granules.end());
    granules.erase(std::unique(granules.begin(), granules.end()), granules.end());

    return granules;
}

MergeTreeIndexUSearch::MergeTreeIndexUSearch(const IndexDescription & index_, const String & distance_function_, unum::usearch::scalar_kind_t scalar_kind_)
    : IMergeTreeIndex(index_)
    , distance_function(distance_function_)
    , scalar_kind(scalar_kind_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexUSearch::createIndexGranule() const
{
    if (distance_function == DISTANCE_FUNCTION_L2)
        return std::make_shared<MergeTreeIndexGranuleUSearch<unum::usearch::metric_kind_t::l2sq_k>>(index.name, index.sample_block, scalar_kind);
    else if (distance_function == DISTANCE_FUNCTION_COSINE)
        return std::make_shared<MergeTreeIndexGranuleUSearch<unum::usearch::metric_kind_t::cos_k>>(index.name, index.sample_block, scalar_kind);
    std::unreachable();
}

MergeTreeIndexAggregatorPtr MergeTreeIndexUSearch::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    if (distance_function == DISTANCE_FUNCTION_L2)
        return std::make_shared<MergeTreeIndexAggregatorUSearch<unum::usearch::metric_kind_t::l2sq_k>>(index.name, index.sample_block, scalar_kind);
    else if (distance_function == DISTANCE_FUNCTION_COSINE)
        return std::make_shared<MergeTreeIndexAggregatorUSearch<unum::usearch::metric_kind_t::cos_k>>(index.name, index.sample_block, scalar_kind);
    std::unreachable();
}

MergeTreeIndexConditionPtr MergeTreeIndexUSearch::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionUSearch>(index, query, distance_function, context);
};

MergeTreeIndexConditionPtr MergeTreeIndexUSearch::createIndexCondition(const ActionsDAGPtr &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeTreeIndexAnnoy cannot be created with ActionsDAG");
}

MergeTreeIndexPtr usearchIndexCreator(const IndexDescription & index)
{
    static constexpr auto default_distance_function = DISTANCE_FUNCTION_L2;
    String distance_function = default_distance_function;
    if (!index.arguments.empty())
        distance_function = index.arguments[0].get<String>();

    static constexpr auto default_scalar_kind = unum::usearch::scalar_kind_t::f16_k;
    auto scalar_kind = default_scalar_kind;
    if (index.arguments.size() > 1)
        scalar_kind = nameToScalarKind.at(index.arguments[1].get<String>());

    return std::make_shared<MergeTreeIndexUSearch>(index, distance_function, scalar_kind);
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

    if (!index.arguments.empty())
    {
        String distance_name = index.arguments[0].get<String>();
        if (distance_name != DISTANCE_FUNCTION_L2 && distance_name != DISTANCE_FUNCTION_COSINE)
            throw Exception(ErrorCodes::INCORRECT_DATA, "USearch index only supports distance functions '{}' and '{}'", DISTANCE_FUNCTION_L2, DISTANCE_FUNCTION_COSINE);
    }

    /// Check that a supported kind was passed as a second argument

    if (index.arguments.size() > 1 && !nameToScalarKind.contains(index.arguments[1].get<String>()))
    {
        String supported_kinds;
        for (const auto & [name, kind] : nameToScalarKind)
        {
            if (!supported_kinds.empty())
                supported_kinds += ", ";
            supported_kinds += name;
        }
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unrecognized scalar kind (second argument) for USearch index. Supported kinds are: {}", supported_kinds);
    }

    /// Check data type of indexed column:

    auto throw_unsupported_underlying_column_exception = []()
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "USearch can only be created on columns of type Array(Float32) and Tuple(Float32[, Float32[, ...]])");
    };

    DataTypePtr data_type = index.sample_block.getDataTypes()[0];

    if (const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
        if (!WhichDataType(nested_type_index).isFloat32())
            throw_unsupported_underlying_column_exception();
    }
    else if (const auto * data_type_tuple = typeid_cast<const DataTypeTuple *>(data_type.get()))
    {
        const DataTypes & inner_types = data_type_tuple->getElements();
        for (const auto & inner_type : inner_types)
        {
            TypeIndex nested_type_index = inner_type->getTypeId();
            if (!WhichDataType(nested_type_index).isFloat32())
                throw_unsupported_underlying_column_exception();
        }
    }
    else
        throw_unsupported_underlying_column_exception();
}

}

#endif
