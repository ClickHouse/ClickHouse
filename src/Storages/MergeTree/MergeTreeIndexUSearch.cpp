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
}

template <unum::usearch::metric_kind_t Metric>
USearchIndexWithSerialization<Metric>::USearchIndexWithSerialization(size_t dimensions)
    : Base(Base::make(unum::usearch::metric_punned_t(dimensions, Metric)))
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
    const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index(nullptr)
{
}

template <unum::usearch::metric_kind_t Metric>
MergeTreeIndexGranuleUSearch<Metric>::MergeTreeIndexGranuleUSearch(
    const String & index_name_,
    const Block & index_sample_block_,
    USearchIndexWithSerializationPtr<Metric> index_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
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
    index = std::make_shared<USearchIndexWithSerialization<Metric>>(dimension);
    index->deserialize(istr);
}

template <unum::usearch::metric_kind_t Metric>
MergeTreeIndexAggregatorUSearch<Metric>::MergeTreeIndexAggregatorUSearch(
    const String & index_name_,
    const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{
}

template <unum::usearch::metric_kind_t Metric>
MergeTreeIndexGranulePtr MergeTreeIndexAggregatorUSearch<Metric>::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleUSearch<Metric>>(index_name, index_sample_block, index);
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

    if (index_sample_block.columns() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected block with single column");

    const String & index_column_name = index_sample_block.getByPosition(0).name;
    ColumnPtr column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);

    if (const auto & column_array = typeid_cast<const ColumnArray *>(column_cut.get()))
    {
        const auto & data = column_array->getData();
        const auto & array = typeid_cast<const ColumnFloat32 &>(data).getData();

        if (array.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array has 0 rows, {} rows expected", rows_read);

        const auto & offsets = column_array->getOffsets();
        const size_t num_rows = offsets.size();


        /// Check all sizes are the same
        size_t size = offsets[0];
        for (size_t i = 0; i < num_rows - 1; ++i)
            if (offsets[i + 1] - offsets[i] != size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column {} must have equal length", index_column_name);

        if (!index)
            index = std::make_shared<USearchIndexWithSerialization<Metric>>(size);

        /// Add all rows of block
        if (!index->reserve(unum::usearch::ceil2(index->size() + num_rows)))
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for usearch index");

        if (auto rc = index->add(index->size(), array.data()); !rc)
            throw Exception(ErrorCodes::INCORRECT_DATA, rc.error.release());
        for (size_t current_row = 1; current_row < num_rows; ++current_row)
            if (auto rc = index->add(index->size(), &array[offsets[current_row - 1]]); !rc)
                throw Exception(ErrorCodes::INCORRECT_DATA, rc.error.release());

    }
    else if (const auto & column_tuple = typeid_cast<const ColumnTuple *>(column_cut.get()))
    {
        const auto & columns = column_tuple->getColumns();
        std::vector<std::vector<Float32>> data{column_tuple->size(), std::vector<Float32>()};
        for (const auto & column : columns)
        {
            const auto & pod_array = typeid_cast<const ColumnFloat32 *>(column.get())->getData();
            for (size_t i = 0; i < pod_array.size(); ++i)
                data[i].push_back(pod_array[i]);
        }

        if (data.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tuple has 0 rows, {} rows expected", rows_read);

        if (!index)
            index = std::make_shared<USearchIndexWithSerialization<Metric>>(data[0].size());

        if (!index->reserve(unum::usearch::ceil2(index->size() + data.size())))
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for usearch index");

        for (const auto & item : data)
            if (auto rc = index->add(index->size(), item.data()); !rc)
                throw Exception(ErrorCodes::INCORRECT_DATA, rc.error.release());
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
    std::vector<UInt64> neighbors(result.size()); /// indexes of dots which were closest to the reference vector
    std::vector<Float32> distances(result.size());
    result.dump_to(neighbors.data(), distances.data());

    std::vector<size_t> granule_numbers;
    granule_numbers.reserve(neighbors.size());
    for (size_t i = 0; i < neighbors.size(); ++i)
    {
        if (comparison_distance && distances[i] > comparison_distance)
            continue;
        granule_numbers.push_back(neighbors[i] / index_granularity);
    }

    /// make unique
    std::sort(granule_numbers.begin(), granule_numbers.end());
    granule_numbers.erase(std::unique(granule_numbers.begin(), granule_numbers.end()), granule_numbers.end());

    return granule_numbers;
}

MergeTreeIndexUSearch::MergeTreeIndexUSearch(const IndexDescription & index_, const String & distance_function_)
    : IMergeTreeIndex(index_)
    , distance_function(distance_function_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexUSearch::createIndexGranule() const
{
    if (distance_function == DISTANCE_FUNCTION_L2)
        return std::make_shared<MergeTreeIndexGranuleUSearch<unum::usearch::metric_kind_t::l2sq_k>>(index.name, index.sample_block);
    else if (distance_function == DISTANCE_FUNCTION_COSINE)
        return std::make_shared<MergeTreeIndexGranuleUSearch<unum::usearch::metric_kind_t::cos_k>>(index.name, index.sample_block);
    std::unreachable();
}

MergeTreeIndexAggregatorPtr MergeTreeIndexUSearch::createIndexAggregator() const
{
    if (distance_function == DISTANCE_FUNCTION_L2)
        return std::make_shared<MergeTreeIndexAggregatorUSearch<unum::usearch::metric_kind_t::l2sq_k>>(index.name, index.sample_block);
    else if (distance_function == DISTANCE_FUNCTION_COSINE)
        return std::make_shared<MergeTreeIndexAggregatorUSearch<unum::usearch::metric_kind_t::cos_k>>(index.name, index.sample_block);
    std::unreachable();
}

MergeTreeIndexConditionPtr MergeTreeIndexUSearch::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionUSearch>(index, query, distance_function, context);
};

MergeTreeIndexPtr usearchIndexCreator(const IndexDescription & index)
{
    static constexpr auto default_distance_function = DISTANCE_FUNCTION_L2;
    String distance_function = default_distance_function;
    if (!index.arguments.empty())
        distance_function = index.arguments[0].get<String>();

    return std::make_shared<MergeTreeIndexUSearch>(index, distance_function);
}

void usearchIndexValidator(const IndexDescription & index, bool /* attach */)
{
    /// Check number and type of USearch index arguments:

    if (index.arguments.size() > 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "USearch index must not have more than one parameters");

    if (!index.arguments.empty() && index.arguments[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Distance function argument of USearch index must be of type String");

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

    /// Check data type of indexed column:

    auto throw_unsupported_underlying_column_exception = []()
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "USearch indexes can only be created on columns of type Array(Float32) and Tuple(Float32)");
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
