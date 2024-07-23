#ifdef ENABLE_ANNOY

#include <Storages/MergeTree/MergeTreeIndexAnnoy.h>

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
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

template <typename Distance>
AnnoyIndexWithSerialization<Distance>::AnnoyIndexWithSerialization(size_t dimensions)
    : Base::AnnoyIndex(static_cast<int>(dimensions))
{
}

template<typename Distance>
void AnnoyIndexWithSerialization<Distance>::serialize(WriteBuffer & ostr) const
{
    chassert(Base::_built);
    writeIntBinary(Base::_s, ostr);
    writeIntBinary(Base::_n_items, ostr);
    writeIntBinary(Base::_n_nodes, ostr);
    writeIntBinary(Base::_nodes_size, ostr);
    writeIntBinary(Base::_K, ostr);
    writeIntBinary(Base::_seed, ostr);
    writeVectorBinary(Base::_roots, ostr);
    ostr.write(reinterpret_cast<const char *>(Base::_nodes), Base::_s * Base::_n_nodes);
}

template<typename Distance>
void AnnoyIndexWithSerialization<Distance>::deserialize(ReadBuffer & istr)
{
    chassert(!Base::_built);
    readIntBinary(Base::_s, istr);
    readIntBinary(Base::_n_items, istr);
    readIntBinary(Base::_n_nodes, istr);
    readIntBinary(Base::_nodes_size, istr);
    readIntBinary(Base::_K, istr);
    readIntBinary(Base::_seed, istr);
    readVectorBinary(Base::_roots, istr);
    Base::_nodes = realloc(Base::_nodes, Base::_s * Base::_n_nodes);
    istr.readStrict(reinterpret_cast<char *>(Base::_nodes), Base::_s * Base::_n_nodes);

    Base::_fd = 0;
    // set flags
    Base::_loaded = false;
    Base::_verbose = false;
    Base::_on_disk = false;
    Base::_built = true;
}

template<typename Distance>
size_t AnnoyIndexWithSerialization<Distance>::getDimensions() const
{
    return Base::get_f();
}


template <typename Distance>
MergeTreeIndexGranuleAnnoy<Distance>::MergeTreeIndexGranuleAnnoy(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index(nullptr)
{}

template <typename Distance>
MergeTreeIndexGranuleAnnoy<Distance>::MergeTreeIndexGranuleAnnoy(
    const String & index_name_,
    const Block & index_sample_block_,
    AnnoyIndexWithSerializationPtr<Distance> index_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index(std::move(index_))
{}

template <typename Distance>
void MergeTreeIndexGranuleAnnoy<Distance>::serializeBinary(WriteBuffer & ostr) const
{
    /// Number of dimensions is required in the index constructor,
    /// so it must be written and read separately from the other part
    writeIntBinary(static_cast<UInt64>(index->getDimensions()), ostr); // write dimension
    index->serialize(ostr);
}

template <typename Distance>
void MergeTreeIndexGranuleAnnoy<Distance>::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt64 dimension;
    readIntBinary(dimension, istr);
    index = std::make_shared<AnnoyIndexWithSerialization<Distance>>(dimension);
    index->deserialize(istr);
}

template <typename Distance>
MergeTreeIndexAggregatorAnnoy<Distance>::MergeTreeIndexAggregatorAnnoy(
    const String & index_name_,
    const Block & index_sample_block_,
    UInt64 trees_,
    size_t max_threads_for_creation_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , trees(trees_)
    , max_threads_for_creation(max_threads_for_creation_)
{}

template <typename Distance>
MergeTreeIndexGranulePtr MergeTreeIndexAggregatorAnnoy<Distance>::getGranuleAndReset()
{
    int threads = (max_threads_for_creation == 0) ? -1 : static_cast<int>(max_threads_for_creation);
    /// clang-tidy reports a false positive: it considers %p with an outdated pointer in fprintf() (used by logging which we don't do) dereferencing
    index->build(static_cast<int>(trees), threads);
    auto granule = std::make_shared<MergeTreeIndexGranuleAnnoy<Distance>>(index_name, index_sample_block, index);
    index = nullptr;
    return granule;
}

template <typename Distance>
void MergeTreeIndexAggregatorAnnoy<Distance>::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos, block.rows());

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

        /// The Annoy algorithm naturally assumes that the indexed vectors have dimension >= 1. This condition is violated if empty arrays
        /// are INSERTed into an Annoy-indexed column or if no value was specified at all in which case the arrays take on their default
        /// value which is also empty.
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
            index = std::make_shared<AnnoyIndexWithSerialization<Distance>>(dimension);

        /// Add all rows of block
        index->add_item(index->get_n_items(), column_array_data_float_data.data());
        for (size_t current_row = 1; current_row < num_rows; ++current_row)
            index->add_item(index->get_n_items(), &column_array_data_float_data[column_array_offsets[current_row - 1]]);
    }
    else if (const auto & column_tuple = typeid_cast<const ColumnTuple *>(column_cut.get()))
    {
        const auto & column_tuple_columns = column_tuple->getColumns();

        /// TODO check if calling index->add_item() directly on the block's tuples is faster than materializing everything
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
            index = std::make_shared<AnnoyIndexWithSerialization<Distance>>(data[0].size());

        for (const auto & item : data)
            index->add_item(index->get_n_items(), item.data());
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array or Tuple column");

    *pos += rows_read;
}


MergeTreeIndexConditionAnnoy::MergeTreeIndexConditionAnnoy(
    const IndexDescription & /*index_description*/,
    const SelectQueryInfo & query,
    const String & distance_function_,
    ContextPtr context)
    : ann_condition(query, context)
    , distance_function(distance_function_)
    , search_k(context->getSettings().annoy_index_search_k_nodes)
{}

bool MergeTreeIndexConditionAnnoy::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /*idx_granule*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for ANN skip indexes");
}

bool MergeTreeIndexConditionAnnoy::alwaysUnknownOrTrue() const
{
    return ann_condition.alwaysUnknownOrTrue(distance_function);
}

std::vector<size_t> MergeTreeIndexConditionAnnoy::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    if (distance_function == DISTANCE_FUNCTION_L2)
        return getUsefulRangesImpl<Annoy::Euclidean>(idx_granule);
    else if (distance_function == DISTANCE_FUNCTION_COSINE)
        return getUsefulRangesImpl<Annoy::Angular>(idx_granule);
    std::unreachable();
}

template <typename Distance>
std::vector<size_t> MergeTreeIndexConditionAnnoy::getUsefulRangesImpl(MergeTreeIndexGranulePtr idx_granule) const
{
    const UInt64 limit = ann_condition.getLimit();
    const UInt64 index_granularity = ann_condition.getIndexGranularity();
    const std::optional<float> comparison_distance = ann_condition.getQueryType() == ApproximateNearestNeighborInformation::Type::Where
        ? std::optional<float>(ann_condition.getComparisonDistanceForWhereQuery())
        : std::nullopt;

    if (comparison_distance && comparison_distance.value() < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to optimize query with where without distance");

    const std::vector<float> reference_vector = ann_condition.getReferenceVector();

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleAnnoy<Distance>>(idx_granule);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    const AnnoyIndexWithSerializationPtr<Distance> annoy = granule->index;

    if (ann_condition.getDimensions() != annoy->getDimensions())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the space in the request ({}) "
                        "does not match the dimension in the index ({})",
                        ann_condition.getDimensions(), annoy->getDimensions());

    std::vector<UInt64> neighbors; /// indexes of dots which were closest to the reference vector
    std::vector<Float32> distances;
    neighbors.reserve(limit);
    distances.reserve(limit);

    annoy->get_nns_by_vector(reference_vector.data(), limit, static_cast<int>(search_k), &neighbors, &distances);

    chassert(neighbors.size() == distances.size());

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

MergeTreeIndexAnnoy::MergeTreeIndexAnnoy(const IndexDescription & index_, UInt64 trees_, const String & distance_function_)
    : IMergeTreeIndex(index_)
    , trees(trees_)
    , distance_function(distance_function_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAnnoy::createIndexGranule() const
{
    if (distance_function == DISTANCE_FUNCTION_L2)
        return std::make_shared<MergeTreeIndexGranuleAnnoy<Annoy::Euclidean>>(index.name, index.sample_block);
    else if (distance_function == DISTANCE_FUNCTION_COSINE)
        return std::make_shared<MergeTreeIndexGranuleAnnoy<Annoy::Angular>>(index.name, index.sample_block);
    std::unreachable();
}

MergeTreeIndexAggregatorPtr MergeTreeIndexAnnoy::createIndexAggregator(const MergeTreeWriterSettings & settings) const
{
    /// TODO: Support more metrics. Available metrics: https://github.com/spotify/annoy/blob/master/src/annoymodule.cc#L151-L171
    if (distance_function == DISTANCE_FUNCTION_L2)
        return std::make_shared<MergeTreeIndexAggregatorAnnoy<Annoy::Euclidean>>(index.name, index.sample_block, trees, settings.max_threads_for_annoy_index_creation);
    else if (distance_function == DISTANCE_FUNCTION_COSINE)
        return std::make_shared<MergeTreeIndexAggregatorAnnoy<Annoy::Angular>>(index.name, index.sample_block, trees, settings.max_threads_for_annoy_index_creation);
    std::unreachable();
}

MergeTreeIndexConditionPtr MergeTreeIndexAnnoy::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionAnnoy>(index, query, distance_function, context);
};

MergeTreeIndexConditionPtr MergeTreeIndexAnnoy::createIndexCondition(const ActionsDAGPtr &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeTreeIndexAnnoy cannot be created with ActionsDAG");
}

MergeTreeIndexPtr annoyIndexCreator(const IndexDescription & index)
{
    static constexpr auto DEFAULT_DISTANCE_FUNCTION = DISTANCE_FUNCTION_L2;
    String distance_function = DEFAULT_DISTANCE_FUNCTION;
    if (!index.arguments.empty())
        distance_function = index.arguments[0].get<String>();

    static constexpr auto DEFAULT_TREES = 100uz;
    UInt64 trees = DEFAULT_TREES;
    if (index.arguments.size() > 1)
        trees = index.arguments[1].get<UInt64>();

    return std::make_shared<MergeTreeIndexAnnoy>(index, trees, distance_function);
}

void annoyIndexValidator(const IndexDescription & index, bool /* attach */)
{
    /// Check number and type of Annoy index arguments:

    if (index.arguments.size() > 2)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Annoy index must not have more than two parameters");

    if (!index.arguments.empty() && index.arguments[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Distance function argument of Annoy index must be of type String");

    if (index.arguments.size() > 1 && index.arguments[1].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Number of trees argument of Annoy index must be of type UInt64");

    /// Check that the index is created on a single column

    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Annoy indexes must be created on a single column");

    /// Check that a supported metric was passed as first argument

    if (!index.arguments.empty())
    {
        String distance_name = index.arguments[0].get<String>();
        if (distance_name != DISTANCE_FUNCTION_L2 && distance_name != DISTANCE_FUNCTION_COSINE)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Annoy index only supports distance functions '{}' and '{}'", DISTANCE_FUNCTION_L2, DISTANCE_FUNCTION_COSINE);
    }

    /// Check data type of indexed column:

    auto throw_unsupported_underlying_column_exception = []()
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Annoy indexes can only be created on columns of type Array(Float32) and Tuple(Float32[, Float32[, ...]])");
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
