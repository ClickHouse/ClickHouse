#ifdef ENABLE_ANNOY

#include <Storages/MergeTree/MergeTreeIndexAnnoy.h>

#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


template <typename Distance>
AnnoyIndexWithSerialization<Distance>::AnnoyIndexWithSerialization(uint64_t dim)
    : Base::AnnoyIndex(dim)
{
}

template<typename Distance>
void AnnoyIndexWithSerialization<Distance>::serialize(WriteBuffer& ostr) const
{
    chassert(Base::_built);
    writeIntBinary(Base::_s, ostr);
    writeIntBinary(Base::_n_items, ostr);
    writeIntBinary(Base::_n_nodes, ostr);
    writeIntBinary(Base::_nodes_size, ostr);
    writeIntBinary(Base::_K, ostr);
    writeIntBinary(Base::_seed, ostr);
    writeVectorBinary(Base::_roots, ostr);
    ostr.write(reinterpret_cast<const char*>(Base::_nodes), Base::_s * Base::_n_nodes);
}

template<typename Distance>
void AnnoyIndexWithSerialization<Distance>::deserialize(ReadBuffer& istr)
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
uint64_t AnnoyIndexWithSerialization<Distance>::getNumOfDimensions() const
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
    writeIntBinary(index->getNumOfDimensions(), ostr); // write dimension
    index->serialize(ostr);
}

template <typename Distance>
void MergeTreeIndexGranuleAnnoy<Distance>::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    uint64_t dimension;
    readIntBinary(dimension, istr);
    index = std::make_shared<AnnoyIndexWithSerialization<Distance>>(dimension);
    index->deserialize(istr);
}

template <typename Distance>
MergeTreeIndexAggregatorAnnoy<Distance>::MergeTreeIndexAggregatorAnnoy(
    const String & index_name_,
    const Block & index_sample_block_,
    uint64_t trees_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , trees(trees_)
{}

template <typename Distance>
MergeTreeIndexGranulePtr MergeTreeIndexAggregatorAnnoy<Distance>::getGranuleAndReset()
{
    // NOLINTNEXTLINE(*)
    index->build(static_cast<int>(trees), /*number_of_threads=*/1);
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

    if (index_sample_block.columns() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected block with single column");

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);

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

        index = std::make_shared<AnnoyIndexWithSerialization<Distance>>(size);

        /// Add all rows of block
        index->add_item(index->get_n_items(), array.data());
        for (size_t current_row = 1; current_row < num_rows; ++current_row)
            index->add_item(index->get_n_items(), &array[offsets[current_row - 1]]);
    }
    else if (const auto & column_tuple = typeid_cast<const ColumnTuple *>(column_cut.get()))
    {
        const auto & columns = column_tuple->getColumns();

        /// TODO check if calling index->add_item() directly on the block's tuples is faster than materializing everything
        std::vector<std::vector<Float32>> data{column_tuple->size(), std::vector<Float32>()};
        for (const auto & column : columns)
        {
            const auto & pod_array = typeid_cast<const ColumnFloat32 *>(column.get())->getData();
            for (size_t i = 0; i < pod_array.size(); ++i)
                data[i].push_back(pod_array[i]);
        }

        if (data.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tuple has 0 rows, {} rows expected", rows_read);

        index = std::make_shared<AnnoyIndexWithSerialization<Distance>>(data[0].size());

        for (const auto & item : data)
            index->add_item(index->get_n_items(), item.data());
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array or Tuple column");

    *pos += rows_read;
}


MergeTreeIndexConditionAnnoy::MergeTreeIndexConditionAnnoy(
    const IndexDescription & /*index*/,
    const SelectQueryInfo & query,
    const String & distance_function_,
    ContextPtr context)
    : condition(query, context)
    , distance_function(distance_function_)
{}


bool MergeTreeIndexConditionAnnoy::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /*idx_granule*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for ANN skip indexes");
}

bool MergeTreeIndexConditionAnnoy::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue(distance_function);
}

std::vector<size_t> MergeTreeIndexConditionAnnoy::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    if (distance_function == "L2Distance")
        return getUsefulRangesImpl<Annoy::Euclidean>(idx_granule);
    else if (distance_function == "cosineDistance")
        return getUsefulRangesImpl<Annoy::Angular>(idx_granule);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown distance name. Must be 'L2Distance' or 'cosineDistance'. Got {}", distance_function);
}


template <typename Distance>
std::vector<size_t> MergeTreeIndexConditionAnnoy::getUsefulRangesImpl(MergeTreeIndexGranulePtr idx_granule) const
{
    UInt64 limit = condition.getLimit();
    UInt64 index_granularity = condition.getIndexGranularity();
    std::optional<float> comp_dist = condition.getQueryType() == ApproximateNearestNeighbour::ANNQueryInformation::Type::Where
        ? std::optional<float>(condition.getComparisonDistanceForWhereQuery())
        : std::nullopt;

    if (comp_dist && comp_dist.value() < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to optimize query with where without distance");

    std::vector<float> target_vec = condition.getTargetVector();

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleAnnoy<Distance>>(idx_granule);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    auto annoy = granule->index;

    if (condition.getNumOfDimensions() != annoy->getNumOfDimensions())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the space in the request ({}) "
                        "does not match with the dimension in the index ({})",
                        toString(condition.getNumOfDimensions()), toString(annoy->getNumOfDimensions()));

    /// neighbors contain indexes of dots which were closest to target vector
    std::vector<UInt64> neighbors;
    std::vector<Float32> distances;
    neighbors.reserve(limit);
    distances.reserve(limit);

    int k_search = -1;
    String params_str = condition.getParamsStr();
    if (!params_str.empty())
    {
        try
        {
            /// k_search=... (algorithm will inspect up to search_k nodes which defaults to n_trees * n if not provided)
            k_search = std::stoi(params_str.data() + 9);
        }
        catch (...)
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Setting of the annoy index should be int");
        }
    }
    annoy->get_nns_by_vector(target_vec.data(), limit, k_search, &neighbors, &distances);
    std::unordered_set<size_t> granule_numbers;
    for (size_t i = 0; i < neighbors.size(); ++i)
    {
        if (comp_dist && distances[i] > comp_dist)
            continue;
        granule_numbers.insert(neighbors[i] / index_granularity);
    }

    std::vector<size_t> result_vector;
    result_vector.reserve(granule_numbers.size());
    for (auto granule_number : granule_numbers)
        result_vector.push_back(granule_number);

    return result_vector;
}

MergeTreeIndexAnnoy::MergeTreeIndexAnnoy(const IndexDescription & index_, uint64_t trees_, const String & distance_function_)
    : IMergeTreeIndex(index_)
    , trees(trees_)
    , distance_function(distance_function_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAnnoy::createIndexGranule() const
{
    if (distance_function == "L2Distance")
        return std::make_shared<MergeTreeIndexGranuleAnnoy<Annoy::Euclidean>>(index.name, index.sample_block);
    else if (distance_function == "cosineDistance")
        return std::make_shared<MergeTreeIndexGranuleAnnoy<Annoy::Angular>>(index.name, index.sample_block);
    std::unreachable();
}

MergeTreeIndexAggregatorPtr MergeTreeIndexAnnoy::createIndexAggregator() const
{
    /// TODO: Support more metrics. Available metrics: https://github.com/spotify/annoy/blob/master/src/annoymodule.cc#L151-L171
    if (distance_function == "L2Distance")
        return std::make_shared<MergeTreeIndexAggregatorAnnoy<Annoy::Euclidean>>(index.name, index.sample_block, trees);
    if (distance_function == "cosineDistance")
        return std::make_shared<MergeTreeIndexAggregatorAnnoy<Annoy::Angular>>(index.name, index.sample_block, trees);
    std::unreachable();
}

MergeTreeIndexConditionPtr MergeTreeIndexAnnoy::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionAnnoy>(index, query, distance_function, context);
};

MergeTreeIndexPtr annoyIndexCreator(const IndexDescription & index)
{
    uint64_t trees = 100;
    String distance_function = "L2Distance";

    if (!index.arguments.empty())
        distance_function = index.arguments[0].get<String>();

    if (index.arguments.size() > 1)
        trees = index.arguments[1].get<uint64_t>();

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
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Number of trees argument of Annoy index must be UInt64");

    /// Check that the index is created on a single column

    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Annoy indexes must be created on a single column");

    /// Check that a supported metric was passed as first argument

    if (!index.arguments.empty())
    {
        String distance_name = index.arguments[0].get<String>();
        if (distance_name != "L2Distance" && distance_name != "cosineDistance")
            throw Exception(ErrorCodes::INCORRECT_DATA, "Annoy index supports only distance functions 'L2Distance' and 'cosineDistance'. Given distance function: {}", distance_name);
    }

    /// Check data type of indexed column:

    auto throw_unsupported_underlying_column_exception = [](DataTypePtr data_type)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Annoy indexes can only be created on columns of type Array(Float32) and Tuple(Float32). Given type: {}",
            data_type->getName());
    };

    DataTypePtr data_type = index.sample_block.getDataTypes()[0];

    if (const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
        if (!WhichDataType(nested_type_index).isFloat32())
            throw_unsupported_underlying_column_exception(data_type);
    }
    else if (const auto * data_type_tuple = typeid_cast<const DataTypeTuple *>(data_type.get()))
    {
        const DataTypes & inner_types = data_type_tuple->getElements();
        for (const auto & inner_type : inner_types)
        {
            TypeIndex nested_type_index = inner_type->getTypeId();
            if (!WhichDataType(nested_type_index).isFloat32())
                throw_unsupported_underlying_column_exception(data_type);
        }
    }
    else
        throw_unsupported_underlying_column_exception(data_type);
}

}

#endif
