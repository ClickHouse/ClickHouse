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

namespace ApproximateNearestNeighbour
{

template<typename Dist>
void AnnoyIndex<Dist>::serialize(WriteBuffer& ostr) const
{
    assert(Base::_built);
    writeIntBinary(Base::_s, ostr);
    writeIntBinary(Base::_n_items, ostr);
    writeIntBinary(Base::_n_nodes, ostr);
    writeIntBinary(Base::_nodes_size, ostr);
    writeIntBinary(Base::_K, ostr);
    writeIntBinary(Base::_seed, ostr);
    writeVectorBinary(Base::_roots, ostr);
    ostr.write(reinterpret_cast<const char*>(Base::_nodes), Base::_s * Base::_n_nodes);
}

template<typename Dist>
void AnnoyIndex<Dist>::deserialize(ReadBuffer& istr)
{
    assert(!Base::_built);
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

template<typename Dist>
uint64_t AnnoyIndex<Dist>::getNumOfDimensions() const
{
    return Base::get_f();
}

}


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
MergeTreeIndexGranuleAnnoy<Distance>::MergeTreeIndexGranuleAnnoy(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index(nullptr)
{}

template <typename Distance>
MergeTreeIndexGranuleAnnoy<Distance>::MergeTreeIndexGranuleAnnoy(
    const String & index_name_,
    const Block & index_sample_block_,
    AnnoyIndexPtr index_base_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index(std::move(index_base_))
{}

template <typename Distance>
void MergeTreeIndexGranuleAnnoy<Distance>::serializeBinary(WriteBuffer & ostr) const
{
    /// number of dimensions is required in the constructor,
    /// so it must be written and read separately from the other part
    writeIntBinary(index->getNumOfDimensions(), ostr); // write dimension
    index->serialize(ostr);
}

template <typename Distance>
void MergeTreeIndexGranuleAnnoy<Distance>::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    uint64_t dimension;
    readIntBinary(dimension, istr);
    index = std::make_shared<AnnoyIndex>(dimension);
    index->deserialize(istr);
}

template <typename Distance>
MergeTreeIndexAggregatorAnnoy<Distance>::MergeTreeIndexAggregatorAnnoy(
    const String & index_name_,
    const Block & index_sample_block_,
    uint64_t number_of_trees_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , number_of_trees(number_of_trees_)
{}

template <typename Distance>
MergeTreeIndexGranulePtr MergeTreeIndexAggregatorAnnoy<Distance>::getGranuleAndReset()
{
    // NOLINTNEXTLINE(*)
    index->build(static_cast<int>(number_of_trees), /*number_of_threads=*/1);
    auto granule = std::make_shared<MergeTreeIndexGranuleAnnoy<Distance> >(index_name, index_sample_block, index);
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
            toString(*pos), toString(block.rows()));

    size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    if (index_sample_block.columns() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only one column is supported");

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);
    const auto & column_array = typeid_cast<const ColumnArray*>(column_cut.get());
    if (column_array)
    {
        const auto & data = column_array->getData();
        const auto & array = typeid_cast<const ColumnFloat32&>(data).getData();
        if (array.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array has 0 rows, {} rows expected", rows_read);
        const auto & offsets = column_array->getOffsets();
        size_t num_rows = offsets.size();

        /// Check all sizes are the same
        size_t size = offsets[0];
        for (size_t i = 0; i < num_rows - 1; ++i)
            if (offsets[i + 1] - offsets[i] != size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrays should have same length");

        index = std::make_shared<AnnoyIndex>(size);

        index->add_item(index->get_n_items(), array.data());
        /// add all rows from 1 to num_rows - 1 (this is the same as the beginning of the last element)
        for (size_t current_row = 1; current_row < num_rows; ++current_row)
            index->add_item(index->get_n_items(), &array[offsets[current_row - 1]]);
    }
    else
    {
        /// Other possible type of column is Tuple
        const auto & column_tuple = typeid_cast<const ColumnTuple*>(column_cut.get());

        if (!column_tuple)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Wrong type was given to index.");

        const auto & columns = column_tuple->getColumns();

        std::vector<std::vector<Float32>> data{column_tuple->size(), std::vector<Float32>()};
        for (const auto& column : columns)
        {
            const auto& pod_array = typeid_cast<const ColumnFloat32*>(column.get())->getData();
            for (size_t i = 0; i < pod_array.size(); ++i)
                data[i].push_back(pod_array[i]);
        }
        assert(!data.empty());
        if (!index)
            index = std::make_shared<AnnoyIndex>(data[0].size());
        for (const auto& item : data)
            index->add_item(index->get_n_items(), item.data());
    }

    *pos += rows_read;
}


MergeTreeIndexConditionAnnoy::MergeTreeIndexConditionAnnoy(
    const IndexDescription & /*index*/,
    const SelectQueryInfo & query,
    ContextPtr context,
    const String& distance_name_)
    : condition(query, context), distance_name(distance_name_)
{}


bool MergeTreeIndexConditionAnnoy::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /* idx_granule */) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for ANN skip indexes");
}

bool MergeTreeIndexConditionAnnoy::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue(distance_name);
}

std::vector<size_t> MergeTreeIndexConditionAnnoy::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    if (distance_name == "L2Distance")
    {
        return getUsefulRangesImpl<::Annoy::Euclidean>(idx_granule);
    }
    else if (distance_name == "cosineDistance")
    {
        return getUsefulRangesImpl<::Annoy::Angular>(idx_granule);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown distance name. Must be 'L2Distance' or 'cosineDistance'. Got {}", distance_name);
    }
}


template <typename Distance>
std::vector<size_t> MergeTreeIndexConditionAnnoy::getUsefulRangesImpl(MergeTreeIndexGranulePtr idx_granule) const
{
    UInt64 limit = condition.getLimit();
    UInt64 index_granularity = condition.getIndexGranularity();
    std::optional<float> comp_dist = condition.getQueryType() == ApproximateNearestNeighbour::ANNQueryInformation::Type::Where ?
     std::optional<float>(condition.getComparisonDistanceForWhereQuery()) : std::nullopt;

    if (comp_dist && comp_dist.value() < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to optimize query with where without distance");

    std::vector<float> target_vec = condition.getTargetVector();

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleAnnoy<Distance> >(idx_granule);
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

MergeTreeIndexGranulePtr MergeTreeIndexAnnoy::createIndexGranule() const
{
    if (distance_name == "L2Distance")
    {
        return std::make_shared<MergeTreeIndexGranuleAnnoy<::Annoy::Euclidean> >(index.name, index.sample_block);
    }
    if (distance_name == "cosineDistance")
    {
        return std::make_shared<MergeTreeIndexGranuleAnnoy<::Annoy::Angular> >(index.name, index.sample_block);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown distance name. Must be 'L2Distance' or 'cosineDistance'. Got {}", distance_name);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexAnnoy::createIndexAggregator() const
{
    if (distance_name == "L2Distance")
    {
        return std::make_shared<MergeTreeIndexAggregatorAnnoy<::Annoy::Euclidean> >(index.name, index.sample_block, number_of_trees);
    }
    if (distance_name == "cosineDistance")
    {
        return std::make_shared<MergeTreeIndexAggregatorAnnoy<::Annoy::Angular> >(index.name, index.sample_block, number_of_trees);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown distance name. Must be 'L2Distance' or 'cosineDistance'. Got {}", distance_name);
}

MergeTreeIndexConditionPtr MergeTreeIndexAnnoy::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionAnnoy>(index, query, context, distance_name);
};

MergeTreeIndexPtr annoyIndexCreator(const IndexDescription & index)
{
    uint64_t param = 100;
    String distance_name = "L2Distance";
    if (!index.arguments.empty() && !index.arguments[0].tryGet<uint64_t>(param))
    {
        if (!index.arguments[0].tryGet<String>(distance_name))
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Can't parse first argument");
        }
    }
    if (index.arguments.size() > 1 && !index.arguments[1].tryGet<String>(distance_name))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Can't parse second argument");
    }
    return std::make_shared<MergeTreeIndexAnnoy>(index, param, distance_name);
}

static void assertIndexColumnsType(const Block & header)
{
    DataTypePtr column_data_type_ptr = header.getDataTypes()[0];

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(column_data_type_ptr.get()))
    {
        TypeIndex nested_type_index = array_type->getNestedType()->getTypeId();
        if (!WhichDataType(nested_type_index).isFloat32())
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Unexpected type {} of Annoy index. Only Array(Float32) and Tuple(Float32) are supported.",
                column_data_type_ptr->getName());
    }
    else if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(column_data_type_ptr.get()))
    {
        const DataTypes & nested_types = tuple_type->getElements();
        for (const auto & type : nested_types)
        {
            TypeIndex nested_type_index = type->getTypeId();
            if (!WhichDataType(nested_type_index).isFloat32())
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Unexpected type {} of Annoy index. Only Array(Float32) and Tuple(Float32) are supported.",
                    column_data_type_ptr->getName());
        }
    }
    else
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Unexpected type {} of Annoy index. Only Array(Float32) and Tuple(Float32) are supported.",
            column_data_type_ptr->getName());

}

void annoyIndexValidator(const IndexDescription & index, bool /* attach */)
{
    if (index.arguments.size() > 2)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Annoy index must not have more than two parameters");
    }
    if (!index.arguments.empty() && index.arguments[0].getType() != Field::Types::UInt64
        && index.arguments[0].getType() != Field::Types::String)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Annoy index first argument must be UInt64 or String.");
    }
    if (index.arguments.size() > 1 && index.arguments[1].getType() != Field::Types::String)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Annoy index second argument must be String.");
    }

    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Annoy indexes must be created on a single column");

    assertIndexColumnsType(index.sample_block);
}

}
#endif // ENABLE_ANNOY
