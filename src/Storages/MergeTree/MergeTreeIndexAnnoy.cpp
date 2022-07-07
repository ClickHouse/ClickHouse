#ifdef ENABLE_ANNOY

#include <Storages/MergeTree/MergeTreeIndexAnnoy.h>

#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

namespace ApproximateNearestNeighbour
{

template<typename Dist>
void AnnoyIndexSerialize<Dist>::serialize(WriteBuffer& ostr) const
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
void AnnoyIndexSerialize<Dist>::deserialize(ReadBuffer& istr)
{
    readIntBinary(Base::_s, istr);
    readIntBinary(Base::_n_items, istr);
    readIntBinary(Base::_n_nodes, istr);
    readIntBinary(Base::_nodes_size, istr);
    readIntBinary(Base::_K, istr);
    readIntBinary(Base::_seed, istr);
    readVectorBinary(Base::_roots, istr);
    Base::_nodes = realloc(Base::_nodes, Base::_s * Base::_n_nodes);
    istr.read(reinterpret_cast<char*>(Base::_nodes), Base::_s * Base::_n_nodes);

    Base::_fd = 0;
    // set flags
    Base::_loaded = false;
    Base::_verbose = false;
    Base::_on_disk = false;
    Base::_built = true;
}

template<typename Dist>
float AnnoyIndexSerialize<Dist>::getNumOfDimensions() const
{
    return Base::get_f();
}

}


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int INCORRECT_DATA;
}

MergeTreeIndexGranuleAnnoy::MergeTreeIndexGranuleAnnoy(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(nullptr)
{}

MergeTreeIndexGranuleAnnoy::MergeTreeIndexGranuleAnnoy(
    const String & index_name_,
    const Block & index_sample_block_,
    AnnoyIndexPtr index_base_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(std::move(index_base_))
{}

bool MergeTreeIndexGranuleAnnoy::empty() const
{
    return !static_cast<bool>(index_base);
}

void MergeTreeIndexGranuleAnnoy::serializeBinary(WriteBuffer & ostr) const
{
    writeIntBinary(index_base->getNumOfDimensions(), ostr); // write dimension
    index_base->serialize(ostr);
}

void MergeTreeIndexGranuleAnnoy::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    int dimension;
    readIntBinary(dimension, istr);
    index_base = std::make_shared<AnnoyIndex>(dimension);
    index_base->deserialize(istr);
}


MergeTreeIndexAggregatorAnnoy::MergeTreeIndexAggregatorAnnoy(
    const String & index_name_,
    const Block & index_sample_block_,
    int index_param_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_param(index_param_)
{}

bool MergeTreeIndexAggregatorAnnoy::empty() const
{
    return !index_base || index_base->get_n_items() == 0;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorAnnoy::getGranuleAndReset()
{
    index_base->build(index_param);
    auto granule = std::make_shared<MergeTreeIndexGranuleAnnoy>(index_name, index_sample_block, index_base);
    index_base = nullptr;
    return granule;
}

void MergeTreeIndexAggregatorAnnoy::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.", 
            toString(*pos), toString(block.rows()));

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (index_sample_block.columns() > 1)
    {
        throw Exception("Only one column is supported", ErrorCodes::LOGICAL_ERROR);
    }

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);
    const auto & column_array = typeid_cast<const ColumnArray*>(column_cut.get());
    if (column_array)
    {
        const auto & data = column_array->getData();
        const auto & array = typeid_cast<const ColumnFloat32&>(data).getData();
        const auto & offsets = column_array->getOffsets();
        size_t num_rows = column_array->size();

        /// All sizes are the same
        size_t size = offsets[1] - offsets[0];
        for (size_t i = 0; i < num_rows - 1; ++ i)
        {
            if (offsets[i + 1] - offsets[i] != size)
            {
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrays should have same length");
            }
        }
        index_base = std::make_shared<AnnoyIndex>(size);

        for (size_t current_row = 0; current_row < num_rows; ++current_row)
        {
            index_base->add_item(index_base->get_n_items(), &array[offsets[current_row]]);
        }
    }
    else
    {
        /// Other possible type of column is Tuple
        const auto & column_tuple = typeid_cast<const ColumnTuple*>(column_cut.get());

        if (!column_tuple)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Wrong type was given to index.");
    
        const auto & columns = column_tuple->getColumns();

        ///TODO swap iteration for less memory
        std::vector<std::vector<Float32>> data{column_tuple->size(), std::vector<Float32>()};
        for (const auto& column : columns)
        {
            const auto& pod_array = typeid_cast<const ColumnFloat32*>(column.get())->getData();
            for (size_t i = 0; i < pod_array.size(); ++i)
            {
                data[i].push_back(pod_array[i]);
            }
        }
        assert(!data.empty());
        if (!index_base)
        {
            index_base = std::make_shared<AnnoyIndex>(data[0].size());
        }
        for (const auto& item : data)
        {
            index_base->add_item(index_base->get_n_items(), &item[0]);
        }
    }

    *pos += rows_read;
}


MergeTreeIndexConditionAnnoy::MergeTreeIndexConditionAnnoy(
    const IndexDescription & /*index*/,
    const SelectQueryInfo & query,
    ContextPtr context)
    : condition(query, context)
{}


bool MergeTreeIndexConditionAnnoy::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /* idx_granule */) const
{
    throw Exception("mayBeTrueOnGranule is not supported for ANN skip indexes", ErrorCodes::LOGICAL_ERROR);
}

bool MergeTreeIndexConditionAnnoy::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue("L2Distance");
}

std::vector<size_t> MergeTreeIndexConditionAnnoy::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    UInt64 limit = condition.getLimitCount();
    UInt64 index_granularity = condition.getIndexGranularity();
    std::optional<float> comp_dist
        = condition.queryHasWhereClause() ? std::optional<float>(condition.getComparisonDistanceForWhereQuery()) : std::nullopt;

    if (comp_dist && comp_dist.value() < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attemp to optimize query with where without distance");
    
    std::vector<float> target_vec = condition.getTargetVector();

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleAnnoy>(idx_granule);
    if (granule == nullptr)
    {
        throw Exception("Granule has the wrong type", ErrorCodes::LOGICAL_ERROR);
    }
    auto annoy = granule->index_base;

    if (condition.getNumOfDimensions() != annoy->getNumOfDimensions())
    {
        throw Exception("The dimension of the space in the request (" + toString(condition.getNumOfDimensions()) + ") "
            + "does not match with the dimension in the index (" + toString(annoy->getNumOfDimensions()) + ")", ErrorCodes::INCORRECT_QUERY);
    }

    std::vector<int32_t> items;
    std::vector<float> dist;
    items.reserve(limit);
    dist.reserve(limit);

    int k_search = -1;
    auto params_str = condition.getParamsStr();
    if (!params_str.empty())
    {
        try
        {
            k_search = std::stoi(params_str);
        }
        catch (...)
        {
            throw Exception("Setting of the annoy index should be int", ErrorCodes::INCORRECT_QUERY);
        }
    }
    annoy->get_nns_by_vector(&target_vec[0], 1, k_search, &items, &dist);
    std::unordered_set<size_t> result;
    for (size_t i = 0; i < items.size(); ++i)
    {
        if (comp_dist && dist[i] > comp_dist)
        {
            continue;
        }
        result.insert(items[i] / index_granularity);
    }

    std::vector<size_t> result_vector;
    result_vector.reserve(result.size());
    for (auto range : result)
    {
        result_vector.push_back(range);
    }

    return result_vector;
}


MergeTreeIndexGranulePtr MergeTreeIndexAnnoy::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleAnnoy>(index.name, index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexAnnoy::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorAnnoy>(index.name, index.sample_block, index_param);
}

MergeTreeIndexConditionPtr MergeTreeIndexAnnoy::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionAnnoy>(index, query, context);
};

MergeTreeIndexFormat MergeTreeIndexAnnoy::getDeserializedFormat(const DataPartStoragePtr & data_part_storage, const std::string & relative_path_prefix) const
{
    if (data_part_storage->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (data_part_storage->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr AnnoyIndexCreator(const IndexDescription & index)
{
    int param = index.arguments[0].get<int>();
    return std::make_shared<MergeTreeIndexAnnoy>(index, param);
}

void AnnoyIndexValidator(const IndexDescription & index, bool /* attach */)
{
    if (index.arguments.size() != 1)
    {
        throw Exception("Annoy index must have exactly one argument.", ErrorCodes::INCORRECT_QUERY);
    }
    if (index.arguments[0].getType() != Field::Types::UInt64)
    {
        throw Exception("Annoy index argument must be UInt64.", ErrorCodes::INCORRECT_QUERY);
    }
}

}
#endif // ENABLE_ANNOY
