#include <Storages/MergeTree/MergeTreeIndexAnnoy.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace Annoy
{

const int NUM_OF_TREES = 5;
const int DIMENSION = 2;

template<typename Dist>
void AnnoyIndexSerialize<Dist>::serialize(WriteBuffer& ostr) const
{
    if (!Base::_built) {
        throw Exception("Annoy Index should be built before serialization", ErrorCodes::LOGICAL_ERROR);
    }
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

}


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

bool MergeTreeIndexGranuleAnnoy::empty() const {
    return !static_cast<bool>(index_base);
}

void MergeTreeIndexGranuleAnnoy::serializeBinary(WriteBuffer & ostr) const
{
    writeIntBinary(index_base->get_f(), ostr); // write dimension
    index_base->serialize(ostr);
}

void MergeTreeIndexGranuleAnnoy::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    int dimension;
    readIntBinary(dimension, istr);
    index_base = std::make_shared<AnnoyIndex>(dimension);
    index_base->deserialize(istr);
}


MergeTreeIndexAggregatorAnnoy::MergeTreeIndexAggregatorAnnoy(const String & index_name_,
                                                                const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(std::make_shared<AnnoyIndex>(Annoy::DIMENSION))
{}

bool MergeTreeIndexAggregatorAnnoy::empty() const
{
    return index_base->get_n_items() == 0;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorAnnoy::getGranuleAndReset()
{
    if (empty()) {
        return std::make_shared<MergeTreeIndexGranuleAnnoy>(index_name, index_sample_block);
    }

    index_base->build(Annoy::NUM_OF_TREES);
    return std::make_shared<MergeTreeIndexGranuleAnnoy>(index_name, index_sample_block, index_base);
}

void MergeTreeIndexAggregatorAnnoy::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (index_sample_block.columns() > 1) {
        throw Exception("Only one column is supported", ErrorCodes::LOGICAL_ERROR);
    }

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);
    const auto & columns = typeid_cast<const ColumnTuple*>(column_cut.get())->getColumns();

    for (const auto& x : columns) {
        const auto& pod_array = typeid_cast<const ColumnFloat32*>(x.get())->getData();
        std::vector<Float32> flatten(pod_array.begin(), pod_array.end());
        index_base->add_item(index_base->get_n_items(), &flatten[0]);
    }

    *pos += rows_read;
}


bool MergeTreeIndexConditionAnnoy::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleAnnoy>(idx_granule);
    auto annoy = std::dynamic_pointer_cast<Annoy::AnnoyIndexSerialize<>>(granule->index_base);

    float zero[] = {0., 0.};
    std::vector<int32_t> items;
    std::vector<float> dist;
    items.reserve(1);
    dist.reserve(1);

    // 1 - num of nearest neighbour (NN)
    // next number - upper limit on the size of the internal queue; -1 means, that it is equal to num of trees * num of NN
    annoy->get_nns_by_vector(&zero[0], 2, -1, &items, &dist);
    const float max_dist = 1.;
    return dist[0] < max_dist;
}


MergeTreeIndexGranulePtr MergeTreeIndexAnnoy::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleAnnoy>(index.name, index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexAnnoy::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorAnnoy>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexAnnoy::createIndexCondition(
    const SelectQueryInfo & /*query*/, ContextPtr /*context*/) const
{
    return std::make_shared<MergeTreeIndexConditionAnnoy>();
};

MergeTreeIndexFormat MergeTreeIndexAnnoy::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr AnnoyIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexAnnoy>(index);
}

void AnnoyIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{}

}
