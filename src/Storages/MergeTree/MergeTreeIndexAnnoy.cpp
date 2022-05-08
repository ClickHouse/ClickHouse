#include <Storages/MergeTree/MergeTreeIndexAnnoy.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>

#include <Poco/Logger.h>
#include <base/logger_useful.h>

#include "Core/Field.h"
#include "Interpreters/Context_fwd.h"
#include "MergeTreeIndices.h"
#include "KeyCondition.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/IAST_fwd.h"
#include "Storages/SelectQueryInfo.h"
#include "base/types.h"


namespace DB
{

namespace Annoy
{

const int NUM_OF_TREES = 20;
const int DIMENSION = 512;

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

template<typename Dist>
float AnnoyIndexSerialize<Dist>::getSpaceDim() const {
    return Base::get_f();
}

}


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
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
    auto granule = std::make_shared<MergeTreeIndexGranuleAnnoy>(index_name, index_sample_block, index_base);
    index_base = std::make_shared<AnnoyIndex>(Annoy::DIMENSION);
    return granule;
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
    const auto & column_tuple = typeid_cast<const ColumnTuple*>(column_cut.get());
    const auto & columns = column_tuple->getColumns();

    std::vector<std::vector<Float32>> data{column_tuple->size(), std::vector<Float32>()};
    for (size_t j = 0; j < columns.size(); ++j) {
        const auto& pod_array = typeid_cast<const ColumnFloat32*>(columns[j].get())->getData();
        for (size_t i = 0; i < pod_array.size(); ++i) {
            data[i].push_back(pod_array[i]);
        }
    }
    for (const auto& item : data) {
        index_base->add_item(index_base->get_n_items(), &item[0]);
    }

    *pos += rows_read;
}


MergeTreeIndexConditionAnnoy::MergeTreeIndexConditionAnnoy(
    const IndexDescription & /*index*/,
    const SelectQueryInfo & query,
    ContextPtr context)
    : condition(query, context)
{
}


bool MergeTreeIndexConditionAnnoy::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleAnnoy>(idx_granule);
    auto annoy = std::dynamic_pointer_cast<Annoy::AnnoyIndexSerialize<>>(granule->index_base);


    if (condition.getMetric() != "L2Distance") {
        throw Exception("The metric in the request (" + toString(condition.getSpaceDim()) + ")"
            + "does not match with the metric in the index (" + toString(annoy->getSpaceDim()) + ")", ErrorCodes::INCORRECT_QUERY);
    }
    if (condition.getSpaceDim() == annoy->getSpaceDim()) {
        throw Exception("The dimension of the space in the request (" + toString(condition.getSpaceDim()) + ")"
            + "does not match with the dimension in the index (" + toString(annoy->getSpaceDim()) + ")", ErrorCodes::INCORRECT_QUERY);
    }
    std::vector<float> target_vec = condition.getTargetVector();
    float max_distance = condition.getComparisonDistance();

    std::vector<int32_t> items;
    std::vector<float> dist;

    // 1 - num of nearest neighbour (NN)
    // next number - upper limit on the size of the internal queue; -1 means, that it is equal to num of trees * num of NN
    annoy->get_nns_by_vector(&target_vec[0], 1, -1, &items, &dist);
    return dist[0] < max_distance;
}

bool MergeTreeIndexConditionAnnoy::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue("L2Distance");
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
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionAnnoy>(index, query, context);
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
