#include <cstddef>
#include <cstring>
#include <sstream>
#include <string>
#include <Storages/MergeTree/MergeTreeIndexSimpleHnsw.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleSimpleHnsw::MergeTreeIndexGranuleSimpleHnsw(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}


MergeTreeIndexGranuleSimpleHnsw::MergeTreeIndexGranuleSimpleHnsw(const String & index_name_, const Block & index_sample_block_, std::shared_ptr<similarity::Hnsw<float>> index_impl_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_), index_impl(std::move(index_impl_))
{}


void MergeTreeIndexGranuleSimpleHnsw::serializeBinary(WriteBuffer & ostr) const{
     std::ostringstream ost;
     index_impl->SaveIndex(ost);
     std::string raw_data = ost.str();
     ostr.write(raw_data.data(), raw_data.size());
}

void MergeTreeIndexGranuleSimpleHnsw::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version){
    if (version != 1){
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);
    }
    size_t index_size = istr.available();
    std::string str(index_size, '\0');
    istr.read(str.data(), index_size);
    std::istringstream ist(str);
    index_impl->LoadIndex(ist);
}

MergeTreeIndexAggregatorSimpleHnsw::MergeTreeIndexAggregatorSimpleHnsw(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_), index_impl(nullptr)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSimpleHnsw::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleSimpleHnsw>(index_name, index_sample_block, std::move(index_impl));
}

void MergeTreeIndexAggregatorSimpleHnsw::update(const Block & /*block*/, size_t * /*pos*/, size_t /*limit*/){

}

MergeTreeIndexConditionSimpleHnsw::MergeTreeIndexConditionSimpleHnsw(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context)
    : index_data_types(index.data_types)
    , condition(query, context, index.column_names, index.expression)
{
}

bool MergeTreeIndexConditionSimpleHnsw::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue();
}

bool MergeTreeIndexConditionSimpleHnsw::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /*idx_granule*/) const
{
    // std::shared_ptr<MergeTreeIndexGranuleSimpleHnsw> granule
    //     = std::dynamic_pointer_cast<MergeTreeIndexGranuleSimpleHnsw>(idx_granule);
    // if (!granule)
    //     throw Exception(
    //         "Minmax index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    // return condition.checkInHyperrectangle(granule->hyperrectangle, index_data_types).can_be_true;
    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexSimpleHnsw::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSimpleHnsw>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexSimpleHnsw::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorSimpleHnsw>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexSimpleHnsw::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionSimpleHnsw>(index, query, context);
};

bool MergeTreeIndexSimpleHnsw::mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const
{
    // const String column_name = node->getColumnName();

    // for (const auto & cname : index.column_names)
    //     if (column_name == cname)
    //         return true;

    // if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
    //     if (func->arguments->children.size() == 1)
    //         return mayBenefitFromIndexForIn(func->arguments->children.front());

    return false;
}

MergeTreeIndexFormat MergeTreeIndexSimpleHnsw::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}


MergeTreeIndexPtr simpleHnswIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexSimpleHnsw>(index);
}

void simpleHnswIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{
}

}
