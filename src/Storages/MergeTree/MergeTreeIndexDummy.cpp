#include <cstddef>
#include <Storages/MergeTree/MergeTreeIndexDummy.h>
#include <cstdlib>
#include <iostream>


namespace DB{
MergeTreeIndexGranuleDummy::MergeTreeIndexGranuleDummy(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{

}


void MergeTreeIndexGranuleDummy::serializeBinary(WriteBuffer & /*ostr*/) const {}

void MergeTreeIndexGranuleDummy::deserializeBinary(ReadBuffer & /*istr*/, MergeTreeIndexVersion /*version*/)
{

}

MergeTreeIndexAggregatorDummy::MergeTreeIndexAggregatorDummy(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorDummy::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleDummy>(index_name, index_sample_block);
}

void MergeTreeIndexAggregatorDummy::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    // FieldRef field_min;
    // FieldRef field_max;
    // for (size_t i = 0; i < index_sample_block.columns(); ++i)
    // {
    //     auto index_column_name = index_sample_block.getByPosition(i).name;
    //     const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);
        
    // }

    *pos += rows_read;
}

MergeTreeIndexConditionDummy::MergeTreeIndexConditionDummy(
    const IndexDescription & index,  const SelectQueryInfo & /*query*/,ContextPtr /*context*/): index_data_types(index.data_types)
{
}

bool MergeTreeIndexConditionDummy::alwaysUnknownOrTrue() const
{
    return false;
}

bool MergeTreeIndexConditionDummy::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    static size_t counter = 0;
    std::shared_ptr<MergeTreeIndexGranuleDummy> granule
        = std::dynamic_pointer_cast<MergeTreeIndexGranuleDummy>(idx_granule);
    if (!granule)
        throw Exception(
            "Dummy index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    return counter++ % 2 == 0;
}

MergeTreeIndexGranulePtr MergeTreeIndexDummy::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleDummy>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexDummy::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorDummy>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexDummy::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionDummy>(index, query, context);
};

bool MergeTreeIndexDummy::mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const
{
    return true;
}

MergeTreeIndexFormat MergeTreeIndexDummy::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr dummyIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexDummy>(index);
}

void dummyIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{
}


}
