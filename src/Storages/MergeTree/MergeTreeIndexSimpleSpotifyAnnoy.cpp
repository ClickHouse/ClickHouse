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


void MergeTreeIndexGranuleMinMax::serializeBinary(WriteBuffer & /*ostr*/) const{}

void MergeTreeIndexGranuleMinMax::deserializeBinary(ReadBuffer & /*istr*/, MergeTreeIndexVersion /*version*/){}
}