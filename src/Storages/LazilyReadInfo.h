#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct LazilyReadInfo;
using LazilyReadInfoPtr = std::shared_ptr<LazilyReadInfo>;

class IMergeTreeDataPartInfoForReader;
using MergeTreeDataPartInfoForReaderPtr = std::shared_ptr<IMergeTreeDataPartInfoForReader>;
using DataPartInfoByIndex = std::unordered_map<size_t, MergeTreeDataPartInfoForReaderPtr>;
using DataPartInfoByIndexPtr = std::shared_ptr<DataPartInfoByIndex>;

struct LazilyReadInfo
{
    ColumnsWithTypeAndName lazily_read_columns;
    bool remove_part_offset_column;
    DataPartInfoByIndexPtr data_part_infos;

    LazilyReadInfo() = default;
};

}
