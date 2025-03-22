#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct LazilyReadInfo;
using LazilyReadInfoPtr = std::shared_ptr<LazilyReadInfo>;

struct LazilyReadInfo
{
    ColumnsWithTypeAndName lazily_read_columns;
    bool remove_part_offset_column;
    DataPartsInfoPtr data_parts_info;

    LazilyReadInfo() = default;
};

}
