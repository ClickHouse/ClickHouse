#pragma once

#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MergeTreeIndexReader
{
public:
    MergeTreeIndexReader(
        MergeTreeIndexPtr index_,
        MergeTreeData::DataPartPtr part_,
        size_t marks_count_,
        const MarkRanges & all_mark_ranges_,
        MergeTreeReaderSettings settings);

    void seek(size_t mark);

    MergeTreeIndexGranulePtr read();

private:
    MergeTreeIndexPtr index;
    MergeTreeReaderStream stream;
};

}
