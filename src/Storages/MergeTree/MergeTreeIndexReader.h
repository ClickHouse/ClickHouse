#pragma once

#include <memory>
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
        MarkCache * mark_cache,
        UncompressedCache * uncompressed_cache,
        MergeTreeReaderSettings settings);

    void seek(size_t mark);

    void read(MergeTreeIndexGranulePtr & granule);

private:
    MergeTreeIndexPtr index;
    std::unique_ptr<MergeTreeReaderStream> stream;
    uint8_t version = 0;
};

}
