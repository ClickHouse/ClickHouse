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
        VectorSimilarityIndexCache * vector_similarity_index_cache,
        MergeTreeReaderSettings settings_);

    MergeTreeIndexGranulePtr read(size_t mark);

private:
    MergeTreeIndexPtr index;
    MergeTreeData::DataPartPtr part;
    size_t marks_count;
    const MarkRanges & all_mark_ranges;
    MarkCache * mark_cache;
    UncompressedCache * uncompressed_cache;
    VectorSimilarityIndexCache * vector_similarity_index_cache;
    MergeTreeReaderSettings settings;

    std::unique_ptr<MergeTreeReaderStream> stream;
    uint8_t version = 0;
    size_t stream_mark = 0;

    void initStreamIfNeeded();
};

}
