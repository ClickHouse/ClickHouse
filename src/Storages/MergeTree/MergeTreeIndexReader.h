#pragma once

#include <memory>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Formats/MarkInCompressedFile.h>


namespace DB
{

class VectorSimilarityIndexCache;

class MergeTreeIndexReader
{
public:
    using StreamMap = std::map<MergeTreeIndexSubstream::Type, MergeTreeReaderStream *>;

    MergeTreeIndexReader(
        MergeTreeIndexPtr index_,
        MergeTreeDataPartInfoForReaderPtr data_part_info_,
        size_t marks_count_,
        const MarkRanges & all_mark_ranges_,
        MarkCache * mark_cache,
        UncompressedCache * uncompressed_cache,
        VectorSimilarityIndexCache * vector_similarity_index_cache,
        MergeTreeReaderSettings settings_,
        /// Only readers whose caller tolerates a cancellation exception may pass true: a throw
        /// from the marks read is reported as a corrupt part by readers that validate or load parts.
        bool interruptible_marks_read_);
    virtual ~MergeTreeIndexReader();

    void read(size_t mark, const IMergeTreeIndexCondition * condition, MergeTreeIndexGranulePtr & granule, const MarkRanges * readable_ranges);
    void read(size_t mark, size_t current_granule_num, MergeTreeIndexBulkGranulesPtr & granules);
    void adjustRightMark(size_t right_mark);
    void prefetchBeginOfRange(size_t from_mark, Priority priority);
    const StreamMap & getStreams() { return streams; }
    static MergeTreeReaderSettings patchSettings(MergeTreeReaderSettings settings, MergeTreeIndexSubstream::Type substream);

private:
    MergeTreeIndexPtr index;
    MergeTreeDataPartInfoForReaderPtr data_part_info;
    size_t marks_count;
    MarkRanges all_mark_ranges;
    MarkCache * mark_cache;
    UncompressedCache * uncompressed_cache;
    VectorSimilarityIndexCache * vector_similarity_index_cache;
    MergeTreeReaderSettings settings;
    const bool interruptible_marks_read;

    StreamMap streams;
    std::vector<std::unique_ptr<MergeTreeReaderStream>> stream_holders;

    uint8_t version = 0;
    size_t stream_mark = 0;

    void initStreamIfNeeded();
};

}
