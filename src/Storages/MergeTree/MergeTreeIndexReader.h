#pragma once

#include <memory>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
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
        MergeTreeData::DataPartPtr part_,
        size_t marks_count_,
        const MarkRanges & all_mark_ranges_,
        MarkCache * mark_cache,
        UncompressedCache * uncompressed_cache,
        VectorSimilarityIndexCache * vector_similarity_index_cache,
        MergeTreeReaderSettings settings_);
    virtual ~MergeTreeIndexReader();

    void read(size_t mark, const IMergeTreeIndexCondition * condition, MergeTreeIndexGranulePtr & granule);
    void read(size_t mark, size_t current_granule_num, MergeTreeIndexBulkGranulesPtr & granules);

    /// Read `[mark_begin, mark_end)` into `granules` in a single deserialization call.
    /// Equivalent to calling `read(mark_begin + i, i, granules)` for each `i` in
    /// `[0, mark_end - mark_begin)` but with one virtual dispatch and one position adjustment
    /// (the underlying `CompressedReadBuffer` streams through adjacent blocks as needed).
    /// For the bulk minmax path this reduces per-granule overhead to near-zero; compressed
    /// block boundaries are still handled transparently by the reader stream.
    void readRange(size_t mark_begin, size_t mark_end, MergeTreeIndexBulkGranulesPtr & granules);

    void adjustRightMark(size_t right_mark);
    void prefetchBeginOfRange(size_t from_mark, Priority priority);
    const StreamMap & getStreams() { return streams; }
    static MergeTreeReaderSettings patchSettings(MergeTreeReaderSettings settings, MergeTreeIndexSubstream::Type substream);

private:
    MergeTreeIndexPtr index;
    MergeTreeData::DataPartPtr part;
    size_t marks_count;
    MarkRanges all_mark_ranges;
    MarkCache * mark_cache;
    UncompressedCache * uncompressed_cache;
    VectorSimilarityIndexCache * vector_similarity_index_cache;
    MergeTreeReaderSettings settings;

    StreamMap streams;
    std::vector<std::unique_ptr<MergeTreeReaderStream>> stream_holders;

    uint8_t version = 0;
    size_t stream_mark = 0;

    void initStreamIfNeeded();
};

}
