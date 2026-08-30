#pragma once

#include <memory>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Formats/MarkInCompressedFile.h>


namespace DB
{

class VectorSimilarityIndexCache;
class SkippingIndexCache;
struct SkippingIndexCacheCell;

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
        SkippingIndexCache * skipping_index_cache,
        MergeTreeReaderSettings settings_);
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
    SkippingIndexCache * skipping_index_cache;
    MergeTreeReaderSettings settings;

    /// Set in the constructor and never changed afterwards: switching to the uncached path mid-way
    /// would deserialize into a granule that is shared with the cache.
    bool use_skipping_index_cache = false;
    String skipping_index_cache_key_prefix;
    size_t current_block_number = 0;
    std::shared_ptr<SkippingIndexCacheCell> current_block;

    StreamMap streams;
    std::vector<std::unique_ptr<MergeTreeReaderStream>> stream_holders;

    uint8_t version = 0;
    size_t stream_mark = 0;

    void initStreamIfNeeded();
    void loadGranule(MergeTreeIndexGranulePtr & res, size_t mark, const IMergeTreeIndexCondition * condition, const MarkRanges * readable_ranges);
    MergeTreeIndexGranules loadBlockOfGranules(size_t block_number);
};

}
