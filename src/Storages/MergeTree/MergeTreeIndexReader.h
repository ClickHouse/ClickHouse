#pragma once

#include <memory>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Formats/MarkInCompressedFile.h>
#include <Storages/MergeTree/SkippingIndexCache.h>


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

    /// Empty if the part is not Active: such parts are removed soon, so their granules are not cached.
    String cache_key_prefix;
    /// Only the block number changes between lookups.
    SkippingIndexCacheKey skipping_index_cache_key;
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
