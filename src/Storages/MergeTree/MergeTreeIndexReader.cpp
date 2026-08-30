#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>
#include <Storages/MergeTree/SkippingIndexCache.h>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::unique_ptr<MergeTreeReaderStream> makeIndexReaderStream(
    const String & stream_name,
    const String & extension,
    const MergeTreeDataPartInfoForReaderPtr & data_part_info,
    size_t marks_count,
    const MarkRanges & all_mark_ranges,
    MarkCache * mark_cache,
    UncompressedCache * uncompressed_cache,
    MergeTreeReaderSettings settings)
{
    auto context = data_part_info->getContext();
    auto * load_marks_threadpool = settings.load_marks_asynchronously ? &context->getLoadMarksThreadpool() : nullptr;

    const auto & index_granularity_info = data_part_info->getIndexGranularityInfo();
    auto marks_loader = std::make_shared<MergeTreeMarksLoader>(
        data_part_info,
        mark_cache,
        index_granularity_info.getMarksFilePath(stream_name),
        marks_count,
        index_granularity_info,
        settings.save_marks_in_cache,
        settings.read_settings,
        load_marks_threadpool,
        /*num_columns_in_mark=*/ 1,
        settings.use_streaming_marks_compression);

    marks_loader->startAsyncLoad();

    /// Mirrors IMergeTreeDataPart::getFileSizeOrZeroResolved: the on-disk name (original or hashed)
    /// comes from checksums, and a stream with no checksums entry is sized via the storage.
    const auto & part_storage = *data_part_info->getDataPartStorage();
    size_t data_file_size = 0;
    if (auto actual = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, data_part_info->getChecksums()))
    {
        data_file_size = data_part_info->getFileSizeOrZero(*actual + extension);
    }
    else
    {
        const String file_name = stream_name + extension;
        data_file_size = part_storage.existsFile(file_name) ? part_storage.getFileSize(file_name) : 0;
    }

    return std::make_unique<MergeTreeReaderStreamSingleColumn>(
        data_part_info->getDataPartStorage(),
        stream_name,
        extension,
        marks_count,
        all_mark_ranges,
        std::move(settings),
        uncompressed_cache,
        data_file_size,
        std::move(marks_loader),
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);
}

MergeTreeIndexReader::MergeTreeIndexReader(
    MergeTreeIndexPtr index_,
    MergeTreeDataPartInfoForReaderPtr data_part_info_,
    size_t marks_count_,
    const MarkRanges & all_mark_ranges_,
    MarkCache * mark_cache_,
    UncompressedCache * uncompressed_cache_,
    VectorSimilarityIndexCache * vector_similarity_index_cache_,
    SkippingIndexCache * skipping_index_cache_,
    MergeTreeReaderSettings settings_)
    : index(index_)
    , data_part_info(std::move(data_part_info_))
    , marks_count(marks_count_)
    , all_mark_ranges(all_mark_ranges_)
    , mark_cache(mark_cache_)
    , uncompressed_cache(uncompressed_cache_)
    , vector_similarity_index_cache(vector_similarity_index_cache_)
    , skipping_index_cache(skipping_index_cache_)
    , settings(std::move(settings_))
{
    auto concrete_part = data_part_info->getDataPart();
    if (concrete_part && concrete_part->getState() == MergeTreeDataPartState::Active)
        cache_key_prefix = concrete_part->getIndexCacheKeyPrefix();

    /// Decided once here: switching to the uncached path later would deserialize into a granule shared with the cache.
    if (cache_key_prefix.empty() || !index->supportsGranuleCache() || (skipping_index_cache && skipping_index_cache->maxSizeInBytes() == 0))
        skipping_index_cache = nullptr;
    else
        skipping_index_cache_key = {cache_key_prefix, index->getFileName(), std::numeric_limits<size_t>::max()};
}

MergeTreeIndexReader::~MergeTreeIndexReader() = default;

void MergeTreeIndexReader::initStreamIfNeeded()
{
    if (!streams.empty())
        return;

    const auto & checksums = data_part_info->getChecksums();
    auto index_format = index->getDeserializedFormat(*data_part_info, index->getFileName());
    auto index_name = index->getFileName();

    /// Blocks of granules are loaded into the cache as a whole, so the stream must cover whole blocks.
    MarkRanges widened_mark_ranges;
    if (skipping_index_cache)
    {
        for (const auto & range : all_mark_ranges)
        {
            auto begin = SkippingIndexCache::blockRange(range.begin / SkippingIndexCache::GRANULES_PER_ENTRY, marks_count).begin;
            auto end = SkippingIndexCache::blockRange((range.end - 1) / SkippingIndexCache::GRANULES_PER_ENTRY, marks_count).end;
            if (!widened_mark_ranges.empty() && widened_mark_ranges.back().end >= begin)
                widened_mark_ranges.back().end = std::max(widened_mark_ranges.back().end, end);
            else
                widened_mark_ranges.emplace_back(begin, end);
        }
    }
    const MarkRanges & stream_mark_ranges = skipping_index_cache ? widened_mark_ranges : all_mark_ranges;
    auto last_mark = getLastMark(stream_mark_ranges);

    for (const auto & substream : index_format.substreams)
    {
        auto full_stream_name = index_name + substream.suffix;
        auto stream_name_opt = DB::IMergeTreeDataPart::getStreamNameOrHash(full_stream_name, substream.extension, checksums);

        /// If the stream doesn't exist (neither original nor hashed name), use the full name
        /// and let it fail later when trying to open the file. This preserves the original error
        /// behavior and compatibility - the error message will indicate the missing file path.
        auto stream_name = stream_name_opt.value_or(full_stream_name);

        auto stream = makeIndexReaderStream(
            stream_name,
            substream.extension,
            data_part_info,
            marks_count,
            stream_mark_ranges,
            mark_cache,
            uncompressed_cache,
            patchSettings(settings, substream.type));

        stream->adjustRightMark(last_mark);
        stream->seekToStart();

        streams[substream.type] = stream.get();
        stream_holders.emplace_back(std::move(stream));
    }

    version = index_format.version;
}

void MergeTreeIndexReader::loadGranule(MergeTreeIndexGranulePtr & res, size_t mark, const IMergeTreeIndexCondition * condition, const MarkRanges * readable_ranges)
{
    initStreamIfNeeded();

    if (stream_mark != mark)
    {
        for (const auto & stream : stream_holders)
            stream->seekToMark(mark);
    }

    if (!res)
        res = index->createIndexGranule();

    MergeTreeIndexDeserializationState state
    {
        .version = version,
        .condition = condition,
        .part_info = *data_part_info,
        .index = *index,
        .readable_ranges = readable_ranges,
        .skip_postings_deserialization = false,
    };

    res->deserializeBinaryWithMultipleStreams(streams, state);
    stream_mark = mark + 1;
}

MergeTreeIndexGranules MergeTreeIndexReader::loadBlockOfGranules(size_t block_number)
{
    auto block = SkippingIndexCache::blockRange(block_number, marks_count);

    MergeTreeIndexGranules granules;
    granules.reserve(block.end - block.begin);
    for (size_t mark = block.begin; mark < block.end; ++mark)
    {
        MergeTreeIndexGranulePtr granule;
        loadGranule(granule, mark, /*condition=*/ nullptr, /*readable_ranges=*/ nullptr);
        granules.push_back(std::move(granule));
    }
    return granules;
}

void MergeTreeIndexReader::read(size_t mark, const IMergeTreeIndexCondition * condition, MergeTreeIndexGranulePtr & granule, const MarkRanges * readable_ranges)
{
    if (skipping_index_cache)
    {
        /// One cache lookup per block of granules. The granules are shared with other readers, so they must never be modified.
        size_t block_number = mark / SkippingIndexCache::GRANULES_PER_ENTRY;
        if (skipping_index_cache_key.block_number != block_number)
        {
            auto key = skipping_index_cache_key;
            key.block_number = block_number;
            current_block = skipping_index_cache->getOrSet(key, [this, block_number] { return loadBlockOfGranules(block_number); });
            skipping_index_cache_key.block_number = block_number;
        }

        granule = current_block->granules.at(mark % SkippingIndexCache::GRANULES_PER_ENTRY);
        return;
    }

    /// Not all skip indexes are created equal. Vector similarity indexes typically have a high index granularity (e.g. GRANULARITY
    /// 1000000), and as a result they tend to be very large (hundreds of megabytes). Besides IO, repeated de-serialization consumes lots of
    /// CPU cycles as the on-disk and the in-memory format differ. We therefore keep the deserialized vector similarity granules in a cache.
    ///
    /// The same cannot be done per granule for other skip indexes. Because their GRANULARITY is small (e.g. 1), the sheer number of skip
    /// index granules would create too much lock contention in the cache (this was learned the hard way). Instead, indexes which support it
    /// are cached in blocks of granules by the skipping index cache above.
    if (index->isVectorSimilarityIndex() && !cache_key_prefix.empty())
    {
        VectorSimilarityIndexCacheKey key{cache_key_prefix, index->getFileName(), mark};
        granule = vector_similarity_index_cache->getOrSet(key, [&](auto & res) { loadGranule(res, mark, condition, readable_ranges); });
    }
    else
    {
        loadGranule(granule, mark, condition, readable_ranges);
    }
}

void MergeTreeIndexReader::read(size_t mark, size_t current_granule_num, MergeTreeIndexBulkGranulesPtr & granules)
{
    if (granules == nullptr)
        granules = index->createIndexBulkGranules();

    initStreamIfNeeded();
    if (streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bulk filtering is not supported for indexes with multiple streams. Have {} streams for index {}", streams.size(), index->getFileName());

    auto * stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    if (stream_mark != mark)
        stream->seekToMark(mark);

    granules->deserializeBinary(current_granule_num, *stream->getDataBuffer(), version);
    stream_mark = mark + 1;
}

void MergeTreeIndexReader::adjustRightMark(size_t right_mark)
{
    for (const auto & stream : stream_holders)
        stream->adjustRightMark(right_mark);
}

MergeTreeReaderSettings MergeTreeIndexReader::patchSettings(MergeTreeReaderSettings settings, MergeTreeIndexSubstream::Type substream)
{
    using enum MergeTreeIndexSubstream::Type;
    settings.is_compressed = MergeTreeIndexSubstream::isCompressed(substream);

    /// Adjust read buffer sizes for text index dictionaries and postings
    /// because usually we read relatively small amounts of data from random places of
    /// these substreams. So, it doesn't make sense to read more data in the buffer.
    if (substream == TextIndexDictionary || substream == TextIndexPostings)
    {
        settings.read_settings.local_fs_settings.buffer_size = 16 * 1024;
        settings.read_settings.remote_fs_settings.buffer_size = 16 * 1024;
    }

    return settings;
}

}
