#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::unique_ptr<MergeTreeReaderStream> makeIndexReaderStream(
    const String & stream_name,
    const String & extension,
    MergeTreeData::DataPartPtr part,
    size_t marks_count,
    const MarkRanges & all_mark_ranges,
    MarkCache * mark_cache,
    UncompressedCache * uncompressed_cache,
    MergeTreeReaderSettings settings)
{
    auto context = part->storage.getContext();
    auto * load_marks_threadpool = settings.load_marks_asynchronously ? &context->getLoadMarksThreadpool() : nullptr;

    auto marks_loader = std::make_shared<MergeTreeMarksLoader>(
        std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>()),
        mark_cache,
        part->index_granularity_info.getMarksFilePath(stream_name),
        marks_count,
        part->index_granularity_info,
        settings.save_marks_in_cache,
        settings.read_settings,
        load_marks_threadpool,
        /*num_columns_in_mark=*/ 1);

    marks_loader->startAsyncLoad();

    return std::make_unique<MergeTreeReaderStreamSingleColumn>(
        part->getDataPartStoragePtr(),
        stream_name,
        extension,
        marks_count,
        all_mark_ranges,
        std::move(settings),
        uncompressed_cache,
        part->getFileSizeOrZero(stream_name + extension),
        std::move(marks_loader),
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);
}

MergeTreeIndexReader::MergeTreeIndexReader(
    MergeTreeIndexPtr index_,
    MergeTreeData::DataPartPtr part_,
    size_t marks_count_,
    const MarkRanges & all_mark_ranges_,
    MarkCache * mark_cache_,
    UncompressedCache * uncompressed_cache_,
    VectorSimilarityIndexCache * vector_similarity_index_cache_,
    MergeTreeReaderSettings settings_)
    : index(index_)
    , part(std::move(part_))
    , marks_count(marks_count_)
    , all_mark_ranges(all_mark_ranges_)
    , mark_cache(mark_cache_)
    , uncompressed_cache(uncompressed_cache_)
    , vector_similarity_index_cache(vector_similarity_index_cache_)
    , settings(std::move(settings_))
{
}

MergeTreeIndexReader::~MergeTreeIndexReader() = default;

void MergeTreeIndexReader::initStreamIfNeeded()
{
    if (!streams.empty())
        return;

    auto index_format = index->getDeserializedFormat(part->checksums, index->getFileName());
    auto index_name = index->getFileName();
    auto last_mark = getLastMark(all_mark_ranges);

    for (const auto & substream : index_format.substreams)
    {
        auto stream_name = index_name + substream.suffix;

        auto stream = makeIndexReaderStream(
            stream_name,
            substream.extension,
            part,
            marks_count,
            all_mark_ranges,
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

void MergeTreeIndexReader::read(size_t mark, const IMergeTreeIndexCondition * condition, MergeTreeIndexGranulePtr & granule)
{
    auto load_func = [this, mark, condition](auto & res)
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
            .part = *part,
            .index = *index,
        };

        res->deserializeBinaryWithMultipleStreams(streams, state);
        stream_mark = mark + 1;
    };

    /// Not all skip indexes are created equal. Vector similarity indexes typically have a high index granularity (e.g. GRANULARITY
    /// 1000000), and as a result they tend to be very large (hundreds of megabytes). Besides IO, repeated de-serialization consumes lots of
    /// CPU cycles as the on-disk and the in-memory format differ. We therefore keep the deserialized vector similarity granules in a cache.
    ///
    /// The same cannot be done for other skip indexes. Because their GRANULARITY is small (e.g. 1), the sheer number of skip index granules
    /// would create too much lock contention in the cache (this was learned the hard way).
    if (index->isVectorSimilarityIndex())
    {
        VectorSimilarityIndexCacheKey key{part->getDataPartStorage().getDiskName() + ":" + part->getDataPartStorage().getFullPath(),
                                          index->getFileName(),
                                          mark};

        granule = vector_similarity_index_cache->getOrSet(key, load_func);
    }
    else
    {
        load_func(granule);
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
        settings.read_settings.local_fs_buffer_size = 16 * 1024;
        settings.read_settings.remote_fs_buffer_size = 16 * 1024;
    }

    return settings;
}

}
