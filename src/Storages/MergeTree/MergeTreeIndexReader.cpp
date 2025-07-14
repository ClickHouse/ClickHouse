#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>

namespace
{

using namespace DB;

std::unique_ptr<MergeTreeReaderStream> makeIndexReader(
    const std::string & extension,
    MergeTreeIndexPtr index,
    MergeTreeData::DataPartPtr part,
    size_t marks_count,
    const MarkRanges & all_mark_ranges,
    MarkCache * mark_cache,
    UncompressedCache * uncompressed_cache,
    MergeTreeReaderSettings settings)
{
    auto context = part->storage.getContext();
    auto * load_marks_threadpool = settings.read_settings.load_marks_asynchronously ? &context->getLoadMarksThreadpool() : nullptr;

    auto marks_loader = std::make_shared<MergeTreeMarksLoader>(
        std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>()),
        mark_cache,
        part->index_granularity_info.getMarksFilePath(index->getFileName()),
        marks_count,
        part->index_granularity_info,
        settings.save_marks_in_cache,
        settings.read_settings,
        load_marks_threadpool,
        /*num_columns_in_mark=*/ 1);

    marks_loader->startAsyncLoad();

    return std::make_unique<MergeTreeReaderStreamSingleColumn>(
        part->getDataPartStoragePtr(),
        index->getFileName(), extension, marks_count,
        all_mark_ranges, std::move(settings), uncompressed_cache,
        part->getFileSizeOrZero(index->getFileName() + extension), std::move(marks_loader),
        ReadBufferFromFileBase::ProfileCallback{}, CLOCK_MONOTONIC_COARSE);
}

}

namespace DB
{

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

void MergeTreeIndexReader::initStreamIfNeeded()
{
    if (stream)
        return;

    auto index_format = index->getDeserializedFormat(part->getDataPartStorage(), index->getFileName());

    stream = makeIndexReader(
        index_format.extension,
        index,
        part,
        marks_count,
        all_mark_ranges,
        mark_cache,
        uncompressed_cache,
        std::move(settings));

    version = index_format.version;

    stream->adjustRightMark(getLastMark(all_mark_ranges));
    stream->seekToStart();
}

MergeTreeIndexGranulePtr MergeTreeIndexReader::read(size_t mark)
{
    auto load_func = [&] {
        initStreamIfNeeded();
        if (stream_mark != mark)
            stream->seekToMark(mark);

        auto granule = index->createIndexGranule();
        granule->deserializeBinary(*stream->getDataBuffer(), version);
        stream_mark = mark + 1;
        return granule;
    };

    /// Not all skip indexes are created equal. Vector similarity indexes typically have a high index granularity (e.g. GRANULARITY
    /// 1000000), and as a result they tend to be very large (hundreds of megabytes). Besides IO, repeated de-serialization consumes lots of
    /// CPU cycles as the on-disk and the in-memory format differ. We therefore keep the deserialized vector similarity granules in a cache.
    ///
    /// The same cannot be done for other skip indexes. Because their GRANULARITY is small (e.g. 1), the sheer number of skip index granules
    /// would create too much lock contention in the cache (this was learned the hard way).
    if (!index->isVectorSimilarityIndex())
        return load_func();
    else
    {
        UInt128 key = VectorSimilarityIndexCache::hash(
            part->getDataPartStorage().getFullPath(),
            index->getFileName(),
            mark);
        return vector_similarity_index_cache->getOrSet(key, load_func);
    }
}

}
