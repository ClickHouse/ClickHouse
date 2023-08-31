#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/SecondaryIndexCache.h>

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

    return std::make_unique<MergeTreeReaderStream>(
        std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>()),
        index->getFileName(), extension, marks_count,
        all_mark_ranges,
        std::move(settings), mark_cache, uncompressed_cache,
        part->getFileSizeOrZero(index->getFileName() + extension),
        &part->index_granularity_info,
        ReadBufferFromFileBase::ProfileCallback{}, CLOCK_MONOTONIC_COARSE, false, load_marks_threadpool);
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
    SecondaryIndexCache * secondary_index_cache_,
    MergeTreeReaderSettings settings_)
    : index(std::move(index_)), part(std::move(part_)), marks_count(marks_count_)
    , all_mark_ranges(all_mark_ranges_), mark_cache(mark_cache_)
    , uncompressed_cache(uncompressed_cache_), secondary_index_cache(secondary_index_cache_)
    , settings(std::move(settings_))
{}

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

MergeTreeIndexReader::~MergeTreeIndexReader() = default;

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
    UInt128 key = SecondaryIndexCache::hash(
        part->getDataPartStorage().getFullPath(),
        index->getFileName(),
        mark);
    return secondary_index_cache->getOrSet(key, load_func);
}

}
