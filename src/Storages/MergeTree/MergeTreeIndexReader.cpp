#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/SecondaryIndexCache.h>
#include <IO/MMappedFileCache.h>

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
    MergeTreeReaderSettings settings,
    bool plain_file)
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
        ReadBufferFromFileBase::ProfileCallback{}, CLOCK_MONOTONIC_COARSE, false, load_marks_threadpool,
        plain_file);
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
{
}

void MergeTreeIndexReader::initStreamIfNeeded()
{
    if (stream)
        return;

    index_format = index->getDeserializedFormat(part->getDataPartStorage(), index->getFileName());

    stream = makeIndexReader(
        index_format.extension,
        index,
        part,
        marks_count,
        all_mark_ranges,
        mark_cache,
        uncompressed_cache,
        std::move(settings),
        index_format.plain_file());

    stream->adjustRightMark(getLastMark(all_mark_ranges));
    stream->seekToStart();
}

MergeTreeIndexReader::~MergeTreeIndexReader() = default;

MergeTreeIndexGranulePtr MergeTreeIndexReader::read(size_t mark)
{
    auto load_func = [&] {
        initStreamIfNeeded();

        auto granule = index->createIndexGranule();

        /// Problems with current implementation of mmapped/seekable indexes:
        ///  * The settings are taken from whichever query happened to load the index into cache.
        ///    So e.g. a query with 'skip_index_allow_mmap = 0' may use mmapped index granules that
        ///    were already in cache from previous queries.
        ///  * We unnecessarily create `stream`, which crates a useless ReadBuffer (file descriptor
        ///    and memory allocation).
        ///  * The mmapped files aren't actively removed from secondary index cache cache when files
        ///    are deleted. So file descriptors for big files are be kept open long after the files
        ///    were deleted, wasting disk space.

        auto random_access = stream->getRandomAccessForGranule(
            mark,
            settings.skip_index_allow_seekable_read && index_format.supports_view_from_seekable_file,
            settings.skip_index_allow_mmap && index_format.supports_view_from_mmapped_file);

        if (!random_access.local_path.empty())
        {
            auto mmap_cache = part->storage.getContext()->getMMappedFileCache();
            std::shared_ptr<MMappedFile> mapped;
            auto do_mmap = [&]
                {
                    return std::make_shared<MMappedFile>(
                        random_access.local_path, 0, random_access.file_size);
                };
            if (mmap_cache)
                mapped = part->storage.getContext()->getMMappedFileCache()->getOrSet(
                    MMappedFileCache::hash(random_access.local_path, 0, random_access.file_size),
                    do_mmap);
            else
                mapped = do_mmap();
            granule->viewFromMMappedFile(
                mapped, random_access.offset, random_access.length, index_format.version);
        }
        else if (random_access.seekable_buffer)
            granule->viewFromSeekableFile(
                random_access.seekable_buffer, random_access.offset, random_access.length,
                index_format.version);
        else
        {
            if (stream_mark != mark)
                stream->seekToMark(mark);
            stream_mark = mark + 1;

            granule->deserializeBinary(*stream->getDataBuffer(), index_format.version);
        }

        return granule;
    };
    UInt128 key = SecondaryIndexCache::hash(
        part->getDataPartStorage().getFullPath(),
        index->getFileName(),
        mark);
    return secondary_index_cache->getOrSet(key, load_func);
}

}
