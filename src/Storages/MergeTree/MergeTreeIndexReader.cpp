#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>

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
    MarkCache * mark_cache,
    UncompressedCache * uncompressed_cache,
    MergeTreeReaderSettings settings)
    : index(index_)
{
    auto index_format = index->getDeserializedFormat(part_->getDataPartStorage(), index->getFileName());

    stream = makeIndexReader(
        index_format.extension,
        index_,
        part_,
        marks_count_,
        all_mark_ranges_,
        mark_cache,
        uncompressed_cache,
        std::move(settings));

    version = index_format.version;

    stream->adjustRightMark(getLastMark(all_mark_ranges_));
    stream->seekToStart();
}

void MergeTreeIndexReader::seek(size_t mark)
{
    stream->seekToMark(mark);
}

void MergeTreeIndexReader::read(MergeTreeIndexGranulePtr & granule)
{
    if (granule == nullptr)
        granule = index->createIndexGranule();

    granule->deserializeBinary(*stream->getDataBuffer(), version);
}

}
