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

MergeTreeIndexReader::~MergeTreeIndexReader() = default;

void MergeTreeIndexReader::seek(size_t mark)
{
    stream->seekToMark(mark);
}

MergeTreeIndexGranulePtr MergeTreeIndexReader::read()
{
    auto granule = index->createIndexGranule();
    granule->deserializeBinary(*stream->getDataBuffer(), version);
    return granule;
}

}
