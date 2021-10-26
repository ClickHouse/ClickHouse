#include <Storages/MergeTree/MergeTreeIndexReader.h>

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
    return std::make_unique<MergeTreeReaderStream>(
        part->volume->getDisk(),
        part->getFullRelativePath() + index->getFileName(), extension, marks_count,
        all_mark_ranges,
        std::move(settings), mark_cache, uncompressed_cache,
        part->getFileSizeOrZero(index->getFileName() + extension),
        &part->index_granularity_info,
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
    const std::string & path_prefix = part_->getFullRelativePath() + index->getFileName();
    auto index_format = index->getDeserializedFormat(part_->volume->getDisk(), path_prefix);

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
    granule->deserializeBinary(*stream->data_buffer, version);
    return granule;
}

}
