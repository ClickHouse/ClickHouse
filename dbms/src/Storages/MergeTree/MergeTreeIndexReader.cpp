#include <Storages/MergeTree/MergeTreeIndexReader.h>


namespace DB
{

MergeTreeIndexReader::MergeTreeIndexReader(
    MergeTreeIndexPtr index, MergeTreeData::DataPartPtr part, size_t marks_count, const MarkRanges & all_mark_ranges)
    : index(index), stream(
        part->getFullPath() + index->getFileName(), ".idx", marks_count,
        all_mark_ranges, nullptr, false, nullptr,
        part->getFileSizeOrZero(index->getFileName() + ".idx"), 0, DBMS_DEFAULT_BUFFER_SIZE,
        &part->storage.index_granularity_info,
        ReadBufferFromFileBase::ProfileCallback{}, CLOCK_MONOTONIC_COARSE)
{
    stream.seekToStart();
}

void MergeTreeIndexReader::seek(size_t mark)
{
    stream.seekToMark(mark);
}

MergeTreeIndexGranulePtr MergeTreeIndexReader::read()
{
    auto granule = index->createIndexGranule();
    granule->deserializeBinary(*stream.data_buffer);
    return granule;
}

}
