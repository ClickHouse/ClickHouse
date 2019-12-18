#include <Storages/MergeTree/MergeTreeIndexReader.h>


namespace DB
{

MergeTreeIndexReader::MergeTreeIndexReader(
    MergeTreeIndexPtr index_, MergeTreeData::DataPartPtr part_, size_t marks_count_, const MarkRanges & all_mark_ranges_)
    : index(index_), stream(
        part_->getFullPath() + index->getFileName(), ".idx", marks_count_,
        all_mark_ranges_,
        { 0, DBMS_DEFAULT_BUFFER_SIZE, false}, nullptr, nullptr,
        part_->getFileSizeOrZero(index->getFileName() + ".idx"),
        &part_->index_granularity_info,
        MergeTreeReaderStream::ReadingMode::INDEX,
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
    std::cerr << "(MergeTreeIndexReader) granule.empty(): " << granule->empty() << "\n";
    return granule;
}

}
