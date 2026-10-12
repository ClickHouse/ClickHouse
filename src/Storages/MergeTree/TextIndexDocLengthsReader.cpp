#include <Storages/MergeTree/TextIndexDocLengthsReader.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TextIndexDocLengthsReader::TextIndexDocLengthsReader(
    std::unique_ptr<MergeTreeReaderStream> stream_,
    const MergeTreeIndexGranularity & index_granularity_,
    size_t num_docs_)
    : stream(std::move(stream_))
    , index_granularity(&index_granularity_)
    , num_docs(static_cast<UInt32>(num_docs_))
{
}

TextIndexDocLengthsReader::TextIndexDocLengthsReader(PaddedPODArray<UInt8> bytes_)
    : num_docs(static_cast<UInt32>(bytes_.size()))
    , bytes(std::move(bytes_))
    , rows_begin(0)
    , rows_end(num_docs)
    , is_positioned(true)
{
}

TextIndexDocLengthsReader::~TextIndexDocLengthsReader() = default;

void TextIndexDocLengthsReader::readRows(size_t from_mark, size_t row_offset, size_t num_rows)
{
    if (num_rows == 0)
        return;

    if (row_offset + num_rows > num_docs)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Rows [{}, {}) are out of range for TextIndexDocLengthsReader with {} docs",
            row_offset, row_offset + num_rows, num_docs);
    }

    /// The in-memory variant holds the whole part.
    if (!stream)
        return;

    /// A read step may resume in the middle of a granule, so a gap is closed by seeking
    /// to the granule's mark and skipping the rows before `row_offset`.
    if (!is_positioned || row_offset != rows_end)
    {
        stream->seekToMark(from_mark);
        stream->getDataBuffer()->ignore(row_offset - index_granularity->getMarkStartingRow(from_mark));
        is_positioned = true;
    }

    bytes.resize(num_rows);
    stream->getDataBuffer()->readStrict(reinterpret_cast<char *>(bytes.data()), num_rows);
    rows_begin = row_offset;
    rows_end = row_offset + num_rows;
}

void TextIndexDocLengthsReader::adjustRightMark(size_t right_mark)
{
    if (stream)
        stream->adjustRightMark(right_mark);
}

UInt8 TextIndexDocLengthsReader::getByte(UInt32 doc_id) const
{
    chassert(doc_id >= rows_begin && doc_id < rows_end);
    return bytes[doc_id - rows_begin];
}

}
