#pragma once

#include <base/types.h>
#include <base/defines.h>
#include <Common/PODArray.h>
#include <memory>

namespace DB
{

class MergeTreeReaderStream;
class MergeTreeIndexGranularity;

/// Document lengths (`SmallFloat` bytes, one per row) of a part for BM25 scoring.
/// The `.dl` substream is read like a plain UInt8 column.
class TextIndexDocLengthsReader
{
public:
    /// Reads the `.dl` substream of a part.
    TextIndexDocLengthsReader(std::unique_ptr<MergeTreeReaderStream> stream_, const MergeTreeIndexGranularity & index_granularity_, size_t num_docs_);

    /// Holds the doc lengths of a whole part in memory (index build and tests).
    explicit TextIndexDocLengthsReader(PaddedPODArray<UInt8> bytes_);
    ~TextIndexDocLengthsReader();

    UInt32 numDocs() const { return num_docs; }

    /// Loads the doc lengths of rows [row_offset, row_offset + num_rows), which start in granule `from_mark`.
    /// Consecutive calls read the stream sequentially; a gap seeks to the granule of `from_mark`.
    void readRows(size_t from_mark, size_t row_offset, size_t num_rows);

    /// Extends the readable range of the stream to `right_mark` when the reader gets new mark ranges.
    void adjustRightMark(size_t right_mark);

    /// Returns the `SmallFloat` doc-length byte of a row loaded by the last `readRows`.
    UInt8 getByte(UInt32 doc_id) const;

private:
    std::unique_ptr<MergeTreeReaderStream> stream;
    const MergeTreeIndexGranularity * index_granularity = nullptr;
    UInt32 num_docs;

    /// Rows loaded by the last `readRows` (all rows of the part for the in-memory variant).
    PaddedPODArray<UInt8> bytes;
    size_t rows_begin = 0;
    size_t rows_end = 0;
    /// False until the first read positions the stream.
    bool is_positioned = false;
};

using TextIndexDocLengthsReaderPtr = std::shared_ptr<TextIndexDocLengthsReader>;

}
