#pragma once

#include <Compression/CompressedReadBufferBase.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
class MergeTreeReaderStream;

/** Reads columns of a wide part by their marks, as `MergeTreeReaderWide` reads them but without the uncompressed cache,
  * for a consumer that decompresses them elsewhere, such as a GPU: as the compressed blocks they are stored in.
  *
  * It reads by ranges of marks, each range of every column from the block its first row is in, by the marks of the
  * column's streams, so that the consumer has every column of exactly the rows of a range once `finishRange` tells it.
  * The columns of a range are read together, always the one that is furthest behind in rows.
  *
  * A column of values of varying width - a `String` - is two streams: its bytes, in the column's own file, and their
  * sizes, in the `.size` stream, which are read a little ahead of the bytes. Its sizes are of exactly the rows of a
  * range; its bytes, whose ends the sizes tell, are the blocks of the range whole, past the end of the range in its last.
  */
class MergeTreeCompressedColumnsReader
{
public:
    enum class Stream
    {
        /// The column's own file: its values, or the bytes of a column of varying width.
        Values,
        /// Of a column of varying width, the `.size` stream: a `UInt64` per row.
        Sizes,
    };

    class Consumer
    {
    public:
        virtual ~Consumer() = default;

        /// A block of a stream of column `column`, as it is stored. Of its decompressed bytes, the column takes `kept`
        /// from `skipped` on: a read of a range of marks takes a block whole, and the rows before and after the range
        /// out of it. `remaining_bytes` is about what is left of the stream, compressed, to size what the consumer keeps.
        virtual void addCompressedBlock(
            size_t column, Stream stream, const CompressedReadBufferBase::CompressedBlock & block, size_t skipped, size_t kept, size_t remaining_bytes)
            = 0;


        /// The range read ends: every column has `rows` rows of it.
        virtual void finishRange(size_t rows) = 0;

        /// Between pieces, for the consumer to stop the read by throwing.
        virtual void checkCancelled() = 0;
    };

    /// Reads the `ranges` of `part`, one range of every row where there are none, as the compressed blocks of its columns.
    MergeTreeCompressedColumnsReader(
        const IMergeTreeDataPart & part,
        const std::vector<NameAndTypePair> & columns,
        const MergeTreeReaderSettings & settings,
        const MarkRanges & ranges);

    ~MergeTreeCompressedColumnsReader();

    /// Reads every column to `consumer`, range after range, and returns the number of rows read.
    size_t read(Consumer & consumer);

    /// Reads the next piece to `consumer`: a compressed block of the column furthest behind. Returns false
    /// once every range is read; `rowsRead` is then the number of rows read.
    bool readNext(Consumer & consumer);
    size_t rowsRead() const { return rows_read; }

    /// Of a part on a remote disk with `remote_filesystem_read_prefetch`: starts the first range and prefetches the first
    /// piece of each column, for a part opened ahead. Reading prefetches the next piece of a column after each one.
    void prefetchStart();

    /// The stream of `column` of a wide part at `substream_path`, read by its `ranges`, every mark where there are none.
    static std::unique_ptr<MergeTreeReaderStream> openStream(
        const IMergeTreeDataPart & part,
        const NameAndTypePair & column,
        const MergeTreeReaderSettings & settings,
        const MarkRanges & ranges,
        const ISerialization::SubstreamPath & substream_path = {});

    static ISerialization::SubstreamPath stringSizesPath();

    /// The decompressed bytes of the stream of `column` at `substream_path` in `part`.
    static size_t streamBytesOf(const IMergeTreeDataPart & part, const NameAndTypePair & column, const ISerialization::SubstreamPath & substream_path);

private:
    struct RangeStream;
    struct ColumnReader;

    /// Starts the next range of rows, returns false if there is none.
    bool startNextRange();
    /// Reads one piece of the current range, returns false once the range is read.
    bool readPiece(Consumer & consumer);
    /// Checks that every column has exactly `num_rows` rows of the range read.
    void checkRowsRead(size_t num_rows) const;

    const IMergeTreeDataPart & part;
    const MarkRanges ranges;
    const bool prefetches;
    std::vector<std::unique_ptr<ColumnReader>> readers;

    size_t next_range = 0;
    /// The rows of the range being read.
    std::optional<size_t> range_rows;
    size_t rows_read = 0;
};

}
