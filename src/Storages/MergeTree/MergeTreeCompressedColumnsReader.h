#pragma once

#include <Compression/CompressedReadBufferBase.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>

#include <memory>
#include <span>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
class MergeTreeReaderStream;

/** Reads columns of a wide part by their marks, as `MergeTreeReaderWide` reads them but without the uncompressed cache,
  * for a consumer that decompresses them elsewhere, such as a GPU: as the compressed blocks they are stored in, or as
  * the bytes of their values decompressed on the host, a piece at a time.
  *
  * The columns are read together, always the one that is furthest behind in rows, so that the consumer has them within
  * a block of each other and what waits there for the others is at most a block per column.
  *
  * A column of values of varying width - a `String` - is two streams: its bytes, in the column's own file, which are
  * read like the values of any other column, and their sizes, in the `.size` stream, which are read a little ahead of
  * the bytes. How many of its rows are read is told by the bytes read, as the rows are as long as the average one. Such
  * a column is read of whole parts only.
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

        /// Room for at most `max_bytes` decompressed bytes of a stream of column `column`, of `remaining_bytes` left of
        /// it, of which `commitBytes` then tells how many were written.
        virtual std::span<char> reserveBytes(size_t column, Stream stream, size_t max_bytes, size_t remaining_bytes) = 0;
        virtual void commitBytes(size_t column, Stream stream, size_t bytes) = 0;

        /// Between pieces, for the consumer to stop the read by throwing.
        virtual void checkCancelled() = 0;
    };

    /// Reads the `ranges` of `part`, every row where there are none, as the compressed blocks of its columns when
    /// `compressed`, and as their decompressed bytes otherwise.
    MergeTreeCompressedColumnsReader(
        const IMergeTreeDataPart & part,
        const std::vector<NameAndTypePair> & columns,
        const MergeTreeReaderSettings & settings,
        bool compressed,
        const MarkRanges & ranges);

    ~MergeTreeCompressedColumnsReader();

    /// Reads every column to `consumer`, range after range, and returns the number of rows read.
    size_t read(Consumer & consumer);

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
    struct ColumnReader;

    void readRows(Consumer & consumer, size_t num_rows);
    void readSizes(Consumer & consumer, size_t column_index);

    const IMergeTreeDataPart & part;
    const MarkRanges ranges;
    std::vector<std::unique_ptr<ColumnReader>> readers;
};

}
