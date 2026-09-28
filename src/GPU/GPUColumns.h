#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::GPU
{

/// The type of a column of `type` as the device takes it, if it takes it at all.
std::optional<GPUColumnType> columnTypeOf(const IDataType & type);

GPUColumnType columnTypeOrThrow(const IDataType & type);

/** The buffers a column of one `GPUColumnType` is made of, in the order both sides keep them, and
  * how to get them out of a ClickHouse column and back into one. This is all that is particular
  * to a kind of column on the host side: the pipes and the copies back work through it.
  *
  * A buffer of offsets points into another of the column's buffers. When the blocks of a column
  * are put one after another, a block's offsets are moved by the elements of that buffer before
  * it, and on the device the offsets start with the 0 the first row starts at.
  */
struct ColumnLayout
{
    struct Buffer
    {
        size_t element_size;
        /// For a buffer of `UInt64` offsets, the buffer they point into.
        std::optional<size_t> offsets_into;
    };

    GPUColumnType type;
    std::vector<Buffer> buffers;

    static ColumnLayout of(GPUColumnType type);

    /// The buffers of a column of this layout in host memory, as they are in it: a block's
    /// offsets are the ends of its rows within the block.
    std::vector<std::string_view> buffersOf(const IColumn & column) const;

    /// The view of a column of `rows` rows whose buffers are at `data` on the device, with
    /// `elements[i]` elements in buffer `i` - the offsets with their leading 0.
    DeviceColumnView viewOf(std::span<char * const> data, size_t rows, std::span<const size_t> elements) const;

    /// Where each buffer of `view` is on the device, and how many bytes of it there are.
    std::vector<std::pair<const char *, size_t>> buffersOf(const DeviceColumnView & view) const;

    /// Puts `rows` rows into an empty `column` from its buffers as the device keeps them.
    void fill(IColumn & column, size_t rows, std::span<const std::string_view> from) const;
};

/** Copies columns from the device into host columns: `add` queues the copies of a column's
  * buffers into pinned memory on the stream, and `finish` waits for the stream and puts them into
  * the columns.
  */
class ColumnDownload
{
public:
    explicit ColumnDownload(rmm::cuda_stream_view stream_) : stream(stream_) { }

    void add(const DeviceColumnView & view, IColumn & column);

    void finish();

private:
    struct Pending
    {
        ColumnLayout layout;
        IColumn * column;
        size_t rows;
        std::vector<PinnedBuffer> buffers;
    };

    const rmm::cuda_stream_view stream;
    std::vector<Pending> pending;
};

}

#endif
