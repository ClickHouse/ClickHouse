#pragma once

#include <IO/SeekableReadBuffer.h>
#include <Common/PODArray.h>

#include <memory>
#include <string_view>
#include <vector>

namespace DB
{

class TemporaryDataBuffer;

/// Reads frames of a complete temporary file at known positions. A frame is a compressed block of the file,
/// as `CompressedWriteBuffer` writes it with the `NONE` codec: its checksum, the header of the codec and the
/// data as it is. The reader checks the checksum of each frame and returns its data where it read it.
///
/// The reader gets the positions of all the frames to read at once, so that one read of the file takes the
/// frames that are close to each other together. A reader serves one thread, and the readers of all threads
/// share the file through one `SharedFile`.
class TemporaryFileFrameReader
{
public:
    /// The frame from `offset` up to `end` in the file.
    struct Frame
    {
        size_t offset;
        size_t end;
    };

    /// A complete file that the readers of all threads share. A file that reads at an offset is opened once
    /// here, and the readers read it concurrently (see `SeekableReadBuffer::readBigAt`), so that a reader
    /// neither opens the file nor allocates a buffer. Any other file has to seek, so each reader opens it on
    /// its own. The file outlives this object, which outlives its readers.
    class SharedFile
    {
    public:
        explicit SharedFile(TemporaryDataBuffer & file_);

    private:
        friend class TemporaryFileFrameReader;

        TemporaryDataBuffer & file;

        /// The file opened once if it reads at an offset, or null if it has to seek.
        std::unique_ptr<SeekableReadBuffer> reader;
    };

    /// Prepares to read `frames_` of `shared_file_`, which ascend and do not overlap.
    TemporaryFileFrameReader(const SharedFile & shared_file_, std::vector<Frame> frames_);

    /// Returns the data of the frame at `offset`, one of the frames of the reader. Calls come in ascending
    /// order of offsets, so that a read of the file also serves the frames after the one that it starts at.
    /// The data stays valid until the next call.
    std::string_view read(size_t offset);

private:
    /// A read of the file takes at most this many bytes, which bounds the memory of the reader. When
    /// the frames to read follow each other, as for lookups of sorted keys, each read takes about this
    /// many bytes.
    static constexpr size_t max_range_bytes = 128 * 1024;

    /// A read takes the next frame only if at most this many bytes lie between it and the read. Each read of
    /// the file has a fixed cost: a system call, and the counters that the buffer of the file updates for
    /// each read, which the readers of all threads share. Reading up to this many bytes that the caller does
    /// not need costs less than another read.
    static constexpr size_t max_gap_bytes = 32 * 1024;

    /// Reads the range of the file that starts with the frame at `position`. The range then takes each next
    /// frame, until one is more than `max_gap_bytes` after the range or does not fit in `max_range_bytes`.
    void readRange();

    /// Reads `bytes` bytes of the file at `offset` into `to`: at the offset through the file that the readers
    /// share, or after a seek through the reader's own one.
    void readFile(char * to, size_t bytes, size_t offset);

    /// Returns the data of `frame`, after it checks the checksum of the frame.
    static std::string_view unwrapFrame(std::string_view frame);

    /// The shared file outlives the reader.
    const SharedFile & shared_file;

    /// The frames to read, and the index of the frame that `read` returned last, where `read` starts looking
    /// for the next one.
    std::vector<Frame> frames;
    size_t position = 0;

    /// A file that reads at an offset puts each range straight into `range` through the reader of
    /// `shared_file`, and `own_reader` is null. Any other file seeks to the range and reads it through the
    /// buffer of `own_reader`, which holds a whole range.
    std::unique_ptr<SeekableReadBuffer> own_reader;

    /// The range of the file that the reader read last holds the bytes from `range_offset` up to `range_end`.
    /// One buffer serves all the ranges, so it grows to the largest range instead of allocating each one.
    PODArray<char> range;
    size_t range_offset = 0;
    size_t range_end = 0;
};

}
