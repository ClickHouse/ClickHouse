#include <Interpreters/TemporaryFileFrameReader.h>

#include <Compression/CompressedReadBufferBase.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_READ_ALL_DATA;
}

TemporaryFileFrameReader::SharedFile::SharedFile(TemporaryDataBuffer & file_)
    : file(file_)
    , reader(file.readRaw(max_range_bytes)) /// Its buffer stays unused if the file reads at an offset
{
    /// `readBigAt` requires a call of `supportsReadAt` first, which must not run concurrently with it. The
    /// readers of the file start after the constructor.
    if (!reader->supportsReadAt())
        reader.reset();
}

TemporaryFileFrameReader::TemporaryFileFrameReader(const SharedFile & shared_file_, std::vector<Frame> frames_)
    : shared_file(shared_file_)
    , frames(std::move(frames_))
    , own_reader(shared_file.reader ? nullptr : shared_file.file.readRaw(max_range_bytes))
{
    chassert(!frames.empty()); /// The caller creates a reader only to read frames
    const auto overlaps_next = [](const Frame & frame, const Frame & next) { return frame.end > next.offset; };
    chassert(std::ranges::adjacent_find(frames, overlaps_next) == frames.end()); /// The frames ascend and do not overlap
}

std::string_view TemporaryFileFrameReader::read(size_t offset)
{
    /// Calls come in ascending order of offsets, so the search starts at the frame of the previous call.
    position = static_cast<size_t>(std::ranges::find(frames.begin() + position, frames.end(), offset, &Frame::offset) - frames.begin());
    chassert(position < frames.size()); /// The frame at `offset` is one of the frames of the reader

    /// The frame is in the range that the reader read last, or it starts the next range.
    if (offset >= range_end)
        readRange();

    const Frame & frame = frames[position];
    chassert(range_offset <= frame.offset && frame.end <= range_end); /// `readRange` took every frame up to `range_end`
    return unwrapFrame({range.data() + (frame.offset - range_offset), frame.end - frame.offset});
}

void TemporaryFileFrameReader::readRange()
{
    range_offset = frames[position].offset;
    range_end = frames[position].end;
    for (size_t i = position + 1; i < frames.size(); ++i)
    {
        if (frames[i].offset - range_end > max_gap_bytes || frames[i].end - range_offset > max_range_bytes)
            break;
        range_end = frames[i].end;
    }
    range.resize(range_end - range_offset);
    readFile(range.data(), range.size(), range_offset);
}

void TemporaryFileFrameReader::readFile(char * to, size_t bytes, size_t offset)
{
    size_t bytes_read = 0;
    if (shared_file.reader)
        bytes_read = shared_file.reader->readBigAt(to, bytes, offset, /*progress_callback=*/nullptr);
    else
    {
        /// A remote file requests the bytes up to its bound. Without a bound, a request runs to the end of
        /// the file. The seek to the next range then cancels it while its data is in flight, which can cost
        /// the connection, so the distributed cache opens a new one for each range. With the bound, the
        /// request ends with the range, which the read consumes.
        own_reader->setReadUntilPosition(offset + bytes);
        own_reader->seek(offset, SEEK_SET);
        bytes_read = own_reader->readBig(to, bytes);
    }
    if (bytes_read != bytes)
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data of temporary file {}. Bytes read: {}. Bytes expected: {}",
            shared_file.file.describeFilePath(),
            bytes_read,
            bytes);
}

std::string_view TemporaryFileFrameReader::unwrapFrame(std::string_view frame)
{
    const auto checksum = CompressedReadBufferBase::readChecksum(frame.data()); /// NOLINT(bugprone-suspicious-stringview-data-usage)
    const std::string_view data = frame.substr(sizeof(CompressedReadBufferBase::Checksum)); /// The header of the codec and the data
    CompressedReadBufferBase::validateChecksum(data.data(), data.size(), checksum, /*external_data=*/false);

    /// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
    chassert(ICompressionCodec::readMethod(data.data()) == static_cast<UInt8>(CompressionMethodByte::NONE));
    return data.substr(ICompressionCodec::getHeaderSize());
}

}
