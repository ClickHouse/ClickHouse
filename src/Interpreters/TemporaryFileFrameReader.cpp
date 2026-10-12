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

TemporaryFileFrameReader::TemporaryFileFrameReader(TemporaryDataBuffer & file_, std::vector<Frame> frames_)
    : file(file_)
    , frames(std::move(frames_))
    , in(file.readRaw(max_range_bytes))
    , supports_read_at(in->supportsReadAt())
{
    chassert(!frames.empty()); /// The caller opens the file only to read frames
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
    if (supports_read_at)
        bytes_read = in->readBigAt(to, bytes, offset, /*progress_callback=*/nullptr);
    else
    {
        in->seek(offset, SEEK_SET);
        bytes_read = in->readBig(to, bytes);
    }
    if (bytes_read != bytes)
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data of temporary file {}. Bytes read: {}. Bytes expected: {}",
            file.describeFilePath(),
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
