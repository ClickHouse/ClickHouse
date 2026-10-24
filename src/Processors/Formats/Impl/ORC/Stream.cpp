#include <Processors/Formats/Impl/ORC/Stream.h>

#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}

namespace DB::ORC
{

UInt64 PositionProvider::next()
{
    if (index >= positions.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not enough positions in a row index entry of ORC file");
    return positions[index++];
}


InputByteStream::InputByteStream(std::string_view data_, CompressionKind compression_, size_t block_size_, std::string name_)
    : data(data_), compression(compression_), block_size(block_size_), name(std::move(name_))
{
}

bool InputByteStream::nextChunk()
{
    while (next_chunk_offset < data.size())
    {
        if (compression == CompressionKind::NONE)
        {
            pos = data.data() + next_chunk_offset;
            end = data.data() + data.size();
            next_chunk_offset = data.size();
            return true;
        }

        if (data.size() - next_chunk_offset < COMPRESSION_HEADER_SIZE)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Truncated compression chunk header in ORC stream {}", name);

        const UInt8 * header = reinterpret_cast<const UInt8 *>(data.data() + next_chunk_offset);
        UInt32 header_value = header[0] | (header[1] << 8) | (header[2] << 16);
        bool is_original = header_value & 1;
        size_t chunk_length = header_value >> 1;
        size_t chunk_offset = next_chunk_offset + COMPRESSION_HEADER_SIZE;

        if (chunk_length > data.size() - chunk_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Compression chunk of size {} at offset {} goes beyond the end of ORC stream {} of size {}",
                chunk_length, next_chunk_offset, name, data.size());

        next_chunk_offset = chunk_offset + chunk_length;
        const char * chunk = data.data() + chunk_offset;

        if (is_original)
        {
            pos = chunk;
            end = chunk + chunk_length;
        }
        else
        {
            if (decompressed.size() < block_size)
                decompressed.resize(block_size);
            size_t size = decompressChunk(compression, chunk, chunk_length, decompressed.data(), block_size);
            pos = decompressed.data();
            end = decompressed.data() + size;
        }

        if (pos != end)
            return true;
    }

    pos = end = nullptr;
    return false;
}

void InputByteStream::readBytes(char * to, size_t n)
{
    while (n)
    {
        if (pos == end && !nextChunk())
            throwUnexpectedEnd();
        size_t count = std::min(n, static_cast<size_t>(end - pos));
        memcpy(to, pos, count);
        pos += count;
        to += count;
        n -= count;
    }
}

void InputByteStream::skipBytes(size_t n)
{
    while (n)
    {
        if (pos == end && !nextChunk())
            throwUnexpectedEnd();
        size_t count = std::min(n, static_cast<size_t>(end - pos));
        pos += count;
        n -= count;
    }
}

void InputByteStream::seek(PositionProvider & positions)
{
    if (compression == CompressionKind::NONE)
    {
        UInt64 offset = positions.next();
        if (offset > data.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Seek position {} is beyond the end of ORC stream {} of size {}", offset, name, data.size());
        pos = data.data() + offset;
        end = data.data() + data.size();
        next_chunk_offset = data.size();
        return;
    }

    UInt64 chunk_offset = positions.next();
    UInt64 uncompressed_offset = positions.next();
    if (chunk_offset > data.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Seek position {} is beyond the end of ORC stream {} of size {}", chunk_offset, name, data.size());

    next_chunk_offset = chunk_offset;
    pos = end = nullptr;
    if (!nextChunk())
    {
        if (uncompressed_offset != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Seek position {} is beyond the end of ORC stream {}", uncompressed_offset, name);
        return;
    }

    if (uncompressed_offset > static_cast<size_t>(end - pos))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Seek position {} is beyond the end of the compression chunk of size {} in ORC stream {}",
            uncompressed_offset, end - pos, name);
    pos += uncompressed_offset;
}

void InputByteStream::throwUnexpectedEnd() const
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of ORC stream {}", name);
}


OutputByteStream::OutputByteStream(Compressor * compressor_, size_t block_size_)
    : compressor(compressor_), block_size(block_size_)
{
    buffer.reserve(std::min<size_t>(block_size, 64 * 1024));
}

void OutputByteStream::write(const char * data, size_t size)
{
    while (size)
    {
        if (buffer.size() == block_size)
            flushChunk();
        size_t count = std::min(size, block_size - buffer.size());
        buffer.append(data, count);
        data += count;
        size -= count;
    }
}

void OutputByteStream::recordPosition(std::vector<UInt64> & positions) const
{
    if (compressor)
    {
        positions.push_back(output.size());
        positions.push_back(buffer.size());
    }
    else
        positions.push_back(output.size() + buffer.size());
}

void OutputByteStream::flushChunk()
{
    if (buffer.empty())
        return;

    if (!compressor)
    {
        output.append(buffer);
        buffer.clear();
        return;
    }

    size_t header_offset = output.size();
    output.append(COMPRESSION_HEADER_SIZE, '\0');
    size_t compressed_size = compressor->compress(buffer.data(), buffer.size(), output);

    UInt32 header_value;
    if (compressed_size)
        header_value = static_cast<UInt32>(compressed_size << 1);
    else
    {
        output.append(buffer);
        header_value = static_cast<UInt32>((buffer.size() << 1) | 1);
    }

    output[header_offset] = static_cast<char>(header_value & 0xFF);
    output[header_offset + 1] = static_cast<char>((header_value >> 8) & 0xFF);
    output[header_offset + 2] = static_cast<char>((header_value >> 16) & 0xFF);
    buffer.clear();
}

size_t OutputByteStream::finish()
{
    flushChunk();
    return output.size();
}

void OutputByteStream::reset()
{
    buffer.clear();
    output.clear();
}

}
