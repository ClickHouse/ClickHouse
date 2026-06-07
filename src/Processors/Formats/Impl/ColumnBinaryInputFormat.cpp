#include <Processors/Formats/Impl/ColumnBinaryInputFormat.h>

#include <algorithm>
#include <vector>

#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Formats/ColumnarV1Wire.h>
#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

ColumnBinaryInputFormat::ColumnBinaryInputFormat(
    ReadBuffer & buf,
    const Block & header,
    const RowInputFormatParams & /*params*/,
    const FormatSettings & /*settings*/)
    : IInputFormat(std::make_shared<const Block>(header), &buf)
    , header_(std::make_shared<const Block>(header))
{
}

Chunk ColumnBinaryInputFormat::read()
{
    if (eof_)
        return {};

    // Try to read the 8-byte header; empty read means clean EOF.
    char hdr_buf[ColumnarV1::COLUMNAR_HEADER_BYTES];
    size_t hdr_read = in->read(hdr_buf, ColumnarV1::COLUMNAR_HEADER_BYTES);
    if (hdr_read == 0)
    {
        eof_ = true;
        return {};
    }
    if (hdr_read < ColumnarV1::COLUMNAR_HEADER_BYTES)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: truncated frame header ({} of {} bytes)", hdr_read, ColumnarV1::COLUMNAR_HEADER_BYTES);

    uint32_t num_rows = 0;
    uint32_t num_cols = 0;
    std::memcpy(&num_rows, hdr_buf, 4);
    std::memcpy(&num_cols, hdr_buf + 4, 4);

    // Read header + descriptor table into a single buffer.
    const size_t desc_total = static_cast<size_t>(num_cols) * ColumnarV1::COLUMNAR_DESC_BYTES;
    const size_t hdr_desc_size = ColumnarV1::COLUMNAR_HEADER_BYTES + desc_total;

    std::vector<uint8_t> frame(hdr_desc_size);
    std::memcpy(frame.data(), hdr_buf, ColumnarV1::COLUMNAR_HEADER_BYTES);

    if (desc_total > 0)
        in->readStrict(reinterpret_cast<char *>(frame.data() + ColumnarV1::COLUMNAR_HEADER_BYTES), desc_total);

    // Compute the furthest byte referenced by any descriptor to get the total frame size.
    // Descriptors use absolute byte offsets from the start of the frame buffer.
    uint64_t data_end = static_cast<uint64_t>(hdr_desc_size);
    for (uint32_t i = 0; i < num_cols; ++i)
    {
        ColumnarV1::ColDescriptor desc{};
        std::memcpy(&desc,
                    frame.data() + ColumnarV1::COLUMNAR_HEADER_BYTES + i * ColumnarV1::COLUMNAR_DESC_BYTES,
                    sizeof(desc));
        data_end = std::max(data_end, desc.data_offset + desc.data_size);
    }

    if (data_end < static_cast<uint64_t>(hdr_desc_size))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: descriptor references data before descriptor table end");

    // Read the column data section exactly.
    if (data_end > static_cast<uint64_t>(hdr_desc_size))
    {
        const size_t data_bytes = static_cast<size_t>(data_end - hdr_desc_size);
        frame.resize(data_end);
        in->readStrict(reinterpret_cast<char *>(frame.data() + hdr_desc_size), data_bytes);
    }

    // Decode columns from the complete in-memory frame.
    const std::span<const uint8_t> buf{frame};
    const uint32_t cols_to_read = std::min(num_cols, static_cast<uint32_t>(header_->columns()));
    MutableColumns result;
    result.reserve(cols_to_read);
    for (uint32_t i = 0; i < cols_to_read; ++i)
    {
        ColumnarV1::ColDescriptor desc{};
        std::memcpy(&desc,
                    buf.data() + ColumnarV1::COLUMNAR_HEADER_BYTES + i * ColumnarV1::COLUMNAR_DESC_BYTES,
                    sizeof(desc));
        result.push_back(ColumnarV1::readColumnFromDesc(buf, desc, num_rows,
                                                        header_->getByPosition(i).type));
    }

    if (in->eof())
        eof_ = true;

    return Chunk(std::move(result), num_rows);
}

void registerInputFormatColumnBinary(FormatFactory & factory)
{
    factory.registerInputFormat("ColumnBinary", [](
        ReadBuffer & buf,
        const Block & header,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<ColumnBinaryInputFormat>(buf, header, params, settings);
    });
}

}
