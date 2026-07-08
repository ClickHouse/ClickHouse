#include <Processors/Formats/Impl/ColumnBinaryInputFormat.h>

#include <algorithm>
#include <limits>
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
    const FormatSettings & settings)
    : IInputFormat(std::make_shared<const Block>(header), &buf)
    , header_(std::make_shared<const Block>(header))
    , format_settings_(settings)
{
    // Reject unsupported signatures (nested Nullable/Variant, Map, >8-byte fixed-width
    // types) here so callers find out at format construction, not on the first block.
    for (const auto & col : header_->getColumnsWithTypeAndName())
        ColumnarV1::validateColumnarV1SupportedType(col.type);
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

    // num_cols comes straight from the (network-facing) frame and is otherwise
    // untrusted; reject it before sizing any buffer off of it. ColumnBinary is
    // schema-driven, so anything other than an exact match is invalid: fewer
    // columns silently drops trailing schema columns from the Chunk (turning a
    // malformed frame into a structural mismatch further downstream instead of
    // a clear parse error here), and more columns is the same untrusted-size
    // problem this check exists to prevent in the first place.
    if (num_cols != header_->columns())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: frame declares {} columns, expected {}",
            num_cols, header_->columns());

    // Read header + descriptor table into a single buffer.
    const size_t desc_total = static_cast<size_t>(num_cols) * ColumnarV1::COLUMNAR_DESC_BYTES;
    const size_t hdr_desc_size = ColumnarV1::COLUMNAR_HEADER_BYTES + desc_total;

    std::vector<uint8_t> frame(hdr_desc_size);
    std::memcpy(frame.data(), hdr_buf, ColumnarV1::COLUMNAR_HEADER_BYTES);

    if (desc_total > 0)
        in->readStrict(reinterpret_cast<char *>(frame.data() + ColumnarV1::COLUMNAR_HEADER_BYTES), desc_total);

    // Compute the furthest byte referenced by any descriptor to get the total frame size.
    // Descriptors use absolute byte offsets from the start of the frame buffer and are
    // otherwise untrusted (network-facing): a hostile frame could set data_offset/data_size
    // to overflow the addition, or to a huge-but-non-overflowing value (e.g. 1 << 40) to
    // make the frame.resize() below try to reserve an absurd amount of host memory before
    // any of the actual column data has even been validated. Reject both.
    uint64_t data_end = static_cast<uint64_t>(hdr_desc_size);
    for (uint32_t i = 0; i < num_cols; ++i)
    {
        ColumnarV1::ColDescriptor desc{};
        std::memcpy(&desc,
                    frame.data() + ColumnarV1::COLUMNAR_HEADER_BYTES + i * ColumnarV1::COLUMNAR_DESC_BYTES,
                    sizeof(desc));

        // null_offset/offsets_offset use 0 as the sentinel for "absent" (no null map / no
        // offsets array); a nonzero value must still point at or past the end of the
        // header + descriptor table, since no genuine null map/offsets array starts inside
        // the metadata region. These two are never included in the data_end computation
        // below, so a small nonzero value here would otherwise pass every later bounds
        // check (which only compares against the frame's total size, not the
        // metadata/data boundary) and read metadata as if it were a null map or offsets
        // array.
        for (uint64_t off : {desc.null_offset, desc.offsets_offset})
            if (off != 0 && off < static_cast<uint64_t>(hdr_desc_size))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: descriptor offset {} points inside the header/descriptor table (< {})",
                    off, hdr_desc_size);

        // data_offset has no "absent" sentinel — the writer's cursor always starts at
        // hdr_desc_size and only grows, so data_offset is never 0 (or otherwise less than
        // hdr_desc_size) in a genuine frame, even for an empty column. Without this check,
        // a descriptor like {data_offset=0, data_size=1} leaves data_end unchanged
        // (0+1 < hdr_desc_size), so no data section is ever read from the wire, and the
        // decoder then silently interprets header/descriptor bytes as column payload
        // instead of throwing.
        if (desc.data_offset < static_cast<uint64_t>(hdr_desc_size))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: descriptor data_offset {} points inside the header/descriptor table (< {})",
                desc.data_offset, hdr_desc_size);

        if (desc.data_offset > std::numeric_limits<uint64_t>::max() - desc.data_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: descriptor data_offset + data_size overflows: offset={}, size={}",
                desc.data_offset, desc.data_size);
        data_end = std::max(data_end, desc.data_offset + desc.data_size);
    }

    if (data_end < static_cast<uint64_t>(hdr_desc_size))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: descriptor references data before descriptor table end");

    // 0 is the pre-existing-setting compatibility fallback (this setting did not exist before
    // 26.7) and means "no cap", matching the pre-setting unlimited behavior — not a literal
    // zero-byte limit, which would reject every non-empty frame.
    if (format_settings_.column_binary.max_frame_size != 0
        && data_end - static_cast<uint64_t>(hdr_desc_size) > format_settings_.column_binary.max_frame_size)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: frame data size {} exceeds column_binary_max_frame_size limit {}",
            data_end - hdr_desc_size, format_settings_.column_binary.max_frame_size);

    // Read the column data section exactly.
    if (data_end > static_cast<uint64_t>(hdr_desc_size))
    {
        const size_t data_bytes = static_cast<size_t>(data_end - hdr_desc_size);
        frame.resize(data_end);
        in->readStrict(reinterpret_cast<char *>(frame.data() + hdr_desc_size), data_bytes);
    }

    // Decode columns from the complete in-memory frame.
    const std::span<const uint8_t> buf{frame};
    MutableColumns result;
    result.reserve(num_cols);
    for (uint32_t i = 0; i < num_cols; ++i)
    {
        ColumnarV1::ColDescriptor desc{};
        std::memcpy(&desc,
                    buf.data() + ColumnarV1::COLUMNAR_HEADER_BYTES + i * ColumnarV1::COLUMNAR_DESC_BYTES,
                    sizeof(desc));
        result.push_back(ColumnarV1::readColumnFromDesc(buf, desc, num_rows, header_->getByPosition(i).type));
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
