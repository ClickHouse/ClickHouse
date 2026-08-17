#include <Processors/Formats/Impl/ColumnBinaryInputFormat.h>

#include <algorithm>
#include <limits>
#include <vector>

#include <Columns/ColumnConst.h>
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

    // Top-level columns occupy disjoint, contiguous byte regions in descriptor order: the
    // writer advances a single monotone cursor across the whole block (`buildColDescriptor`
    // takes the cursor and returns the next free offset), and every one of its branches
    // leaves `data_offset + data_size` equal to that returned cursor. So column `i`'s region
    // ends exactly where column `i + 1`'s begins, starting right after the header and
    // descriptor table.
    //
    // Derive those regions here and confine each column to its own `[region_start,
    // region_end)` before decoding. `readColumnFromDesc` only bounds the ranges a descriptor
    // references against the size of the buffer it is given, so without this a malformed
    // frame could repoint column `i`'s `null_offset` / `offsets_offset` / `data_offset` into
    // bytes owned by a sibling column and still pass every check — column `0` would then
    // decode from column `1`'s payload instead of being rejected as malformed. Passing a
    // subspan that ends at `region_end` (rather than only checking the start offsets) makes
    // every internal bounds check inside `readColumnFromDesc` bound against this column's
    // region automatically, so a range that starts inside the region but extends past it is
    // rejected too. This mirrors what the nested `COL_VARIANT` / `COL_LOWCARD` descriptor
    // paths in `readColumnFromDesc` already do for their sub-columns.
    uint64_t region_start = hdr_desc_size;
    for (uint32_t i = 0; i < num_cols; ++i)
    {
        ColumnarV1::ColDescriptor desc{};
        std::memcpy(&desc,
                    buf.data() + ColumnarV1::COLUMNAR_HEADER_BYTES + i * ColumnarV1::COLUMNAR_DESC_BYTES,
                    sizeof(desc));

        // data_offset + data_size cannot overflow here: the loop above already rejected that.
        const uint64_t region_end = desc.data_offset + desc.data_size;
        if (desc.data_offset < region_start)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: column {} data range [{}, {}) starts before its own region (expected start {})",
                i, desc.data_offset, region_end, region_start);
        if (region_end > data_end)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: column {} data range [{}, {}) extends past the frame end {}",
                i, desc.data_offset, region_end, data_end);

        // 0 is the "absent" sentinel for both (no null map / no offsets array); a nonzero
        // value must land inside this column's own region. `region_end` is inclusive here
        // because a zero-length array legitimately starts at the region end.
        for (uint64_t off : {desc.null_offset, desc.offsets_offset})
            if (off != 0 && (off < region_start || off > region_end))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ColumnBinary: column {} descriptor offset {} outside its data region [{}, {})",
                    i, off, region_start, region_end);

        const auto & expected_type = header_->getByPosition(i).type;
        auto column = ColumnarV1::readColumnFromDesc(buf.subspan(0, region_end), desc, num_rows, expected_type);

        // Defence in depth for the whole tag dispatch, not just the families that check the
        // declared type themselves: the chunk goes on to be inserted into the destination
        // columns with `insertRangeFrom`, whose `assert_cast` is a plain `static_cast` in
        // release builds, so a decoded column of the wrong concrete class would be host-side
        // undefined behaviour rather than a rejected frame.
        // COL_IS_CONST legitimately decodes to a ColumnConst wrapper, which never structurally
        // equals the plain column the declared type creates; compare what it wraps.
        const IColumn & decoded = column->isConst()
            ? assert_cast<const ColumnConst &>(*column).getDataColumn()
            : *column;
        if (!decoded.structureEquals(*expected_type->createColumn()))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ColumnBinary: column {} decoded as {}, which does not match the declared type {}",
                i, decoded.getName(), expected_type->getName());

        result.push_back(std::move(column));

        region_start = region_end;
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
        ColumnarV1::checkColumnBinaryFormatIsAllowed(settings.column_binary.allow_experimental);
        return std::make_shared<ColumnBinaryInputFormat>(buf, header, params, settings);
    });

    factory.setDocumentation("ColumnBinary", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

:::note Experimental
`ColumnBinary` is experimental and disabled by default; set `allow_experimental_column_binary_format = 1` to use it.
Its `COLUMNAR_V1` frame header carries no wire version, so the layout may change incompatibly and data written today
may not be readable by a future version. Do not persist `ColumnBinary` data until the layout is frozen and the frame
header is versioned.
:::

`ColumnBinary` is a compact columnar binary format that reuses the `COLUMNAR_V1` wire layout also used by the [`COLUMNAR_V1` WASM UDF ABI](/sql-reference/functions/wasm_udf#abi-columnar-v1). Unlike [Native](./Native.md) and [Buffers](./Buffers.md), which serialize each column independently one after another, `ColumnBinary` writes a single frame per block: a header, a fixed-size descriptor table (one descriptor per column), and then every column's data packed contiguously. All numeric fields are little-endian.

A frame has the following layout:

```txt
[ColumnarV1Header: 8 bytes]
[ColDescriptor × num_columns: 40 bytes each]
[Column data blocks]
```

The 8-byte header contains two `uint32` fields: `num_rows` then `num_cols`.

Each `ColDescriptor` is exactly 40 bytes and contains five `uint64` fields (all byte offsets are absolute from the start of the frame):

- `type` (8 bytes) — column type identifier (see below)
- `null_offset` (8 bytes) — absolute offset to the `u8[num_rows]` null map (0 if not nullable; 1 = null, 0 = non-null)
- `offsets_offset` (8 bytes) — absolute offset to the `uint64[num_rows+1]` offsets array (for `COL_BYTES` columns; 0 otherwise)
- `data_offset` (8 bytes) — absolute offset to the column data block
- `data_size` (8 bytes) — size of the data block in bytes

Base column types:

- `0` — `COL_BYTES` — variable-length byte strings (`String`); paired with `offsets_offset` array and a data block of raw bytes (no null terminators)
- `1` — `COL_FIXED8` — 1-byte fixed-width scalars (`Int8`, `UInt8`)
- `2` — `COL_FIXED16` — 2-byte fixed-width scalars (`Int16`, `UInt16`)
- `3` — `COL_FIXED32` — 4-byte fixed-width scalars (`Int32`, `UInt32`, `Float32`)
- `4` — `COL_FIXED64` — 8-byte fixed-width scalars (`Int64`, `UInt64`, `Float64`, `DateTime64`)
- `5` — `COL_COMPLEX` — recursive format for `Array(T)`, `Tuple(T…)`, and `Map(K, V)`
- `6` — `COL_VARIANT` — discriminated union (`Variant(…)`)
- `7` — `COL_FIXEDN` — fixed-width scalars of any other width (`UUID`, `IPv6`, `Int128`/`UInt128`, `Int256`/`UInt256`, `Decimal128`/`Decimal256`, …)
- `8` — `COL_LOWCARD` — top-level `LowCardinality(T)`; dictionary sub-column plus a compact index array (nested `LowCardinality` materializes to `T` instead)

Modifier flags (OR'd onto the base type):

- `COL_IS_NULLABLE` (`0x20`) — nullable column; `null_offset` carries a `u8[num_rows]` null map
- `COL_IS_CONST` (`0x80`) — constant column; only 1 row of data is stored; the reader replicates it to the full row count

Because the entire frame is laid out contiguously, a well-formed `data_offset`/`data_size` pair for `COL_BYTES` always starts at byte `0` of the data block and covers it exactly, with no gaps.

## Example usage {#example-usage}

Write to a file:

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.columnbinary'
FORMAT ColumnBinary;
```

Read back with explicit column types:

```sql
SELECT
    *
FROM file(
    'squares.columnbinary',
    'ColumnBinary',
    'col_1 UInt64, col_2 UInt64'
);
```
)DOCS_MD"});
}

}
