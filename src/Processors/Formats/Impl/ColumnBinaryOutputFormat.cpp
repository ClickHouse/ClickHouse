#include "ColumnBinaryOutputFormat.h"

#include <Core/Block.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <Common/typeid_cast.h>
#include <Formats/ColumnarV1Wire.h>

namespace DB
{

// TODO(ColumnBinary settings): add two FormatSettings knobs for diagnostics/benchmarking:
//   column_binary_disable_preallocation  — return std::nullopt here to fall through to
//     CH's normal heap-allocation path (eliminates the conservative-size scan entirely;
//     useful to measure the overhead of the two-phase layout vs. a plain WriteBuffer).
//   column_binary_disable_repeat_detection — pass detect_repeats=false to consume()'s
//     buildColDescriptor() calls as well (skips detectPeriod() in both phases; lets you
//     benchmark the COL_IS_REPEAT win in isolation without changing wire format).

std::optional<uint64_t> ColumnBinaryOutputFormat::precomputeSerializedSize(const Block & block, size_t rows) const
{
    if (disable_preallocation_)
        return std::nullopt;

    if (rows == 0 || block.columns() == 0)
        return std::nullopt;

    uint64_t cursor = ColumnarV1::COLUMNAR_HEADER_BYTES + block.columns() * ColumnarV1::COLUMNAR_DESC_BYTES;

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const IColumn & raw_col = *block.getByPosition(i).column;
        bool is_const = isColumnConst(raw_col);
        const IColumn * actual = is_const
            ? &static_cast<const ColumnConst &>(raw_col).getDataColumn()
            : &raw_col;
        bool is_nullable = typeid_cast<const ColumnNullable *>(actual) != nullptr;
        uint32_t col_rows = is_const ? 1u : static_cast<uint32_t>(rows);

        ColumnarV1::ColDescriptor desc{};
        cursor = ColumnarV1::buildColDescriptor(actual, is_const, is_nullable, col_rows, cursor, desc);
    }

    return cursor;
}

void ColumnBinaryOutputFormat::consume(Chunk chunk)
{
    if (!chunk)
        return;

    uint32_t num_rows = static_cast<uint32_t>(chunk.getNumRows());
    uint32_t num_cols = static_cast<uint32_t>(std::min(chunk.getNumColumns(), header_->columns()));

    // Layout pass: build descriptors (compute offsets and total size).
    uint64_t cursor = ColumnarV1::COLUMNAR_HEADER_BYTES + num_cols * ColumnarV1::COLUMNAR_DESC_BYTES;

    std::vector<ColumnarV1::ColDescriptor> descs(num_cols);
    for (uint32_t i = 0; i < num_cols; ++i)
    {
        const IColumn & raw_col = *chunk.getColumns()[i];
        bool is_const = isColumnConst(raw_col);
        const IColumn * actual = is_const
            ? &static_cast<const ColumnConst &>(raw_col).getDataColumn()
            : &raw_col;
        bool is_nullable = typeid_cast<const ColumnNullable *>(actual) != nullptr;
        uint32_t col_rows = is_const ? 1u : num_rows;
        cursor = ColumnarV1::buildColDescriptor(actual, is_const, is_nullable, col_rows, cursor, descs[i]);
    }

    // Get write destination: use the pre-allocated region in out when available,
    // otherwise fall back to a temporary buffer (e.g. when the caller did not
    // pre-allocate via precomputeSerializedSize, such as in tests or the legacy
    // IOutputFormat::write() compatibility path).
    std::vector<uint8_t> tmp_buf;
    uint8_t * buf;
    bool use_prealloc = !disable_preallocation_ && out.available() >= cursor;
    if (!use_prealloc)
    {
        tmp_buf.resize(cursor);
        buf = tmp_buf.data();
    }
    else
    {
        buf = reinterpret_cast<uint8_t *>(out.position());
    }

    // Write header and descriptor table.
    std::memcpy(buf,     &num_rows, 4);
    std::memcpy(buf + 4, &num_cols, 4);
    std::memcpy(buf + ColumnarV1::COLUMNAR_HEADER_BYTES,
                descs.data(),
                num_cols * ColumnarV1::COLUMNAR_DESC_BYTES);

    // Write column data.
    std::span<uint8_t> buf_span{buf, cursor};
    for (uint32_t i = 0; i < num_cols; ++i)
    {
        const IColumn & raw_col = *chunk.getColumns()[i];
        bool is_const = isColumnConst(raw_col);
        const IColumn * actual = is_const
            ? &static_cast<const ColumnConst &>(raw_col).getDataColumn()
            : &raw_col;
        bool is_nullable = typeid_cast<const ColumnNullable *>(actual) != nullptr;
        uint32_t col_rows = is_const ? 1u : num_rows;
        ColumnarV1::writeColData(actual, is_nullable, col_rows, descs[i], buf_span);
    }

    if (!use_prealloc)
        out.write(reinterpret_cast<char *>(buf), cursor);
    else
        out.position() += cursor;
}

ColumnBinaryOutputFormat::ColumnBinaryOutputFormat(WriteBuffer & out_, SharedHeader header,
                                                   bool disable_preallocation)
    : IOutputFormat(header, out_)
    , header_(header)
    , disable_preallocation_(disable_preallocation)
{
}

void registerOutputFormatColumnBinary(FormatFactory & factory)
{
    factory.registerOutputFormat("ColumnBinary", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<ColumnBinaryOutputFormat>(
            buf,
            std::make_shared<const Block>(sample),
            format_settings.column_binary.disable_preallocation);
    });
    factory.markOutputFormatSupportsParallelFormatting("ColumnBinary");
}

}
