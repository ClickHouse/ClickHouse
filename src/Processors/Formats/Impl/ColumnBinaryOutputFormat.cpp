#include <Processors/Formats/Impl/ColumnBinaryOutputFormat.h>

#include <Core/Block.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>

#include <Common/typeid_cast.h>
#include <Formats/ColumnarV1Wire.h>

namespace DB
{

// TODO(ColumnBinary settings): add a FormatSettings knob for diagnostics/benchmarking:
//   column_binary_disable_preallocation  — return std::nullopt here to fall through to
//     CH's normal heap-allocation path (eliminates the conservative-size scan entirely;
//     useful to measure the overhead of the two-phase layout vs. a plain WriteBuffer).

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
        // TODO(LowCardinality wire format): fully materialized to the dictionary's full
        // column here rather than encoded as dictionary+index on the wire — see the TODO
        // on validateColumnarV1SupportedType's LowCardinality branch. Must happen before
        // is_nullable below: a LowCardinality(Nullable(T)) dictionary materializes to a
        // real ColumnNullable, which is_nullable needs to see to reserve a null map.
        ColumnPtr lowcard_materialized;
        if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(actual))
        {
            lowcard_materialized = lc_col->convertToFullColumn();
            actual = lowcard_materialized.get();
        }
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
    uint32_t num_cols = static_cast<uint32_t>(std::min<size_t>(chunk.getNumColumns(), header_->columns()));

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
        ColumnPtr lowcard_materialized;
        if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(actual))
        {
            lowcard_materialized = lc_col->convertToFullColumn();
            actual = lowcard_materialized.get();
        }
        bool is_nullable = typeid_cast<const ColumnNullable *>(actual) != nullptr;
        uint32_t col_rows = is_const ? 1u : num_rows;
        cursor = ColumnarV1::buildColDescriptor(actual, is_const, is_nullable, col_rows, cursor, descs[i]);
    }

    // Get write destination: use the pre-allocated region in out when available,
    // otherwise fall back to a temporary buffer (e.g. when the caller did not
    // pre-allocate via precomputeSerializedSize, such as in tests or the legacy
    // IOutputFormat::write() compatibility path).
    std::vector<uint8_t> tmp_buf;
    uint8_t * buf = nullptr;
    bool use_prealloc = !disable_preallocation_ && out.available() >= cursor;
    if (!use_prealloc)
    {
        tmp_buf.resize(cursor);
        buf = tmp_buf.data();
    }
    else
    {
        // Unlike tmp_buf (std::vector::resize value-initializes to 0), the
        // WriteBuffer's internal buffer is not zeroed. Alignment padding gaps
        // between COL_COMPLEX/COL_VARIANT sub-blocks are intentionally never
        // written by writeColData, so they must be zeroed here to avoid
        // leaking uninitialized memory into the output stream.
        buf = reinterpret_cast<uint8_t *>(out.position());
        std::memset(buf, 0, cursor);
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
        ColumnPtr lowcard_materialized;
        if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(actual))
        {
            lowcard_materialized = lc_col->convertToFullColumn();
            actual = lowcard_materialized.get();
        }
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
    // Reject unsupported signatures (nested Nullable/Variant, Map, >8-byte fixed-width
    // types) here so callers find out at format construction, not on the first block.
    for (const auto & col : header_->getColumnsWithTypeAndName())
        ColumnarV1::validateColumnarV1SupportedType(col.type);
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
