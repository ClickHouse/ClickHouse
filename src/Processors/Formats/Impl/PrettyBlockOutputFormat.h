#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Common/PODArray.h>
#include <Common/ThreadPool.h>


namespace DB
{

class WriteBuffer;
class Context;


/** Prints the result in the form of beautiful tables.
  */
class PrettyBlockOutputFormat final : public IOutputFormat
{
public:
    enum class Style
    {
        Full,    /// Table borders are displayed between every row.
        Compact, /// Table borders only for outline, but not between rows.
        Space,   /// Blank spaces instead of table borders.
    };

    /// no_escapes - do not use ANSI escape sequences - to display in the browser, not in the console.
    PrettyBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, Style style_, bool mono_block_, bool color_, bool glue_chunks_);
    ~PrettyBlockOutputFormat() override;

    String getName() const override { return "PrettyBlockOutputFormat"; }

protected:
    void consume(Chunk) override;
    void consumeTotals(Chunk) override;
    void consumeExtremes(Chunk) override;

    size_t total_rows = 0;
    size_t displayed_rows = 0;
    size_t prev_row_number_width = 7;
    size_t row_number_width = 7; // "10000. "

    FormatSettings format_settings;

    using Widths = PODArray<size_t>;
    using WidthsPerColumn = std::vector<Widths>;

    void write(Chunk chunk, PortKind port_kind);
    void writeChunk(const Chunk & chunk, PortKind port_kind);
    void writeMonoChunkIfNeeded();
    void writeSuffix() override;
    void writeSuffixImpl();

    void onRowsReadBeforeUpdate() override;

    /// `value_width_limit` is the effective width limit of a value: it is `output_format_pretty_max_value_width`,
    /// or zero when the values are not cut at all (the single-value exemption), in which case the cells are
    /// only bounded by `output_format_pretty_max_column_pad_width`.
    /// `min_widths`, when not empty, gives a lower bound for the width of every column; it is used to
    /// recalculate the widths after the columns have been widened to fit the names of the Tuple groups,
    /// because the visible width of a value containing a tab depends on the position where the value starts.
    void calculateWidths(
        const Block & header, const Chunk & chunk, bool split_by_lines, size_t value_width_limit,
        const Widths & min_widths, bool & out_has_newlines,
        WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths, Strings & names);

    /// `prefix` is the position on the line where the cell starts: the visible width of a value
    /// containing a tab depends on it, so it must be the same as in `calculateWidths`.
    void writeValueWithPadding(
        const IColumn & column, const ISerialization & serialization, size_t row_num,
        bool split_by_lines, std::optional<String> & serialized_value, size_t & start_from_offset,
        size_t value_width, size_t pad_to_width, size_t cut_to_width, size_t prefix, bool align_right, bool is_number);

    /// The position on the line where the first cell starts.
    size_t firstCellPrefix() const;

    /// Writes one cell-padding character: `U+00A0` when `use_nbsp_for_padding` is on, ASCII space otherwise.
    void writePaddingSpace();
    void writePaddingSpaces(size_t count);

    void resetFormatterImpl() override
    {
        total_rows = 0;
        displayed_rows = 0;
        use_vertical_format = false;
    }

    static bool cutInTheMiddle(size_t row_num, size_t num_rows, size_t max_rows);

    bool readable_number_tip = false;

private:
    Style style;
    bool mono_block;
    bool color;
    bool glue_chunks;

    /// Fallback to Vertical format for wide but short tables.
    std::unique_ptr<IRowOutputFormat> vertical_format_fallback;
    bool use_vertical_format = false;

    /// True iff `format_settings.pretty.use_nbsp_for_padding` AND charset is `UTF-8`.
    bool use_nbsp_for_padding = false;

    /// For mono_block == true only
    Chunk mono_chunk;
    Widths prev_chunk_max_widths;
    bool had_footer = false;

    /// Implements squashing of chunks by time
    std::condition_variable mono_chunk_condvar;
    std::optional<ThreadFromGlobalPool> thread;
    std::atomic_bool finish{false};
    void writingThread();
    void stopThread();
};

}
