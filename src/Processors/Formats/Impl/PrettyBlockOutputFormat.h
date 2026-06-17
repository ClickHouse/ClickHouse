#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Common/PODArray.h>


namespace DB
{

class WriteBuffer;
class Context;


/** Prints the result in the form of beautiful tables.
  */
class PrettyBlockOutputFormat : public IOutputFormat
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

    const FormatSettings format_settings;
    Serializations serializations;

    using Widths = PODArray<size_t>;
    using WidthsPerColumn = std::vector<Widths>;

    void write(Chunk chunk, PortKind port_kind);
    virtual void writeChunk(const Chunk & chunk, PortKind port_kind);
    void writeMonoChunkIfNeeded();
    void writeSuffix() override;
    virtual void writeSuffixImpl();

    void onRowsReadBeforeUpdate() override { total_rows = getRowsReadBefore(); }

    void calculateWidths(
        const Block & header, const Chunk & chunk, bool split_by_lines, bool & out_has_newlines,
        WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths, Strings & names);

    void writeValueWithPadding(
        const IColumn & column, const ISerialization & serialization, size_t row_num,
        bool split_by_lines, std::optional<String> & serialized_value, size_t & start_from_offset,
        size_t value_width, size_t pad_to_width, size_t cut_to_width, bool align_right, bool is_number);

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
