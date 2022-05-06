#pragma once
#include <Processors/Formats/Impl/JSONColumnsBaseBlockOutputFormat.h>

namespace DB
{

/* Format JSONCompactColumns outputs each block of data in the next format:
 * [
 *     [value1, value2, value3, ...],
 *     [value1, value2m value3, ...],
 *     ...
 * ]
 * There is also JSONCompactColumnsMonoBlock format that buffers up to output_format_json_columns_max_rows_to_buffer rows
 * and outputs them as a single block.
 */
class JSONCompactColumnsBlockOutputFormat : public JSONColumnsBaseBlockOutputFormat
{
public:
    /// no_escapes - do not use ANSI escape sequences - to display in the browser, not in the console.
    JSONCompactColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, bool mono_block_);

    String getName() const override { return "JSONCompactColumnsBlockOutputFormat"; }

protected:
    void writeChunkStart() override;
    void writeChunkEnd() override;

    void writeColumnStart(size_t column_index) override;

    Names column_names;
};

}
