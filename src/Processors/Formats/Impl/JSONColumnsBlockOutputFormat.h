#pragma once

#include <Processors/Formats/Impl/JSONColumnsBaseBlockOutputFormat.h>

namespace DB
{

/* Format JSONColumns outputs each block of data in the next format:
 * {
 *     "name1": [value1, value2, value3, ...],
 *     "name2": [value1, value2m value3, ...],
 *     ...
 * }
 * There is also JSONColumnsMonoBlock format that buffers up to output_format_json_columns_max_rows_to_buffer rows
 * and outputs them as a single block.
 */
class JSONColumnsBlockOutputFormat : public JSONColumnsBaseBlockOutputFormat
{
public:
    JSONColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, bool mono_block_, size_t indent_ = 0);

    String getName() const override { return "JSONColumnsBlockOutputFormat"; }

protected:
    void writeChunkStart() override;
    void writeChunkEnd() override;

    void writeColumnStart(size_t column_index) override;

    NamesAndTypes fields;
    size_t indent;
};

}
