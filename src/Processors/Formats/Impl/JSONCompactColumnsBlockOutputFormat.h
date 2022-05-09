#pragma once
#include <Processors/Formats/Impl/JSONColumnsBaseBlockOutputFormat.h>

namespace DB
{

/* Format JSONCompactColumns outputs all data as a single block in the next format:
 * [
 *     [value1, value2, value3, ...],
 *     [value1, value2m value3, ...],
 *     ...
 * ]
 */
class JSONCompactColumnsBlockOutputFormat : public JSONColumnsBaseBlockOutputFormat
{
public:
    JSONCompactColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "JSONCompactColumnsBlockOutputFormat"; }

protected:
    void writeChunkStart() override;
    void writeChunkEnd() override;

    void writeColumnStart(size_t column_index) override;

    Names column_names;
};

}
