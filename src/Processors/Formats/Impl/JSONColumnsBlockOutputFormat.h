#pragma once

#include <Core/Block.h>
#include <Processors/Formats/Impl/JSONColumnsBlockOutputFormatBase.h>

namespace DB
{

/* Format JSONColumns outputs all data as a single block in the next format:
 * {
 *     "name1": [value1, value2, value3, ...],
 *     "name2": [value1, value2, value3, ...],
 *     ...
 * }
 */
class JSONColumnsBlockOutputFormat : public JSONColumnsBlockOutputFormatBase
{
public:
    JSONColumnsBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, bool validate_utf8, size_t indent_ = 0);

    String getName() const override { return "JSONColumnsBlockOutputFormat"; }

protected:
    void writeChunkStart() override;
    void writeChunkEnd() override;

    void writeColumnStart(size_t column_index) override;

    Names names;
    size_t indent;

    SharedHeader header;
};

}
