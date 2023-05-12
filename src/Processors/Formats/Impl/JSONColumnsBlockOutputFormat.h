#pragma once

#include <Processors/Formats/Impl/JSONColumnsBlockOutputFormatBase.h>

namespace DB
{

/* Format JSONColumns outputs all data as a single block in the next format:
 * {
 *     "name1": [value1, value2, value3, ...],
 *     "name2": [value1, value2m value3, ...],
 *     ...
 * }
 */
class JSONColumnsBlockOutputFormat : public JSONColumnsBlockOutputFormatBase
{
public:
    JSONColumnsBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, size_t indent_ = 0);

    String getName() const override { return "JSONColumnsBlockOutputFormat"; }

protected:
    void writeChunkStart() override;
    void writeChunkEnd() override;

    void writeColumnStart(size_t column_index) override;

    NamesAndTypes fields;
    size_t indent;
};

}
