#pragma once

#include <Processors/Formats/Impl/JSONColumnsBlockInputFormatBase.h>

namespace DB
{

/* Format JSONColumns reads each block of data in the next format:
 * {
 *     "name1": [value1, value2, value3, ...],
 *     "name2": [value1, value2m value3, ...],
 *     ...
 * }
 */
class JSONColumnsReader : public JSONColumnsReaderBase
{
public:
    explicit JSONColumnsReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    void readChunkStart() override;
    std::optional<String> readColumnStart() override;
    bool checkChunkEnd() override;

protected:
    const FormatSettings format_settings;
};

}
