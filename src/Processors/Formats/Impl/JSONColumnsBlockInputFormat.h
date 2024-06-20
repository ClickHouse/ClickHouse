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
    JSONColumnsReader(ReadBuffer & in_);

    void readChunkStart() override;
    std::optional<String> readColumnStart() override;
    bool checkChunkEnd() override;
};

}
