#pragma once

#include <Processors/Formats/Impl/JSONColumnsBlockInputFormatBase.h>

namespace DB
{

/* Format JSONCompactColumns reads each block of data in the next format:
 * [
 *     [value1, value2, value3, ...],
 *     [value1, value2m value3, ...],
 *     ...
 * ]
 */
class JSONCompactColumnsReader : public JSONColumnsReaderBase
{
public:
    JSONCompactColumnsReader(ReadBuffer & in_);

    void readChunkStart() override;
    std::optional<String> readColumnStart() override;
    bool checkChunkEnd() override;
};

}
