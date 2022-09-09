#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/* Outputs data as a single JSON Object with rows as fields:
 * {
 *     "row_1": {"num": 42, "str": "hello", "arr": [0,1]},
 *     "row_2": {"num": 43, "str": "hello", "arr": [0,1,2]},
 *     "row_3": {"num": 44, "str": "hello", "arr": [0,1,2,3]},
 * }
 */

class JSONObjectEachRowRowOutputFormat : public JSONEachRowRowOutputFormat
{
public:
    JSONObjectEachRowRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_);

    String getName() const override { return "JSONObjectEachRowRowOutputFormat"; }

private:
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;

    void writePrefix() override;
    void writeSuffix() override;

    size_t row_num = 0;
};

}
