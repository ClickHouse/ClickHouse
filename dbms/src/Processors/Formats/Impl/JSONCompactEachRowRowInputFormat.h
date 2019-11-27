#pragma once

#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

class ReadBuffer;


class JSONCompactEachRowRowInputFormat : public IRowInputFormat
{
public:
    JSONCompactEachRowRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "JSONCompactEachRowRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;


private:
    void readField(size_t index, MutableColumns & columns);

    const FormatSettings format_settings;
};

}
