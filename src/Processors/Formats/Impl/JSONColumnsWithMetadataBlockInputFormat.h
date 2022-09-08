#pragma once

#include <Processors/Formats/Impl/JSONColumnsBlockInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

/* Format JSONCompactColumns reads each block of data in the next format:
 * [
 *     [value1, value2, value3, ...],
 *     [value1, value2m value3, ...],
 *     ...
 * ]
 */
class JSONColumnsWithMetadataReader : public JSONColumnsReader
{
public:
    JSONColumnsWithMetadataReader(ReadBuffer & in_, const Block & header_, const FormatSettings & settings);

    void readChunkStart() override;
    bool checkChunkEnd() override;

private:
    const Block & header;
    bool use_metadata;
};

class JSONColumnsWithMetadataSchemaReader : public ISchemaReader
{
public:
    JSONColumnsWithMetadataSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;
};


}
