#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

class ReadBuffer;

/// This format slurps all input data into single value.
/// This format can only parse a table with single field of type String or similar.

class RawBLOBRowInputFormat final : public IRowInputFormat
{
public:
    RawBLOBRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    String getName() const override { return "RawBLOBRowInputFormat"; }

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
};

class RawBLOBSchemaReader: public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override
    {
        return {{"raw_blob", std::make_shared<DataTypeString>()}};
    }
};

}
