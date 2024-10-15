#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

class ReadBuffer;

/// This format parses a sequence of Line objects separated by newlines, spaces and/or comma.
/// Each Line object is parsed as a whole to string.
/// This format can only parse a table with single field of type String.

class LineAsStringRowInputFormat final : public IRowInputFormat
{
public:
    LineAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    String getName() const override { return "LineAsStringRowInputFormat"; }
    void resetParser() override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

    void readLineObject(IColumn & column);

    size_t countRows(size_t max_block_size) override;
    bool supportsCountRows() const override { return true; }
};

class LinaAsStringSchemaReader : public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override
    {
        return {{"line", std::make_shared<DataTypeString>()}};
    }
};

}
