#pragma once

#include "Processors/Formats/IRowInputFormat.h"
#include "Processors/Formats/ISchemaReader.h"

namespace DB
{

class FreeformRowInputFormat final : public IRowInputFormat
{
public:
    FreeformRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "FreeformRowInputFormat"; }

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    const FormatSettings format_settings;
};

class FreeformSchemaReader : public IRowSchemaReader
{
public:
    FreeformSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
    // readSchema initiates the simultaneous iterations on multiple lines and pick the best solution
    NamesAndTypesList readSchema() override;

private:
    std::vector<DataTypes> readRowAndGenerateSolutions();
    std::vector<std::pair<DataTypePtr, char *>> readNextPossibleFields();
    void recursivelyGetNextFieldInRow(char * current_pos, DataTypes current_result, std::vector<DataTypes> & solutions);
};

}
