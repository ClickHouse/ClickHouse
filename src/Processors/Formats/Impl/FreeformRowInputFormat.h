#pragma once

#include "Core/NamesAndTypes.h"
#include "DataTypes/IDataType.h"
#include "Processors/Formats/IInputFormat.h"
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
    bool readRow(MutableColumns & column, RowReadExtension &) override;
    const FormatSettings format_settings;
};

class FreeformSchemaReader : public IRowSchemaReader
{
public:
    FreeformSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

private:
    NamesAndTypesList readSchema() override;
    DataTypes readRowAndGetDataTypes() override;
};

}
