#pragma once

#include <unordered_map>
#include <vector>
#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>
#include "Columns/IColumn.h"
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>

using NpySizeT = uint32_t;
static const uint8_t NPY_DOCUMENT_END = 0x00;

namespace DB
{

class ReadBuffer;

class NpyRowInputFormat final : public IRowInputFormat
{
public:
    NpyRowInputFormat(ReadBuffer & in_, Block header_, Params params_);

    String getName() const override { return "NpyRowInputFormat"; }

    void readFromBuffer(MutableColumns &  /*columns*/);

    void resetParser() override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    void readData(MutableColumns & columns);

    void readRows(MutableColumns & columns);

    void readValueAndinsertIntoColumn(IColumn& column);

    std::unordered_map<String, String> header;
    std::vector<int> shape;
    DataTypePtr nestedType;
    int endian;
};

class NpySchemaReader : public ISchemaReader
{
public:
    explicit NpySchemaReader(ReadBuffer & in_);

    std::unordered_map<String, String> getHeader();

private:
    NamesAndTypesList readSchema() override;
    // NamesAndTypesList getDataTypesFromNpyDocument([[maybe_unused]]bool allow_to_skip_unsupported_types);
    // String readHeader(bool & eof);

    bool first_row = true;
};

}
