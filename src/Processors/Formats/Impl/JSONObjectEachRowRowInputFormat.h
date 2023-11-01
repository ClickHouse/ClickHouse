#pragma once

#include <Core/Block.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

class ReadBuffer;


class JSONObjectEachRowInputFormat final : public JSONEachRowRowInputFormat
{
public:
    JSONObjectEachRowInputFormat(
        ReadBuffer & in_,
        const Block & header_,
        Params params_,
        const FormatSettings & format_settings_);

    String getName() const override { return "JSONObjectEachRowInputFormat"; }

private:
    void readPrefix() override;
    void readSuffix() override {}
    void readRowStart(MutableColumns & columns) override;
    void skipRowStart() override;
    bool checkEndOfData(bool is_first_row) override;

    std::optional<size_t> field_index_for_object_name;
};


class JSONObjectEachRowSchemaReader : public IRowWithNamesSchemaReader
{
public:
    JSONObjectEachRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

private:
    NamesAndTypesList readRowAndGetNamesAndDataTypes(bool & eof) override;
    NamesAndTypesList getStaticNamesAndTypes() override;
    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;
    void transformFinalTypeIfNeeded(DataTypePtr & type) override;

    bool first_row = true;
    JSONInferenceInfo inference_info;
};

std::optional<size_t> getColumnIndexForJSONObjectEachRowObjectName(const Block & header, const FormatSettings & settings);

}
