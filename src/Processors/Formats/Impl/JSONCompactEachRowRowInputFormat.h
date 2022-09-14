#pragma once

#include <Core/Block.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

class ReadBuffer;


/** A stream for reading data in a bunch of formats:
 *  - JSONCompactEachRow
 *  - JSONCompactEachRowWithNamesAndTypes
 *  - JSONCompactStringsEachRow
 *  - JSONCompactStringsEachRowWithNamesAndTypes
 *
*/
class JSONCompactEachRowRowInputFormat final : public RowInputFormatWithNamesAndTypes
{
public:
    JSONCompactEachRowRowInputFormat(
        const Block & header_,
        ReadBuffer & in_,
        Params params_,
        bool with_names_,
        bool with_types_,
        bool yield_strings_,
        const FormatSettings & format_settings_);

    String getName() const override { return "JSONCompactEachRowRowInputFormat"; }

private:
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;
};

class JSONCompactEachRowFormatReader final : public FormatWithNamesAndTypesReader
{
public:
    JSONCompactEachRowFormatReader(ReadBuffer & in_, bool yield_strings_, const FormatSettings & format_settings_);


    bool parseRowStartWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override
    {
        return *pos != ',' && *pos != ']' && *pos != ' ' && *pos != '\t';
    }

    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

    void skipField(size_t /*column_index*/) override { skipField(); }
    void skipField();
    void skipHeaderRow();
    void skipNames() override { skipHeaderRow(); }
    void skipTypes() override { skipHeaderRow(); }
    void skipRowStartDelimiter() override;
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;

    std::vector<String> readHeaderRow();
    std::vector<String> readNames() override { return readHeaderRow(); }
    std::vector<String> readTypes() override { return readHeaderRow(); }

    bool yieldStrings() const { return yield_strings; }
private:
    bool yield_strings;
};

class JSONCompactEachRowRowSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    JSONCompactEachRowRowSchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, bool yield_strings_, const FormatSettings & format_settings_);

private:
    DataTypes readRowAndGetDataTypes() override;

    JSONCompactEachRowFormatReader reader;
    bool first_row = true;
};

}
