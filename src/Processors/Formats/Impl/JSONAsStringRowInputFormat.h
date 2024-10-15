#pragma once

#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <IO/PeekableReadBuffer.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/DataTypeObject.h>

namespace DB
{

class ReadBuffer;

/// This format parses a sequence of JSON objects separated by newlines, spaces and/or comma.
class JSONAsRowInputFormat : public JSONEachRowRowInputFormat
{
public:
    JSONAsRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings);

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

protected:
    virtual void readJSONObject(IColumn & column) = 0;
};

/// Each JSON object is parsed as a whole to string.
/// This format can only parse a table with single field of type String.
class JSONAsStringRowInputFormat final : public JSONAsRowInputFormat
{
public:
    JSONAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);
    String getName() const override { return "JSONAsStringRowInputFormat"; }

    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;

private:
    JSONAsStringRowInputFormat(const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, Params params_, const FormatSettings & format_settings_);

    void readJSONObject(IColumn & column) override;

    std::unique_ptr<PeekableReadBuffer> buf;
};


/// Each JSON object is parsed as a whole to object.
/// This format can only parse a table with single field of type Object.
class JSONAsObjectRowInputFormat final : public JSONAsRowInputFormat
{
public:
    JSONAsObjectRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);
    String getName() const override { return "JSONAsObjectRowInputFormat"; }

private:
    Chunk getChunkForCount(size_t rows) override;
    void readJSONObject(IColumn & column) override;
};

class JSONAsStringExternalSchemaReader : public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override
    {
        return {{"json", std::make_shared<DataTypeString>()}};
    }
};

class JSONAsObjectExternalSchemaReader : public IExternalSchemaReader
{
public:
    explicit JSONAsObjectExternalSchemaReader(const FormatSettings & settings_);

    NamesAndTypesList readSchema() override
    {
        if (settings.json.allow_json_type)
            return {{"json", std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON)}};
        return {{"json", std::make_shared<DataTypeObjectDeprecated>("json", false)}};
    }

private:
    FormatSettings settings;
};

}
