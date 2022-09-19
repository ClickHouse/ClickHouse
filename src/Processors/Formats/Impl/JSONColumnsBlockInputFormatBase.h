#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>


namespace DB
{

class ReadBuffer;


/// Base class for reading data in Columnar JSON formats.
class JSONColumnsReaderBase
{
public:
    JSONColumnsReaderBase(ReadBuffer & in_);

    virtual ~JSONColumnsReaderBase() = default;

    void setReadBuffer(ReadBuffer & in_) { in = &in_; }

    virtual void readChunkStart() = 0;
    virtual std::optional<String> readColumnStart() = 0;

    virtual bool checkChunkEnd() = 0;
    bool checkChunkEndOrSkipColumnDelimiter();

    bool checkColumnEnd();
    bool checkColumnEndOrSkipFieldDelimiter();

    void skipColumn();

protected:
    ReadBuffer * in;
};


/// Base class for Columnar JSON input formats. It works with data using
/// JSONColumnsReaderBase interface.
/// To implement new Columnar JSON format you need to implement new JSONColumnsReaderBase
/// interface and provide it to JSONColumnsBlockInputFormatBase.
class JSONColumnsBlockInputFormatBase : public IInputFormat
{
public:
    JSONColumnsBlockInputFormatBase(ReadBuffer & in_, const Block & header_, const FormatSettings & format_settings_, std::unique_ptr<JSONColumnsReaderBase> reader_);

    String getName() const override { return "JSONColumnsBlockInputFormatBase"; }

    void setReadBuffer(ReadBuffer & in_) override;

    const BlockMissingValues & getMissingValues() const override { return block_missing_values; }

protected:
    Chunk generate() override;

    size_t readColumn(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, const String & column_name);

    const FormatSettings format_settings;
    const NamesAndTypes fields;
    /// Maps column names and their positions in header.
    std::unordered_map<String, size_t> name_to_index;
    Serializations serializations;
    std::unique_ptr<JSONColumnsReaderBase> reader;
    BlockMissingValues block_missing_values;
};


/// Base class for schema inference from Columnar JSON input formats. It works with data using
/// JSONColumnsReaderBase interface.
/// To implement schema reader for the new Columnar JSON format you need to implement new JSONColumnsReaderBase
/// interface and provide it to JSONColumnsSchemaReaderBase.
class JSONColumnsSchemaReaderBase : public ISchemaReader
{
public:
    JSONColumnsSchemaReaderBase(ReadBuffer & in_, const FormatSettings & format_settings_, std::unique_ptr<JSONColumnsReaderBase> reader_);

private:
    NamesAndTypesList readSchema() override;

    /// Read whole column in the block (up to max_rows_to_read rows) and extract the data type.
    DataTypePtr readColumnAndGetDataType(const String & column_name, size_t & rows_read, size_t max_rows_to_read);

    /// Choose result type for column from two inferred types from different rows.
    void chooseResulType(DataTypePtr & type, const DataTypePtr & new_type, const String & column_name, size_t row) const;

    const FormatSettings format_settings;
    std::unique_ptr<JSONColumnsReaderBase> reader;
};

}
