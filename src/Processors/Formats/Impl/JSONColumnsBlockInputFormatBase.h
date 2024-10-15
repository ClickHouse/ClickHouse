#pragma once

#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>


namespace DB
{

class ReadBuffer;


/// Base class for reading data in Columnar JSON formats.
class JSONColumnsReaderBase
{
public:
    explicit JSONColumnsReaderBase(ReadBuffer & in_);

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

    const BlockMissingValues * getMissingValues() const override { return &block_missing_values; }

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

protected:
    Chunk read() override;

    size_t readColumn(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, const String & column_name);

    const FormatSettings format_settings;
    const NamesAndTypes fields;
    /// Maps column names and their positions in header.
    Block::NameMap name_to_index;
    Serializations serializations;
    std::unique_ptr<JSONColumnsReaderBase> reader;
    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;
};


/// Base class for schema inference from Columnar JSON input formats. It works with data using
/// JSONColumnsReaderBase interface.
/// To implement schema reader for the new Columnar JSON format you need to implement new JSONColumnsReaderBase
/// interface and provide it to JSONColumnsSchemaReaderBase.
class JSONColumnsSchemaReaderBase : public ISchemaReader
{
public:
    JSONColumnsSchemaReaderBase(ReadBuffer & in_, const FormatSettings & format_settings_, std::unique_ptr<JSONColumnsReaderBase> reader_);

    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;
    void transformTypesFromDifferentFilesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;

    bool needContext() const override { return !hints_str.empty(); }
    void setContext(const ContextPtr & ctx) override;

    void setMaxRowsAndBytesToRead(size_t max_rows, size_t max_bytes) override
    {
        max_rows_to_read = max_rows;
        max_bytes_to_read = max_bytes;
    }

    size_t getNumRowsRead() const override { return total_rows_read; }

private:
    NamesAndTypesList readSchema() override;

    /// Read whole column in the block (up to max_rows rows) and extract the data type.
    DataTypePtr readColumnAndGetDataType(const String & column_name, size_t & rows_read, size_t max_rows);

    const FormatSettings format_settings;
    String hints_str;
    std::unordered_map<String, DataTypePtr> hints;
    String hints_parsing_error;
    std::unique_ptr<JSONColumnsReaderBase> reader;
    Names column_names_from_settings;
    JSONInferenceInfo inference_info;

    size_t total_rows_read = 0;
    size_t max_rows_to_read;
    size_t max_bytes_to_read;
};

}
