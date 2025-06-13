#include "CriblInputFormat.h"
#include <algorithm>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeObject.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/castColumn.h>
#include <boost/algorithm/string.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <Common/assert_cast.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include "Columns/IColumn.h"
#include "Processors/Formats/Impl/LineAsStringRowInputFormat.h"

/// Name of the automatically added timestamp column
constexpr static auto TIME_COLUMN_NAME = "time";
/// Name of the automatically added JSON-like object column containing all parquet data
constexpr static auto PARSED_COLUMN_NAME = "data";
constexpr static auto RAW_COLUMN_NAME = "raw";
constexpr static auto SOURCE_COLUMN_NAME = "source";
constexpr static auto CRIBL_INPUT_FORMAT_NAME = "CriblInputFormat";

namespace DB
{
CriblInputFormat::CriblInputFormat(const Block & header_, ReadBuffer & in_, const FormatSettings &, const ReadSettings &)
    : IRowInputFormat(header_, in_, IRowInputFormat::Params())
{
}

String CriblInputFormat::getName() const
{
    return CRIBL_INPUT_FORMAT_NAME;
}


void CriblInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
}

void CriblInputFormat::readLineObject(MutableColumns & columns)
{
    ColumnDecimal<DateTime64> & column_time = assert_cast<ColumnDecimal<DateTime64> &>(*columns[0]);
    ColumnString & column_source = assert_cast<ColumnString &>(*columns[1]);
    ColumnString & column_string = assert_cast<ColumnString &>(*columns[2]);
    ColumnObject & column_object = assert_cast<ColumnObject &>(*columns[3]);

    const ReadBufferFromFileBase * bufferFile = dynamic_cast<ReadBufferFromFileBase *>(in);

    if (bufferFile)
    {
        column_source.insert(bufferFile->getFileName());
    }
    else
    {
        column_source.insert("N/A");
    }


    auto now = std::chrono::system_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    column_time.insertValue(ns / 1000);


    auto & chars = column_string.getChars();
    auto & offsets = column_string.getOffsets();

    String line;
    readStringUntilNewlineInto(line, *in);
    chars.insert(line.begin(), line.end());
    chars.push_back(0);
    offsets.push_back(line.size());
    Object row_data;

    // SimdJSONParser json_parser;
    // ReadBufferFromString buf(line);
    // auto document = json_parser.parse(buf);

    // Object row_data = column_object
    // for(size_t i =0;i<document.;++i){
        
    // }

    // //auto titi = DataTypeObject.doGetDefaultSerialization();
    column_object.insert(row_data);

    if (!in->eof())
        in->ignore(); /// Skip '\n'
}

bool CriblInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    readLineObject(columns);
    return true;
}

size_t CriblInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    while (!in->eof() && num_rows < max_block_size)
    {
        skipToNextLineOrEOF(*in);
        ++num_rows;
    }

    return num_rows;
}
/* Chunk CriblInputFormat::read()
{
    const Block & header = getPort().getHeader(); // our fixed output column names/types
    Columns columns; // array of our output columns
    columns.reserve(header.columns());

    Chunk oldchunk = source_line_as_string_row_format->generate();
    auto num_rows = oldchunk.getNumRows(); // memorize how many rows are in the chunk (before detaching)
    auto oldcolumns = oldchunk.detachColumns(); // take ownership of the columns

    for (const auto & column : header) // iterate over the (output) columns of the InputFormat (fixed columns)
    {
        if (column.name == TIME_COLUMN_NAME)
        {
            // TODO: Real code would map a parquet column of numeric type to DateTime64
            auto column_ptr = column.type->createColumn();
            auto * col = assert_cast<ColumnDecimal<DateTime64> *>(column_ptr.get());

            for (size_t i = 0; i < num_rows; i++)
            {
                auto now = std::chrono::system_clock::now();
                auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
                col->insertValue(ns / 1000);
            }
            columns.push_back(std::move(column_ptr));
        }
        else if (column.name == PARSED_COLUMN_NAME)
        {
            auto column_ptr = column.type->createColumn();
            auto * col = assert_cast<ColumnObject *>(column_ptr.get());
            //const auto & parquet_header = source_line_as_string_row_format->getPort().getHeader();

            // TODO: Implement more efficient way to move the parquet column into
            //       the ColumnObject directly instead of constructing a temprary
            //       Object.
            Object row_data;

            // For each row, create an object with all parquet columns
            for (size_t row = 0; row < num_rows; ++row)
            {

                // Create a Field containing the Object and insert into ColumnObject
                col->insert(row_data);
            }

            /// Create chunk with the ColumnObject
            columns.push_back(std::move(column_ptr));
        }
    }
        return Chunk(std::move(columns), num_rows);
} */


CriblSchemaReader::CriblSchemaReader(ReadBuffer & buf, const FormatSettings & set)
    : ISchemaReader(buf)
    , settings(set)
{
}


/// Read and return the schema of the parquet file
NamesAndTypesList CriblSchemaReader::readSchema()
{
    NamesAndTypesList columns;
    columns.emplace_back(TIME_COLUMN_NAME, std::make_shared<DataTypeDateTime64>(6));
    columns.emplace_back(SOURCE_COLUMN_NAME, std::make_shared<DataTypeString>());
    columns.emplace_back(RAW_COLUMN_NAME, std::make_shared<DataTypeString>());
    columns.emplace_back(PARSED_COLUMN_NAME, std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));
    //const ReadBufferFromFileBase & bufferFile = dynamic_cast<ReadBufferFromFileBase &>(in);
    //columns.emplace_back(bufferFile.getFileName() , std::make_shared<DataTypeString>());
    return columns;
}


static std::pair<bool, size_t> segmentationEngine(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
{
    char * pos = in.position();
    bool need_more_data = true;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        pos = find_first_symbols<'\n'>(pos, in.buffer().end());
        if (pos > in.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
        if (pos == in.buffer().end())
            continue;

        ++number_of_rows;
        if ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows))
            need_more_data = false;

        if (*pos == '\n')
            ++pos;
    }

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}


void registerFileSegmentationEngineCriblLineAsString(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine(CRIBL_INPUT_FORMAT_NAME, &segmentationEngine);
}


void registerInputFormatCribl(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        CRIBL_INPUT_FORMAT_NAME,
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & read_settings,
           bool,
           size_t,
           size_t) -> std::shared_ptr<IInputFormat> { return std::make_shared<CriblInputFormat>(sample, buf, settings, read_settings); });
    factory.markFormatSupportsSubsetOfColumns(CRIBL_INPUT_FORMAT_NAME);

    factory.registerSchemaReader(
        CRIBL_INPUT_FORMAT_NAME,
        [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<CriblSchemaReader>(buf, settings); });
}
}
