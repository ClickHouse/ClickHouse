#include "config.h"

#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ExcelCommon.h>
#include <Processors/Formats/Impl/XLSReader.h>
#include <Processors/Formats/Impl/XLSXReader.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

namespace
{

enum class ExcelFileFormat : uint8_t
{
    XLSX,
    XLS,
};

const char * formatName(ExcelFileFormat file_format)
{
    return file_format == ExcelFileFormat::XLSX ? "XLSX" : "XLS";
}

std::unique_ptr<Excel::RangeReader> openRange(ExcelFileFormat file_format, std::string_view data, const FormatSettings & settings)
{
    std::unique_ptr<Excel::ISheetReader> sheet_reader;
#if USE_MINIZIP
    if (file_format == ExcelFileFormat::XLSX)
        sheet_reader = Excel::openXLSXSheet(data, settings.excel.sheet);
#endif
    if (file_format == ExcelFileFormat::XLS)
        sheet_reader = Excel::openXLSSheet(data, settings.excel.sheet);
    chassert(sheet_reader);
    return std::make_unique<Excel::RangeReader>(std::move(sheet_reader), settings);
}


class ExcelInputFormat final : public IInputFormat
{
public:
    ExcelInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & settings_, UInt64 max_block_size_, ExcelFileFormat file_format_)
        : IInputFormat(header_, &in_)
        , settings(settings_)
        , max_block_size(max_block_size_)
        , file_format(file_format_)
    {
        for (const auto & column : *header_)
        {
            types.push_back(column.type);
            serializations.push_back(column.type->getDefaultSerialization());
        }
    }

    String getName() const override { return formatName(file_format); }

    void resetParser() override
    {
        IInputFormat::resetParser();
        data.clear();
        range_reader.reset();
        cells.clear();
        has_current_row = false;
        is_finished = false;
    }

private:
    Chunk read() override
    {
        if (is_finished)
            return {};

        if (!range_reader)
            initialize();

        auto columns = getPort().getHeader().cloneEmptyColumns();
        size_t num_rows = 0;
        while (num_rows < max_block_size && !isCancelled())
        {
            if (!has_current_row && !range_reader->next(cells))
            {
                is_finished = true;
                break;
            }
            has_current_row = false;

            for (size_t i = 0; i < columns.size(); ++i)
            {
                const auto & cell = i < cells.size() ? cells[i] : empty_cell;
                try
                {
                    Excel::insertCell(*columns[i], types[i], serializations[i], cell, range_reader->isDate1904(), settings);
                }
                catch (Exception & e)
                {
                    e.addMessage("while reading the column '{}' (column {} of the range) of row {} of the range",
                        getPort().getHeader().getByPosition(i).name, i + 1, num_rows_read + num_rows + 1);
                    throw;
                }
            }
            ++num_rows;
        }

        num_rows_read += num_rows;
        if (!num_rows)
            return {};
        return Chunk(std::move(columns), num_rows);
    }

    void initialize()
    {
        readStringUntilEOF(data, *in);
        range_reader = openRange(file_format, data, settings);

        if (!range_reader->next(cells))
        {
            is_finished = true;
            return;
        }
        if (!Excel::isHeaderRow(cells, settings))
            has_current_row = true;
    }

    const FormatSettings settings;
    const UInt64 max_block_size;
    const ExcelFileFormat file_format;
    DataTypes types;
    Serializations serializations;

    String data;
    std::unique_ptr<Excel::RangeReader> range_reader;
    std::vector<Excel::Cell> cells;
    const Excel::Cell empty_cell;
    /// The first row has already been read into `cells` to check if it is a header.
    bool has_current_row = false;
    bool is_finished = false;
    size_t num_rows_read = 0;
};


class ExcelSchemaReader final : public ISchemaReader
{
public:
    ExcelSchemaReader(ReadBuffer & in_, const FormatSettings & settings_, ExcelFileFormat file_format_)
        : ISchemaReader(in_)
        , settings(settings_)
        , file_format(file_format_)
    {
    }

    NamesAndTypesList readSchema() override
    {
        String data;
        readStringUntilEOF(data, in);
        auto range_reader = openRange(file_format, data, settings);

        std::vector<Excel::Cell> cells;
        if (!range_reader->next(cells))
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Cannot infer the table structure from {} data: the sheet (or the selected range) is empty",
                formatName(file_format));

        std::vector<String> names;
        std::vector<Excel::ColumnTypeInferrer> inferrers;
        size_t num_rows = 0;
        auto add_row = [&]
        {
            if (inferrers.size() < cells.size())
            {
                size_t old_size = inferrers.size();
                inferrers.resize(cells.size());
                /// The new columns were missing in the previous rows.
                if (num_rows)
                    for (size_t i = old_size; i < inferrers.size(); ++i)
                        inferrers[i].addMissing();
            }
            for (size_t i = 0; i < inferrers.size(); ++i)
            {
                if (i < cells.size())
                    inferrers[i].add(cells[i]);
                else
                    inferrers[i].addMissing();
            }
            ++num_rows;
        };

        if (Excel::isHeaderRow(cells, settings))
        {
            for (const auto & cell : cells)
                Excel::cellToString(cell, range_reader->isDate1904(), names.emplace_back());
        }
        else
            add_row();

        while (num_rows < settings.max_rows_to_read_for_schema_inference && range_reader->next(cells))
            add_row();

        size_t num_columns = std::max(names.size(), inferrers.size());
        NamesAndTypesList result;
        for (size_t i = 0; i < num_columns; ++i)
        {
            String name = i < names.size() && !names[i].empty() ? names[i] : "c" + std::to_string(i + 1);
            Excel::ColumnTypeInferrer missing_column;
            missing_column.addMissing();
            auto type = i < inferrers.size() ? inferrers[i].getType(settings) : missing_column.getType(settings);
            result.emplace_back(std::move(name), std::move(type));
        }
        return result;
    }

private:
    const FormatSettings settings;
    const ExcelFileFormat file_format;
};


String getAdditionalInfoForSchemaCache(const FormatSettings & settings)
{
    return fmt::format(
        "sheet={}, range={}, header={}, skip_empty_rows={}, schema_inference_make_columns_nullable={}, try_infer_integers={}",
        settings.excel.sheet,
        settings.excel.range,
        settings.excel.header ? (*settings.excel.header ? "1" : "0") : "auto",
        settings.excel.skip_empty_rows,
        settings.schema_inference_make_columns_nullable,
        settings.try_infer_integers);
}

void registerExcelInputFormat(FormatFactory & factory, ExcelFileFormat file_format)
{
    const char * name = formatName(file_format);
    factory.registerInputFormat(
        name,
        [file_format](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings & settings)
        {
            return std::make_shared<ExcelInputFormat>(
                buf, std::make_shared<const Block>(sample), settings, params.max_block_size_rows, file_format);
        });
    factory.registerFileExtension(file_format == ExcelFileFormat::XLSX ? "xlsx" : "xls", name);
}

void registerExcelSchemaReader(FormatFactory & factory, ExcelFileFormat file_format)
{
    const char * name = formatName(file_format);
    factory.registerSchemaReader(
        name,
        [file_format](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<ExcelSchemaReader>(buf, settings, file_format); });
    factory.registerAdditionalInfoForSchemaCacheGetter(name, getAdditionalInfoForSchemaCache);
}

constexpr auto input_settings_documentation = R"DOCS_MD(
| Setting | Description | Default |
|---------|-------------|---------|
| [`input_format_excel_sheet`](/reference/settings/formats/input-format#input_format_excel_sheet) | The name or the 1-based index of the sheet to read. If empty, the first visible worksheet is read. | `''` |
| [`input_format_excel_range`](/reference/settings/formats/input-format#input_format_excel_range) | The range of cells to read, like `B2:D10`, `B2:D`, `A:C`, `3:10`, or the top-left cell like `B2`. | `''` |
| [`input_format_excel_header`](/reference/settings/formats/input-format#input_format_excel_header) | Whether the first row of the range is a header with the column names: `1`, `0`, or `auto`. | `auto` |
| [`input_format_excel_skip_empty_rows`](/reference/settings/formats/input-format#input_format_excel_skip_empty_rows) | Skip empty rows inside the data instead of reading them as rows with empty cells. | `1` |
)DOCS_MD";

constexpr auto input_documentation = R"DOCS_MD(
## Reading {#reading}

ClickHouse reads one sheet of the workbook. By default, it is the first visible worksheet; you can select another one by its name or 1-based index with the `input_format_excel_sheet` setting.

By default, ClickHouse reads the whole used range of the sheet: columns start from the leftmost used column, and rows start from the first non-empty row. You can read a part of the sheet with the `input_format_excel_range` setting, for example `B3:F100`. The range can be open-ended: `B3:F` (from row 3 to the end of the data), `A:C` (whole columns), or just the top-left cell `B3`.

The first row of the range is the header with column names if all its non-empty cells are strings. This automatic detection can be overridden with the `input_format_excel_header` setting. Without a header, the columns are named `c1`, `c2`, ... in schema inference. Columns are matched by position.

Empty cells and cells with errors (like `#N/A` or `#DIV/0!`) are read as `NULL`, or as default values for non-`Nullable` columns. Formulas are not evaluated: ClickHouse reads the cached results that the spreadsheet application stored in the file.

## Data types matching {#data-types-matching}

Excel stores dates and times as numbers of days since 1900-01-01 (or since 1904-01-01 if the workbook uses the 1904 date system), and only the number format of the cell says that a number is a date. ClickHouse uses the number format to infer the types:

| Cell                                    | Inferred ClickHouse data type |
|-----------------------------------------|-------------------------------|
| Integer numbers                         | `Int64`                       |
| Other numbers                           | `Float64`                     |
| Numbers with a date format              | `Date32`                      |
| Numbers with a date and time format     | `DateTime64(3)`               |
| Numbers with a time format              | `String`, like `12:30:00`     |
| Boolean values                          | `Bool`                        |
| Strings, and columns with mixed values  | `String`                      |

The inferred types are `Nullable` (see the [`schema_inference_make_columns_nullable`](/reference/settings/formats/schema-inference#schema_inference_make_columns_nullable) setting). The number of rows used for schema inference is controlled by the `input_format_max_rows_to_read_for_schema_inference` setting.

When the table structure is specified explicitly, numbers are converted to the requested types: a number can be read into a numeric, `Decimal`, `Date`, `Date32`, `DateTime` or `DateTime64` column (a number is interpreted as an Excel date for the latter, in the time zone of the column), and strings are parsed with the text representation of the type. Any cell can be read into a `String` column: numbers are written in the shortest exact representation, dates as `YYYY-MM-DD` or `YYYY-MM-DD hh:mm:ss`, and boolean values as `true` and `false`.

## Format settings {#format-settings}
)DOCS_MD";

}

void registerInputFormatExcel(FormatFactory & factory);
void registerInputFormatExcel(FormatFactory & factory)
{
#if USE_MINIZIP
    registerExcelInputFormat(factory, ExcelFileFormat::XLSX);
    factory.setDocumentation("XLSX", Documentation{
        .description = String(R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `XLSX` format reads and writes Microsoft Excel workbooks in the Office Open XML format (`.xlsx` files), used by Excel 2007 and later, and supported by most spreadsheet applications.

## Example usage {#example-usage}

```bash
clickhouse-local --query "SELECT number, toString(number) AS s, today() + number AS d FROM numbers(10) FORMAT XLSX" > data.xlsx
clickhouse-local --query "SELECT * FROM file('data.xlsx')"
clickhouse-local --query "SELECT * FROM file('data.xlsx') SETTINGS input_format_excel_sheet = 'Sheet1', input_format_excel_range = 'A1:B5'"
```
)DOCS_MD") + input_documentation + input_settings_documentation + R"DOCS_MD(| [`output_format_xlsx_sheet_name`](/reference/settings/formats/output-format#output_format_xlsx_sheet_name) | The name of the sheet in the output. | `Sheet1` |
| [`output_format_xlsx_write_header`](/reference/settings/formats/output-format#output_format_xlsx_write_header) | Write the column names in the first row of the sheet. | `1` |

## Writing {#writing}

ClickHouse writes the result into a single sheet with the column names in the first row (the row is bold and frozen). If the result has more rows than an Excel sheet can contain (1048576), the following rows are written to additional sheets named `Sheet1 (2)`, `Sheet1 (3)`, and so on, each with its own header. A result with more than 16384 columns cannot be written.

| ClickHouse data type | Excel cell |
|----------------------|------------|
| Integers, floating-point numbers, `Decimal` | Number |
| `Bool` | Boolean |
| `Date`, `Date32` | Number with the `yyyy-mm-dd` format |
| `DateTime` | Number with the `yyyy-mm-dd hh:mm:ss` format, in the time zone of the column |
| `DateTime64` | Number with the `yyyy-mm-dd hh:mm:ss.000` format, in the time zone of the column |
| `NULL` | Empty cell |
| Other types | String, in the text representation of the type |

Excel stores numbers as 64-bit floating-point values, so it rounds integers with more than 15 significant digits for display. ClickHouse writes the exact value to the file, and reads it back exactly. `NaN` and infinite values, dates before 1900, and dates after 9999 are written as strings. Invalid UTF-8 sequences in strings are replaced with the replacement character `U+FFFD`, because Excel files cannot contain them.
)DOCS_MD"});
#endif

    registerExcelInputFormat(factory, ExcelFileFormat::XLS);
    factory.setDocumentation("XLS", Documentation{
        .description = String(R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

The `XLS` format reads Microsoft Excel workbooks in the legacy binary format (`.xls` files): the BIFF8 format of Excel 97-2003 and the BIFF5 format of Excel 5.0/95. Encrypted files and files of Excel 2.x-4.x are not supported. To write Excel files, use the [`XLSX`](/interfaces/formats/XLSX) format.

## Example usage {#example-usage}

```bash
clickhouse-local --query "SELECT * FROM file('data.xls')"
clickhouse-local --query "SELECT * FROM file('data.xls') SETTINGS input_format_excel_sheet = '2', input_format_excel_header = 0"
```
)DOCS_MD") + input_documentation + input_settings_documentation});
}

void registerExcelSchemaReaders(FormatFactory & factory);
void registerExcelSchemaReaders(FormatFactory & factory)
{
#if USE_MINIZIP
    registerExcelSchemaReader(factory, ExcelFileFormat::XLSX);
#endif
    registerExcelSchemaReader(factory, ExcelFileFormat::XLS);
}

}
