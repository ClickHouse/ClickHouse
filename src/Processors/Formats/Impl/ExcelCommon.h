#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <optional>
#include <vector>


namespace DB
{

class IColumn;
struct FormatSettings;

/// Common code for the `XLSX` and `XLS` (Microsoft Excel) formats.
namespace Excel
{

/// Maximum sizes of a worksheet in the `XLSX` format.
constexpr UInt64 XLSX_MAX_ROWS = 1048576;
constexpr UInt32 XLSX_MAX_COLUMNS = 16384;

enum class CellType : uint8_t
{
    Empty,
    Number,
    String,
    Bool,
    Error,
};

/// What a number format of a cell displays a number as. Excel stores dates and times as numbers
/// (the number of days since an epoch), and only the number format tells that a number is a date.
enum class NumberFormatKind : uint8_t
{
    Number,
    Date,
    DateTime,
    Time,
};

struct Cell
{
    CellType type = CellType::Empty;
    NumberFormatKind format = NumberFormatKind::Number;
    /// The value of a `Number` cell, or 0/1 for a `Bool` cell.
    double number = 0;
    /// The value of a `String` cell, the error text (like `#N/A`) of an `Error` cell,
    /// or the original textual representation of a `Number` cell if the file has it (it is exact, unlike the double).
    String text;

    void reset()
    {
        type = CellType::Empty;
        format = NumberFormatKind::Number;
        number = 0;
        text.clear();
    }

    bool isEmpty() const { return type == CellType::Empty || type == CellType::Error; }
};

/// A row of a worksheet as stored in the file: only the cells that are present, ordered by column.
struct Row
{
    /// 0-based.
    UInt64 index = 0;
    std::vector<std::pair<UInt32, Cell>> cells;
};

/// A rectangular range of cells. All indices are 0-based; the last row and column are inclusive.
struct CellRange
{
    UInt64 first_row = 0;
    UInt32 first_column = 0;
    std::optional<UInt64> last_row;
    std::optional<UInt32> last_column;
};

/// Reads the rows of one worksheet.
class ISheetReader
{
public:
    virtual ~ISheetReader() = default;

    /// Reads the next row that is present in the file. Rows are returned in ascending order of their index.
    virtual bool readRow(Row & row) = 0;

    /// The range of cells used in the sheet, if the file contains it.
    virtual std::optional<CellRange> getDimension() const = 0;

    /// Whether dates are counted from 1904-01-01 instead of 1900-01-01.
    virtual bool isDate1904() const = 0;
};

/// `A` -> 0, `Z` -> 25, `AA` -> 26.
String columnName(UInt32 column);

/// Parses a cell reference like `B12` into a 0-based row and column. The row or the column may be missing (`B` or `12`).
/// Returns false if the reference is malformed.
bool parseCellReference(std::string_view reference, std::optional<UInt64> & row, std::optional<UInt32> & column);

/// Parses a range like `B2:D10`, `B2:D`, `A:C`, `3:10` or `B2` (only the top-left corner).
CellRange parseCellRange(std::string_view range);

/// Classifies a number format given by its built-in id and its format code (the code is empty for built-in formats).
NumberFormatKind classifyNumberFormat(UInt32 builtin_id, std::string_view format_code);

/// A point in time as Excel represents it: a civil date and the time of day without a time zone.
struct LocalDateTime
{
    Int64 days_since_epoch = 0; /// Since 1970-01-01.
    UInt32 milliseconds_of_day = 0;
};

/// Converts an Excel serial date (the number of days since the epoch of the workbook) to a civil date and time.
std::optional<LocalDateTime> serialToLocalDateTime(double serial, bool date1904);

/// Converts a civil date and the time of day to an Excel serial date in the 1900 date system.
/// Returns nothing for dates before 1900-01-01 or after 9999-12-31, which Excel cannot represent.
std::optional<double> localDateTimeToSerial(Int64 days_since_epoch, double seconds_of_day);

/// Formats a cell as text, like it would look in the `String` column.
void cellToString(const Cell & cell, bool date1904, String & out);

/// Decodes the `_xHHHH_` escape sequences that Office Open XML uses for characters that cannot be represented in XML.
void decodeOOXMLEscapes(String & str);

/// Inserts a cell into a column of the specified type.
void insertCell(
    IColumn & column,
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    const Cell & cell,
    bool date1904,
    const FormatSettings & format_settings);

/// Accumulates the kinds of values in a column for schema inference.
class ColumnTypeInferrer
{
public:
    void add(const Cell & cell);
    void addMissing() { has_empty = true; }
    DataTypePtr getType(const FormatSettings & format_settings) const;

private:
    bool has_empty = false;
    bool has_integer = false;
    bool has_float = false;
    bool has_bool = false;
    bool has_date = false;
    bool has_date_time = false;
    bool has_string = false;
};

/** Reads the rows of the selected range of a sheet and returns them as dense arrays of cells.
  * Rows above and columns to the left of the range are skipped. Empty rows before the first non-empty row are always skipped,
  * empty rows after the last non-empty row are never returned, and empty rows in between are skipped if `skip_empty_rows` is set.
  */
class RangeReader
{
public:
    RangeReader(std::unique_ptr<ISheetReader> sheet_reader_, const FormatSettings & format_settings);

    /// Returns the next row. `cells[i]` is the cell in the i-th column of the range; `cells` is only as long as needed.
    bool next(std::vector<Cell> & cells);

    bool isDate1904() const { return date1904; }

private:
    bool readNonEmptyRow();

    std::unique_ptr<ISheetReader> sheet_reader;
    CellRange range;
    bool skip_empty_rows;
    bool date1904;

    Row row;
    bool has_pending_row = false;
    bool finished = false;
    bool seen_non_empty_row = false;
    std::optional<UInt64> previous_row_index;
    UInt64 pending_empty_rows = 0;
};

/// Decides whether the first row of the range is a header, according to the `input_format_excel_header` setting.
bool isHeaderRow(const std::vector<Cell> & cells, const FormatSettings & format_settings);

}

}
