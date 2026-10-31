#include <Processors/Formats/Impl/ExcelCommon.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/intExp10.h>
#include <Common/Hex.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>

#include <cmath>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int INCORRECT_DATA;
}

namespace Excel
{

namespace
{

/// The number of days between 1899-12-30 (day 0 of the 1900 date system, as most software counts it) and 1970-01-01.
constexpr Int64 DAYS_FROM_1900_EPOCH_TO_UNIX_EPOCH = 25569;
/// The number of days between 1904-01-01 and 1970-01-01.
constexpr Int64 DAYS_FROM_1904_EPOCH_TO_UNIX_EPOCH = 24107;
/// The serial number of 9999-12-31 in the 1900 date system, the last day Excel can represent.
constexpr double MAX_SERIAL = 2958465;

constexpr UInt32 MILLISECONDS_IN_DAY = 86400000;

/// http://howardhinnant.github.io/date_algorithms.html#civil_from_days
void civilFromDays(Int64 days, Int64 & year, UInt32 & month, UInt32 & day)
{
    days += 719468;
    const Int64 era = (days >= 0 ? days : days - 146096) / 146097;
    const UInt32 day_of_era = static_cast<UInt32>(days - era * 146097);
    const UInt32 year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
    year = static_cast<Int64>(year_of_era) + era * 400;
    const UInt32 day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    const UInt32 shifted_month = (5 * day_of_year + 2) / 153;
    day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    month = shifted_month < 10 ? shifted_month + 3 : shifted_month - 9;
    if (month <= 2)
        ++year;
}

void writeTwoDigits(UInt32 value, String & out)
{
    out += static_cast<char>('0' + value / 10);
    out += static_cast<char>('0' + value % 10);
}

void formatLocalDateTime(const LocalDateTime & value, NumberFormatKind kind, String & out)
{
    UInt32 ms = value.milliseconds_of_day;
    UInt32 hours = ms / 3600000;
    UInt32 minutes = ms / 60000 % 60;
    UInt32 seconds = ms / 1000 % 60;
    UInt32 fraction = ms % 1000;

    bool write_date = kind != NumberFormatKind::Time;
    bool write_time = kind == NumberFormatKind::Time || ms != 0;

    if (write_date)
    {
        Int64 year;
        UInt32 month;
        UInt32 day;
        civilFromDays(value.days_since_epoch, year, month, day);
        out += fmt::format("{:04}-", year);
        writeTwoDigits(month, out);
        out += '-';
        writeTwoDigits(day, out);
    }
    if (write_time)
    {
        if (write_date)
            out += ' ';
        writeTwoDigits(hours, out);
        out += ':';
        writeTwoDigits(minutes, out);
        out += ':';
        writeTwoDigits(seconds, out);
        if (fraction)
            out += fmt::format(".{:03}", fraction);
    }
}

void formatNumber(double value, String & out)
{
    WriteBufferFromString buf(out, AppendModeTag{});
    writeFloatText(value, buf);
    buf.finalize();
}

bool isBuiltinDateFormat(UInt32 id, NumberFormatKind & kind)
{
    if ((id >= 14 && id <= 17) || (id >= 27 && id <= 31) || id == 36 || (id >= 50 && id <= 54) || id == 57 || id == 58)
        kind = NumberFormatKind::Date;
    else if ((id >= 18 && id <= 21) || (id >= 32 && id <= 35) || (id >= 45 && id <= 47) || id == 55 || id == 56)
        kind = NumberFormatKind::Time;
    else if (id == 22)
        kind = NumberFormatKind::DateTime;
    else
        return false;
    return true;
}

/// Inserts a value given as text by using the text deserialization of the type.
void insertText(IColumn & column, const SerializationPtr & serialization, std::string_view text, const FormatSettings & format_settings)
{
    ReadBufferFromMemory buf(text.data(), text.size());
    serialization->deserializeWholeText(column, buf, format_settings);
}

bool tryInsertText(IColumn & column, const SerializationPtr & serialization, std::string_view text, const FormatSettings & format_settings)
{
    ReadBufferFromMemory buf(text.data(), text.size());
    return serialization->tryDeserializeWholeText(column, buf, format_settings);
}

LocalDateTime serialToLocalDateTimeOrThrow(double serial, bool date1904)
{
    auto result = serialToLocalDateTime(serial, date1904);
    if (!result)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "The number {} is not a valid Excel date", serial);
    return *result;
}

Int64 makeDateTime(const DateLUTImpl & time_zone, const LocalDateTime & value)
{
    Int64 year;
    UInt32 month;
    UInt32 day;
    civilFromDays(value.days_since_epoch, year, month, day);
    UInt32 seconds = value.milliseconds_of_day / 1000;
    return time_zone.makeDateTime(
        static_cast<Int16>(year),
        static_cast<UInt8>(month),
        static_cast<UInt8>(day),
        static_cast<UInt8>(seconds / 3600),
        static_cast<UInt8>(seconds / 60 % 60),
        static_cast<UInt8>(seconds % 60));
}

/// Inserts a number cell into a column of a date or date-time type. Returns false if the type is not a date or date-time type.
bool tryInsertSerialDate(IColumn & column, const IDataType & type, double serial, bool date1904)
{
    WhichDataType which(type);
    if (which.isDate())
    {
        auto value = serialToLocalDateTimeOrThrow(serial, date1904);
        if (value.days_since_epoch < 0 || value.days_since_epoch > DATE_LUT_MAX_DAY_NUM)
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "The Excel date {} is out of the range of the `Date` type", serial);
        assert_cast<ColumnDate &>(column).getData().push_back(static_cast<UInt16>(value.days_since_epoch));
        return true;
    }
    if (which.isDate32())
    {
        auto value = serialToLocalDateTimeOrThrow(serial, date1904);
        assert_cast<ColumnDate32 &>(column).getData().push_back(static_cast<Int32>(value.days_since_epoch));
        return true;
    }
    if (which.isDateTime())
    {
        auto value = serialToLocalDateTimeOrThrow(serial, date1904);
        /// Round to whole seconds.
        if (value.milliseconds_of_day % 1000 >= 500)
        {
            value.milliseconds_of_day += 1000;
            if (value.milliseconds_of_day >= MILLISECONDS_IN_DAY)
            {
                value.milliseconds_of_day -= MILLISECONDS_IN_DAY;
                ++value.days_since_epoch;
            }
        }
        Int64 time = makeDateTime(assert_cast<const DataTypeDateTime &>(type).getTimeZone(), value);
        if (time < 0 || time > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "The Excel date {} is out of the range of the `DateTime` type", serial);
        assert_cast<ColumnDateTime &>(column).getData().push_back(static_cast<UInt32>(time));
        return true;
    }
    if (which.isDateTime64())
    {
        const auto & date_time64_type = assert_cast<const DataTypeDateTime64 &>(type);
        auto value = serialToLocalDateTimeOrThrow(serial, date1904);
        Int64 time = makeDateTime(date_time64_type.getTimeZone(), value);
        UInt32 scale = date_time64_type.getScale();
        Int64 milliseconds = value.milliseconds_of_day % 1000;
        Int64 fraction = scale >= 3 ? milliseconds * intExp10(scale - 3) : milliseconds / intExp10(3 - scale);
        assert_cast<ColumnDateTime64 &>(column).getData().push_back(DateTime64(time * intExp10(scale) + fraction));
        return true;
    }
    return false;
}

void insertNotNullCell(
    IColumn & column,
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    const Cell & cell,
    bool date1904,
    const FormatSettings & format_settings)
{
    WhichDataType which(type);

    if (which.isString())
    {
        if (cell.type == CellType::String)
        {
            assert_cast<ColumnString &>(column).insertData(cell.text.data(), cell.text.size());
        }
        else
        {
            String text;
            cellToString(cell, date1904, text);
            assert_cast<ColumnString &>(column).insertData(text.data(), text.size());
        }
        return;
    }

    if (cell.type == CellType::String)
    {
        insertText(column, serialization, cell.text, format_settings);
        return;
    }

    if (cell.type == CellType::Bool)
    {
        if (which.isNativeInteger() || which.isFloat() || which.isDecimal())
            insertText(column, serialization, cell.number != 0 ? "1" : "0", format_settings);
        else
            insertText(column, serialization, cell.number != 0 ? "true" : "false", format_settings);
        return;
    }

    chassert(cell.type == CellType::Number);

    if (which.isFloat64())
    {
        assert_cast<ColumnFloat64 &>(column).getData().push_back(cell.number);
        return;
    }
    if (which.isFloat32())
    {
        assert_cast<ColumnFloat32 &>(column).getData().push_back(static_cast<Float32>(cell.number));
        return;
    }
    if (tryInsertSerialDate(column, *type, cell.number, date1904))
        return;

    if (which.isIntegerOrDecimal())
    {
        /// The original text is exact, while the double may be rounded (e.g. for integers above 2^53).
        /// But the original text may be like `1.0` or `1E+3`, which is not a valid integer.
        if (!cell.text.empty() && tryInsertText(column, serialization, cell.text, format_settings))
            return;
        String text;
        formatNumber(cell.number, text);
        insertText(column, serialization, text, format_settings);
        return;
    }

    String text;
    cellToString(cell, date1904, text);
    insertText(column, serialization, text, format_settings);
}

}

String columnName(UInt32 column)
{
    String result;
    UInt64 value = static_cast<UInt64>(column) + 1;
    while (value)
    {
        --value;
        result.insert(result.begin(), static_cast<char>('A' + value % 26));
        value /= 26;
    }
    return result;
}

bool parseCellReference(std::string_view reference, std::optional<UInt64> & row, std::optional<UInt32> & column)
{
    row.reset();
    column.reset();

    size_t pos = 0;
    if (pos < reference.size() && reference[pos] == '$')
        ++pos;

    UInt64 column_number = 0;
    size_t column_begin = pos;
    while (pos < reference.size() && isAlphaASCII(reference[pos]))
    {
        column_number = column_number * 26 + (toUpperIfAlphaASCII(reference[pos]) - 'A' + 1);
        if (column_number > XLSX_MAX_COLUMNS)
            return false;
        ++pos;
    }
    if (pos > column_begin)
        column = static_cast<UInt32>(column_number - 1);

    if (pos < reference.size() && reference[pos] == '$')
        ++pos;

    UInt64 row_number = 0;
    size_t row_begin = pos;
    while (pos < reference.size() && isNumericASCII(reference[pos]))
    {
        row_number = row_number * 10 + (reference[pos] - '0');
        if (row_number > XLSX_MAX_ROWS)
            return false;
        ++pos;
    }
    if (pos > row_begin)
    {
        if (row_number == 0)
            return false;
        row = row_number - 1;
    }

    return pos == reference.size() && (row || column);
}

CellRange parseCellRange(std::string_view range)
{
    auto throw_bad = [&]
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid cell range '{}'. Expected a range like 'B2:D10', 'B2:D', 'A:C', '3:10', or a top-left cell like 'B2'",
            range);
    };

    CellRange result;
    auto colon = range.find(':');
    std::string_view first = range.substr(0, colon);
    std::optional<UInt64> row;
    std::optional<UInt32> column;
    if (!parseCellReference(first, row, column))
        throw_bad();
    result.first_row = row.value_or(0);
    result.first_column = column.value_or(0);

    if (colon != std::string_view::npos)
    {
        if (!parseCellReference(range.substr(colon + 1), row, column))
            throw_bad();
        result.last_row = row;
        result.last_column = column;
        if ((result.last_row && *result.last_row < result.first_row) || (result.last_column && *result.last_column < result.first_column))
            throw_bad();
    }

    return result;
}

NumberFormatKind classifyNumberFormat(UInt32 builtin_id, std::string_view format_code)
{
    NumberFormatKind kind = NumberFormatKind::Number;
    if (format_code.empty())
    {
        isBuiltinDateFormat(builtin_id, kind);
        return kind;
    }

    bool has_date = false;
    bool has_time = false;
    bool has_month_or_minute = false;

    for (size_t pos = 0; pos < format_code.size(); ++pos)
    {
        char c = format_code[pos];
        if (c == ';')
            break; /// Only the first section (for positive numbers) matters.
        if (c == '"')
        {
            auto end = format_code.find('"', pos + 1);
            if (end == std::string_view::npos)
                break;
            pos = end;
        }
        else if (c == '\\' || c == '_' || c == '*')
        {
            ++pos; /// The next character is a literal (or a padding character).
        }
        else if (c == '[')
        {
            auto end = format_code.find(']', pos + 1);
            if (end == std::string_view::npos)
                break;
            /// Elapsed time like `[h]:mm:ss`; other brackets are colors, conditions and locales.
            std::string_view content = format_code.substr(pos + 1, end - pos - 1);
            if (!content.empty() && content.find_first_not_of("hHmMsS") == std::string_view::npos)
                has_time = true;
            pos = end;
        }
        else if (c == 'y' || c == 'Y' || c == 'd' || c == 'D')
            has_date = true;
        else if (c == 'h' || c == 'H' || c == 's' || c == 'S')
            has_time = true;
        else if (c == 'm' || c == 'M')
            has_month_or_minute = true;
        else if ((c == 'a' || c == 'A') && (format_code.substr(pos, 5) == "AM/PM" || format_code.substr(pos, 5) == "am/pm"
                     || format_code.substr(pos, 3) == "A/P" || format_code.substr(pos, 3) == "a/p"))
            has_time = true;
    }

    /// `m` is a month unless it is next to hours or seconds, in which case it is minutes.
    if (has_month_or_minute && !has_time)
        has_date = true;

    if (has_date && has_time)
        return NumberFormatKind::DateTime;
    if (has_date)
        return NumberFormatKind::Date;
    if (has_time)
        return NumberFormatKind::Time;
    return NumberFormatKind::Number;
}

std::optional<LocalDateTime> serialToLocalDateTime(double serial, bool date1904)
{
    if (!std::isfinite(serial) || serial < 0 || serial >= MAX_SERIAL + 1)
        return {};

    /// Excel stores times with the precision of milliseconds, but the double may be slightly off.
    Int64 total_milliseconds = std::llround(serial * MILLISECONDS_IN_DAY);
    Int64 days = total_milliseconds / MILLISECONDS_IN_DAY;

    LocalDateTime result;
    result.milliseconds_of_day = static_cast<UInt32>(total_milliseconds % MILLISECONDS_IN_DAY);
    if (date1904)
        result.days_since_epoch = days - DAYS_FROM_1904_EPOCH_TO_UNIX_EPOCH;
    else if (days < 61)
        /// Excel considers 1900 a leap year (for compatibility with Lotus 1-2-3), so the serial numbers before 1900-03-01 are off by one day.
        /// The non-existent 1900-02-29 (serial 60) becomes 1900-03-01.
        result.days_since_epoch = days - DAYS_FROM_1900_EPOCH_TO_UNIX_EPOCH + 1;
    else
        result.days_since_epoch = days - DAYS_FROM_1900_EPOCH_TO_UNIX_EPOCH;
    return result;
}

std::optional<double> localDateTimeToSerial(Int64 days_since_epoch, double seconds_of_day)
{
    Int64 serial_days = days_since_epoch + DAYS_FROM_1900_EPOCH_TO_UNIX_EPOCH;
    if (serial_days < 61)
        --serial_days;
    if (serial_days < 1 || static_cast<double>(serial_days) > MAX_SERIAL)
        return {};
    return static_cast<double>(serial_days) + seconds_of_day / 86400.0;
}

void cellToString(const Cell & cell, bool date1904, String & out)
{
    switch (cell.type)
    {
        case CellType::Empty:
        case CellType::Error:
            return;
        case CellType::String:
            out += cell.text;
            return;
        case CellType::Bool:
            out += cell.number != 0 ? "true" : "false";
            return;
        case CellType::Number:
            if (cell.format != NumberFormatKind::Number)
            {
                if (auto value = serialToLocalDateTime(cell.number, date1904))
                {
                    formatLocalDateTime(*value, cell.format, out);
                    return;
                }
            }
            formatNumber(cell.number, out);
            return;
    }
}

void decodeOOXMLEscapes(String & str)
{
    size_t pos = str.find("_x");
    if (pos == String::npos)
        return;

    String result;
    result.reserve(str.size());
    size_t copied = 0;
    while (pos != String::npos)
    {
        if (pos + 7 <= str.size() && str[pos + 6] == '_' && isHexDigit(str[pos + 2]) && isHexDigit(str[pos + 3]) && isHexDigit(str[pos + 4])
            && isHexDigit(str[pos + 5]))
        {
            result.append(str, copied, pos - copied);
            UInt32 code_point = 0;
            for (size_t i = 2; i < 6; ++i)
                code_point = code_point * 16 + unhex(str[pos + i]);
            /// Encode as UTF-8. Surrogates cannot appear in XML text, so the code point is always below 0x10000 here.
            if (code_point < 0x80)
                result += static_cast<char>(code_point);
            else if (code_point < 0x800)
            {
                result += static_cast<char>(0xC0 | (code_point >> 6));
                result += static_cast<char>(0x80 | (code_point & 0x3F));
            }
            else
            {
                result += static_cast<char>(0xE0 | (code_point >> 12));
                result += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
                result += static_cast<char>(0x80 | (code_point & 0x3F));
            }
            copied = pos + 7;
            pos = str.find("_x", copied);
        }
        else
        {
            pos = str.find("_x", pos + 1);
        }
    }
    result.append(str, copied, String::npos);
    str = std::move(result);
}

void insertCell(
    IColumn & column,
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    const Cell & cell,
    bool date1904,
    const FormatSettings & format_settings)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto & nullable_column = assert_cast<ColumnNullable &>(column);
        if (cell.isEmpty())
        {
            nullable_column.insertDefault();
            return;
        }
        const auto & nested_type = nullable_type->getNestedType();
        insertNotNullCell(
            nullable_column.getNestedColumn(), nested_type, nested_type->getDefaultSerialization(), cell, date1904, format_settings);
        nullable_column.getNullMapData().push_back(false);
        return;
    }

    if (cell.isEmpty())
    {
        if (!format_settings.null_as_default)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot insert an empty cell into a column of non-Nullable type {}. "
                "Enable the setting `input_format_null_as_default` to insert the default value instead",
                type->getName());
        column.insertDefault();
        return;
    }

    insertNotNullCell(column, type, serialization, cell, date1904, format_settings);
}

void ColumnTypeInferrer::add(const Cell & cell)
{
    switch (cell.type)
    {
        case CellType::Empty:
        case CellType::Error:
            has_empty = true;
            return;
        case CellType::String:
            has_string = true;
            return;
        case CellType::Bool:
            has_bool = true;
            return;
        case CellType::Number:
            switch (cell.format)
            {
                case NumberFormatKind::Date:
                    has_date = true;
                    return;
                case NumberFormatKind::DateTime:
                    has_date_time = true;
                    return;
                case NumberFormatKind::Time:
                    /// There is no data type for a time of day by default, so it is read as text like `12:30:00`.
                    has_string = true;
                    return;
                case NumberFormatKind::Number:
                    /// 2^63 is exactly representable as a double.
                    if (cell.number == std::trunc(cell.number) && std::abs(cell.number) < 9223372036854775808.0)
                        has_integer = true;
                    else
                        has_float = true;
                    return;
            }
    }
}

DataTypePtr ColumnTypeInferrer::getType(const FormatSettings & format_settings) const
{
    DataTypePtr type;
    size_t kinds = has_string + has_bool + (has_integer || has_float) + (has_date || has_date_time);
    if (kinds != 1 || has_string)
        type = std::make_shared<DataTypeString>();
    else if (has_bool)
        type = DataTypeFactory::instance().get("Bool");
    else if (has_date_time)
        type = std::make_shared<DataTypeDateTime64>(3);
    else if (has_date)
        type = std::make_shared<DataTypeDate32>();
    else if (has_float || !format_settings.try_infer_integers)
        type = std::make_shared<DataTypeFloat64>();
    else
        type = std::make_shared<DataTypeInt64>();

    /// 0 - never `Nullable`, 2 - `Nullable` if there are empty cells, 1 and 3 - always `Nullable` (the format has no metadata about nullability).
    bool make_nullable = format_settings.schema_inference_make_columns_nullable == 1 || format_settings.schema_inference_make_columns_nullable == 3
        || (format_settings.schema_inference_make_columns_nullable == 2 && (has_empty || kinds == 0));
    if (make_nullable)
        type = std::make_shared<DataTypeNullable>(type);
    return type;
}

RangeReader::RangeReader(std::unique_ptr<ISheetReader> sheet_reader_, const FormatSettings & format_settings)
    : sheet_reader(std::move(sheet_reader_))
    , skip_empty_rows(format_settings.excel.skip_empty_rows)
    , date1904(sheet_reader->isDate1904())
{
    if (!format_settings.excel.range.empty())
        range = parseCellRange(format_settings.excel.range);
    else if (auto dimension = sheet_reader->getDimension())
        /// Use only the top-left corner of the used range: the cells to the left of it are empty.
        range.first_column = dimension->first_column;
}

bool RangeReader::readNonEmptyRow()
{
    while (true)
    {
        if (!sheet_reader->readRow(row))
            return false;
        if (row.index < range.first_row)
            continue;
        if (range.last_row && row.index > *range.last_row)
            return false;
        if (previous_row_index && row.index <= *previous_row_index)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Rows of the sheet are not ordered: row {} follows row {}", row.index + 1, *previous_row_index + 1);

        bool has_value = false;
        for (const auto & [column, cell] : row.cells)
        {
            if (column >= range.first_column && (!range.last_column || column <= *range.last_column) && cell.type != CellType::Empty)
            {
                has_value = true;
                break;
            }
        }
        if (has_value)
            return true;
    }
}

bool RangeReader::next(std::vector<Cell> & cells)
{
    if (finished)
        return false;

    for (auto & cell : cells)
        cell.reset();

    if (pending_empty_rows)
    {
        --pending_empty_rows;
        cells.clear();
        return true;
    }

    if (!has_pending_row)
    {
        if (!readNonEmptyRow())
        {
            finished = true;
            return false;
        }

        /// Empty rows between non-empty rows: rows that are missing in the file or have only empty cells.
        if (!skip_empty_rows && seen_non_empty_row && row.index > *previous_row_index + 1)
        {
            pending_empty_rows = row.index - *previous_row_index - 1;
            has_pending_row = true;
            previous_row_index = row.index;
            --pending_empty_rows;
            cells.clear();
            return true;
        }
        previous_row_index = row.index;
    }

    has_pending_row = false;
    seen_non_empty_row = true;

    size_t width = 0;
    for (const auto & [column, cell] : row.cells)
        if (column >= range.first_column && (!range.last_column || column <= *range.last_column))
            width = column - range.first_column + 1;

    cells.resize(width);
    for (auto & [column, cell] : row.cells)
        if (column >= range.first_column && column - range.first_column < width)
            std::swap(cells[column - range.first_column], cell);

    return true;
}

bool isHeaderRow(const std::vector<Cell> & cells, const FormatSettings & format_settings)
{
    if (format_settings.excel.header)
        return *format_settings.excel.header;

    /// Automatic detection: the first row is a header if all its non-empty cells are strings.
    bool has_value = false;
    for (const auto & cell : cells)
    {
        if (cell.type == CellType::Empty)
            continue;
        if (cell.type != CellType::String)
            return false;
        has_value = true;
    }
    return has_value;
}

}

}
