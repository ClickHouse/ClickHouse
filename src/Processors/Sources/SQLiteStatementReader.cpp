#include <Processors/Sources/SQLiteStatementReader.h>

#if USE_SQLITE

#include <base/sleep.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <algorithm>
#include <cmath>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int SQLITE_ENGINE_ERROR;
}

/// How long to sleep between sqlite3_step retries when the database is locked (SQLITE_BUSY).
/// Bounds how quickly the read reacts to cancellation while waiting for the lock.
static constexpr UInt64 sqlite_busy_retry_ms = 10;

namespace
{

const char * storageClassName(int storage_class)
{
    switch (storage_class)
    {
        case SQLITE_INTEGER:
            return "INTEGER";
        case SQLITE_FLOAT:
            return "REAL";
        case SQLITE_TEXT:
            return "TEXT";
        case SQLITE_BLOB:
            return "BLOB";
        case SQLITE_NULL:
            return "NULL";
        default:
            return "unknown";
    }
}

std::string_view getTextValue(sqlite3_stmt * statement, int idx)
{
    const char * data = reinterpret_cast<const char *>(sqlite3_column_text(statement, idx));
    int len = sqlite3_column_bytes(statement, idx);
    if (!data && len)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot read text value from SQLite database");

    return {data ? data : "", static_cast<size_t>(len)};
}

/// Whether a SQLite INTEGER cell holds a value that the native accessor of `value_type` reads back
/// unchanged. The accessors of the narrow integer types truncate the 64-bit cell into the column type.
bool integerFitsValueType(ExternalResultDescription::ValueType value_type, Int64 value)
{
    auto fits = []<typename T>(Int64 v) { return v >= static_cast<Int64>(std::numeric_limits<T>::min())
                                              && v <= static_cast<Int64>(std::numeric_limits<T>::max()); };

    switch (value_type)
    {
        case ExternalResultDescription::ValueType::vtUInt8:
            return fits.operator()<UInt8>(value);
        case ExternalResultDescription::ValueType::vtUInt16:
            return fits.operator()<UInt16>(value);
        case ExternalResultDescription::ValueType::vtUInt32:
            return fits.operator()<UInt32>(value);
        case ExternalResultDescription::ValueType::vtInt8:
            return fits.operator()<Int8>(value);
        case ExternalResultDescription::ValueType::vtInt16:
            return fits.operator()<Int16>(value);
        case ExternalResultDescription::ValueType::vtInt32:
            return fits.operator()<Int32>(value);
        default:
            /// `Int64` holds every INTEGER cell, and `UInt64` is read through the text path.
            return true;
    }
}

/// Whether the double round-trips through `Float32`, i.e. whether the narrowing cast in the read path
/// keeps the value. A NaN stays a NaN, and an infinity stays an infinity, so both are exact.
bool isExactlyRepresentableAsFloat32(double value)
{
    if (std::isnan(value))
        return true;

    return static_cast<double>(static_cast<Float32>(value)) == value;
}

template <typename ColumnType, typename Value>
void insertNativeValue(IColumn & column, Value value)
{
    if (auto * column_low_cardinality = typeid_cast<ColumnLowCardinality *>(&column))
    {
        auto full_column = ColumnType::create();
        full_column->insertValue(value);
        column_low_cardinality->insertFromFullColumn(*full_column, 0);
        return;
    }

    assert_cast<ColumnType &>(column).insertValue(value);
}

}

SQLiteStatementReader::ColumnReadInfo SQLiteStatementReader::createColumnReadInfoForNative(
    const ColumnWithTypeAndName & column,
    ValueType native_value_type,
    bool is_nullable) const
{
    ColumnReadInfo info;
    info.name = column.name;
    auto type_not_nullable = removeNullable(column.type);
    info.serialization = type_not_nullable->getDefaultSerialization();
    info.is_nullable = is_nullable;
    info.data_type = type_not_nullable;
    info.native_value_type = native_value_type;

    return info;
}

SQLiteStatementReader::ColumnReadInfo SQLiteStatementReader::createColumnReadInfoForText(const ColumnWithTypeAndName & column) const
{
    ColumnReadInfo info;
    info.name = column.name;
    info.serialization = column.type->getDefaultSerialization();
    info.is_nullable = canContainNull(*column.type);
    info.data_type = removeNullable(column.type);

    return info;
}

static std::optional<ExternalResultDescription::ValueType> getNativeFloatValueType(const ColumnWithTypeAndName & column)
{
    WhichDataType which(removeLowCardinalityAndNullable(column.type));

    if (which.isFloat32())
        return ExternalResultDescription::ValueType::vtFloat32;
    if (which.isFloat64())
        return ExternalResultDescription::ValueType::vtFloat64;

    return std::nullopt;
}

/// Whether `ExternalResultDescription` (shared with the other external database sources) has a value type
/// for this type, i.e. whether the native read path can decode it at all. The list mirrors the cases of
/// `ExternalResultDescription::init`; every other type (`Dynamic`, `Variant`, `Tuple`, `Map`, `IPv4`,
/// `IPv6`, `JSON`, ...) has no native accessor and is read through the text path instead. The geometric
/// types are deliberately left out: `ExternalResultDescription` describes them for the sake of `MySQL`'s
/// WKB decoding, while `insertValue` has no case for them and reads their text anyway.
static bool isDescribedByExternalResultDescription(const DataTypePtr & type_not_nullable)
{
    WhichDataType which(type_not_nullable);
    return which.isNativeInt() || which.isNativeUInt() || which.isInt256()
        || which.isFloat() || which.isString() || which.isFixedString()
        || which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64()
        || which.isTime() || which.isTime64()
        || which.isUUID() || which.isEnum() || which.isDecimal() || which.isArray();
}

SQLiteStatementReader::SQLiteStatementReader(
    const Block & sample_block_,
    const FormatSettings & format_settings_,
    ValueReadMode value_read_mode_,
    DeclaredTypeTrust declared_type_trust_)
    : format_settings(format_settings_)
    , declared_type_trust(declared_type_trust_)
{
    if (value_read_mode_ == ValueReadMode::Native)
    {
        columns_info.reserve(sample_block_.columns());

        for (const auto & column : sample_block_)
        {
            /// `ExternalResultDescription` (shared with the other external database sources) has no value
            /// type for the wide integers `Int128`, `UInt128` and `UInt256`, but the sink stores every wide
            /// integer as its ClickHouse text serialization (see `bindSQLiteValue`), so such columns are
            /// read through the text path instead of being rejected. A wide-integer value that fit into a
            /// SQLite INTEGER cell still renders as its decimal text, so the text path round-trips both
            /// storage classes.
            ///
            /// `LowCardinality(...)` wrappers are not unwrapped by `ExternalResultDescription` either, so an
            /// explicitly declared `LowCardinality` column also goes through the text path: its default
            /// serialization deserializes the rendered text straight into the `LowCardinality` column, and a
            /// numeric value stored as INTEGER or REAL renders as its decimal text, so this reads every
            /// storage class the sink produces.
            ///
            /// The same holds for every type `ExternalResultDescription` cannot describe at all (`Dynamic`,
            /// `Variant`, `Tuple`, `Map`, `IPv4`, `IPv6`, `JSON`, ...): the sink writes such a value as its
            /// ClickHouse text serialization (`bindSQLiteValue` binds everything without a native SQLite
            /// counterpart as text), so the text path reads it back, whereas `ExternalResultDescription::init`
            /// would throw `UNKNOWN_TYPE` on the first read of a table that was created without complaint.
            const auto type_not_nullable = removeLowCardinalityAndNullable(column.type);
            WhichDataType which(type_not_nullable);
            if (column.type->lowCardinality() || which.isInt128() || which.isUInt128() || which.isInt256() || which.isUInt256()
                || !isDescribedByExternalResultDescription(type_not_nullable))
            {
                sample_block.insert(column.cloneEmpty());
                columns_info.push_back(createColumnReadInfoForText(column));
                continue;
            }

            ExternalResultDescription description;
            description.init(Block{column});

            const auto & described_column = description.sample_block.getByPosition(0);
            const auto & [value_type, is_nullable] = description.types[0];
            sample_block.insert(described_column.cloneEmpty());
            columns_info.push_back(createColumnReadInfoForNative(described_column, value_type, is_nullable));
        }

        return;
    }

    sample_block = sample_block_.cloneEmpty();
    columns_info.reserve(sample_block.columns());

    for (const auto & column : sample_block)
    {
        auto native_float_value_type = getNativeFloatValueType(column);
        if (native_float_value_type)
            columns_info.push_back(createColumnReadInfoForNative(column, *native_float_value_type, canContainNull(*column.type)));
        else
            columns_info.push_back(createColumnReadInfoForText(column));
    }
}

Chunk SQLiteStatementReader::readChunk(sqlite3 * db, sqlite3_stmt * statement, UInt64 max_block_size, bool & finished, const std::function<bool()> & is_cancelled)
{
    finished = false;

    MutableColumns columns = sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (num_rows < max_block_size)
    {
        int status = sqlite3_step(statement);

        if (status == SQLITE_DONE)
        {
            finished = true;
            break;
        }

        if (status == SQLITE_INTERRUPT)
        {
            /// `sqlite3_interrupt` is issued by our own cancellation path on a connection dedicated to this
            /// reader. If the read was not cancelled, the interrupt came from elsewhere; finishing silently
            /// would then return a truncated result set as if it were complete, so fail instead.
            if (is_cancelled())
            {
                finished = true;
                break;
            }
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "SQLite statement was interrupted, but the query was not cancelled. Message: {}",
                sqlite3_errmsg(db));
        }

        if (status == SQLITE_BUSY)
        {
            /// The database is locked by another connection. Without this, the loop retries with no
            /// delay and busy-spins a full CPU core. Bail out on cancellation and back off before retrying.
            if (is_cancelled())
            {
                finished = true;
                break;
            }
            sleepForMilliseconds(sqlite_busy_retry_ms);
            continue;
        }

        if (status != SQLITE_ROW)
        {
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "Expected SQLITE_ROW status, but got status {}. Error: {}, Message: {}",
                status,
                sqlite3_errstr(status),
                sqlite3_errmsg(db));
        }

        int column_count = sqlite3_column_count(statement);
        if (column_count != static_cast<int>(columns_info.size()))
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "Expected {} columns from SQLite query, but got {}",
                columns_info.size(),
                column_count);

        resolveUndeclaredColumns(statement);

        for (int column_index = 0; column_index != column_count; ++column_index)
        {
            const auto & info = columns_info[column_index];

            if (sqlite3_column_type(statement, column_index) == SQLITE_NULL)
            {
                if (!info.is_nullable && !format_settings.null_as_default)
                    throw Exception(
                        ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN,
                        "Cannot insert NULL value into non-Nullable column {}",
                        info.name);

                columns[column_index]->insertDefault();
                continue;
            }

            if (info.native_value_type)
            {
                if (auto * column_nullable = typeid_cast<ColumnNullable *>(columns[column_index].get()))
                {
                    insertValue(column_nullable->getNestedColumn(), info, statement, column_index);
                    column_nullable->getNullMapData().emplace_back(false);
                }
                else
                    insertValue(*columns[column_index], info, statement, column_index);
            }
            else
            {
                insertValue(*columns[column_index], info, statement, column_index);
            }
        }

        ++num_rows;
    }

    return num_rows ? Chunk(std::move(columns), num_rows) : Chunk{};
}

void SQLiteStatementReader::resolveUndeclaredColumns(sqlite3_stmt * statement)
{
    if (undeclared_columns_resolved)
        return;

    /// `sqlite3_column_decltype` is valid as soon as the statement is prepared and does not change between
    /// rows, so this is settled on the first row and holds for the lifetime of the statement.
    for (size_t i = 0; i < columns_info.size(); ++i)
        columns_info[i].requires_exact_storage_class
            = declared_type_trust == DeclaredTypeTrust::Untrusted || sqlite3_column_decltype(statement, static_cast<int>(i)) == nullptr;

    undeclared_columns_resolved = true;
}

void SQLiteStatementReader::checkStorageClass(const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const
{
    const int storage_class = sqlite3_column_type(statement, idx);

    /// Whether the storage class of the cell is the one the native accessor of this type decodes without
    /// coercing it into another class.
    bool class_matches = false;
    /// Whether the value is also representable in the column type exactly. A matching storage class is not
    /// enough on its own: the accessors wrap within the class (`sqlite3_column_int` reads the INTEGER cell
    /// `300` into a `UInt8` as `44`) and round within it (`sqlite3_column_double` reads the REAL cell
    /// `16777217` into a `Float32` as `16777216`).
    bool value_is_exact = true;

    switch (*info.native_value_type)
    {
        case ValueType::vtUInt8:
        case ValueType::vtUInt16:
        case ValueType::vtUInt32:
        case ValueType::vtInt8:
        case ValueType::vtInt16:
        case ValueType::vtInt32:
        case ValueType::vtInt64:
        {
            class_matches = storage_class == SQLITE_INTEGER;
            if (class_matches)
                value_is_exact = integerFitsValueType(*info.native_value_type, sqlite3_column_int64(statement, idx));
            break;
        }
        case ValueType::vtFloat32:
        case ValueType::vtFloat64:
        {
            const bool is_float32 = *info.native_value_type == ValueType::vtFloat32;
            if (storage_class == SQLITE_INTEGER)
            {
                class_matches = true;

                /// An INTEGER cell converts to a double exactly only within +-2^53; beyond that
                /// `sqlite3_column_double` rounds it to the nearest double, so it is not the same value.
                const Int64 value = sqlite3_column_int64(statement, idx);
                value_is_exact = value >= -(1LL << 53) && value <= (1LL << 53);
                if (value_is_exact && is_float32)
                    value_is_exact = isExactlyRepresentableAsFloat32(static_cast<double>(value));
            }
            else if (storage_class == SQLITE_FLOAT)
            {
                class_matches = true;
                if (is_float32)
                    value_is_exact = isExactlyRepresentableAsFloat32(sqlite3_column_double(statement, idx));
            }
            break;
        }
        default:
            /// Every other value type is decoded from the text rendering of the cell, whatever its storage
            /// class, so there is no coercing accessor to guard.
            class_matches = true;
            break;
    }

    if (!class_matches)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cannot read a value of the SQLite storage class {} into column {} of type {}: "
            "a SQLite query result column carries no type contract for its cells - a declared type, when SQLite reports one at "
            "all, is taken from a single arm of the query - so its values must have the storage class of that type in every row. "
            "Select the column as a string (for example, by declaring the column as `String`, or by casting it to text in the SQLite query) "
            "to read values of mixed storage classes",
            storageClassName(storage_class),
            info.name,
            info.data_type->getName());

    if (!value_is_exact)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cannot read the SQLite value {} into column {} of type {}: "
            "a SQLite query result column carries no type contract for its cells, so its values are read without conversion "
            "and must be exactly representable in that type in every row. "
            "Declare the column with a type that holds the value (for example, `Int64` or `Float64`), or as `String` "
            "to read the values as text",
            getTextValue(statement, idx),
            info.name,
            info.data_type->getName());
}

void SQLiteStatementReader::insertValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const
{
    if (!info.native_value_type)
    {
        insertTextValue(column, info, statement, idx);
        return;
    }

    if (info.requires_exact_storage_class)
        checkStorageClass(info, statement, idx);

    switch (*info.native_value_type)
    {
        case ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(static_cast<UInt8>(sqlite3_column_int(statement, idx)));
            break;
        case ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(static_cast<UInt16>(sqlite3_column_int(statement, idx)));
            break;
        case ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(sqlite3_column_int64(statement, idx)));
            break;
        case ValueType::vtUInt64:
            /// SQLite has no unsigned 64-bit integer type, so the sink writes `UInt64` as text to preserve
            /// values above the signed 64-bit range (see `bindSQLiteValue`). `sqlite3_column_int64` would
            /// clamp such a cell to `INT64_MAX`, so read it back as text and parse the whole unsigned range.
            /// A value that fit in signed 64-bit and was stored as INTEGER still renders as its decimal text
            /// here, so this path round-trips both cases.
            insertTextValue(column, info, statement, idx);
            break;
        case ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(static_cast<Int8>(sqlite3_column_int(statement, idx)));
            break;
        case ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(static_cast<Int16>(sqlite3_column_int(statement, idx)));
            break;
        case ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(sqlite3_column_int(statement, idx));
            break;
        case ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(sqlite3_column_int64(statement, idx));
            break;
        case ValueType::vtFloat32:
        {
            const int storage_class = sqlite3_column_type(statement, idx);
            if (storage_class == SQLITE_TEXT)
            {
                insertTextValue(column, info, statement, idx);
                break;
            }

            /// `sqlite3_column_double` silently returns 0.0 for a `BLOB`, so reject any storage class
            /// that is neither a number nor text (the `SQLITE_NULL` case is handled by the caller).
            if (storage_class != SQLITE_FLOAT && storage_class != SQLITE_INTEGER)
                throw Exception(
                    ErrorCodes::SQLITE_ENGINE_ERROR,
                    "Cannot read a floating-point value for column {} from a SQLite BLOB value",
                    info.name);

            insertNativeValue<ColumnFloat32>(column, static_cast<Float32>(sqlite3_column_double(statement, idx)));
            break;
        }
        case ValueType::vtFloat64:
        {
            const int storage_class = sqlite3_column_type(statement, idx);
            if (storage_class == SQLITE_TEXT)
            {
                insertTextValue(column, info, statement, idx);
                break;
            }

            if (storage_class != SQLITE_FLOAT && storage_class != SQLITE_INTEGER)
                throw Exception(
                    ErrorCodes::SQLITE_ENGINE_ERROR,
                    "Cannot read a floating-point value for column {} from a SQLite BLOB value",
                    info.name);

            insertNativeValue<ColumnFloat64>(column, sqlite3_column_double(statement, idx));
            break;
        }
        case ValueType::vtEnum8:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnInt8 &>(column).insertValue(
                static_cast<Int8>(assert_cast<const DataTypeEnum<Int8> &>(*info.data_type).castToValue(value).safeGet<Int8>()));
            break;
        }
        case ValueType::vtEnum16:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnInt16 &>(column).insertValue(
                static_cast<Int16>(assert_cast<const DataTypeEnum<Int16> &>(*info.data_type).castToValue(value).safeGet<Int16>()));
            break;
        }
        case ValueType::vtString:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
            break;
        }
        case ValueType::vtDate:
        {
            auto value = getTextValue(statement, idx);
            ReadBufferFromString in(value);
            DayNum day;
            readDateText(day, in);
            assert_cast<ColumnUInt16 &>(column).insertValue(day);
            break;
        }
        case ValueType::vtDate32:
        {
            auto value = getTextValue(statement, idx);
            ReadBufferFromString in(value);
            ExtendedDayNum day;
            readDateText(day, in);
            assert_cast<ColumnInt32 &>(column).insertValue(day);
            break;
        }
        case ValueType::vtDateTime:
        {
            auto value = getTextValue(statement, idx);
            ReadBufferFromString in(value);
            time_t time = 0;
            readDateTimeText(time, in, assert_cast<const DataTypeDateTime &>(*info.data_type).getTimeZone());
            time = std::max<time_t>(time, 0);
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(time));
            break;
        }
        case ValueType::vtUUID:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnUUID &>(column).insert(parse<UUID>(value.data(), value.size()));
            break;
        }
        case ValueType::vtFixedString:
        {
            auto value = getTextValue(statement, idx);
            assert_cast<ColumnFixedString &>(column).insertData(value.data(), value.size());
            break;
        }
        default:
            insertTextValue(column, info, statement, idx);
            break;
    }
}

void SQLiteStatementReader::insertTextValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const
{
    auto value = getTextValue(statement, idx);
    ReadBufferFromString buffer(value);
    info.serialization->deserializeWholeText(column, buffer, format_settings);
}

}

#endif
