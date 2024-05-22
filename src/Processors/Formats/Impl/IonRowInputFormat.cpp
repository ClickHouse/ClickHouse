#include <Processors/Formats/Impl/IonRowInputFormat.h>

#if USE_ION

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int ILLEGAL_COLUMN;
extern const int NOT_IMPLEMENTED;
}

static void insertCommonInteger(IColumn & column, DataTypePtr type, Int64 value)
{
    switch (type->getTypeId())
    {
        case TypeIndex::UInt8: {
            assert_cast<ColumnUInt8 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Date:
            [[fallthrough]];
        case TypeIndex::UInt16: {
            assert_cast<ColumnUInt16 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime:
            [[fallthrough]];
        case TypeIndex::UInt32: {
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value));
            break;
        }
        case TypeIndex::UInt64: {
            assert_cast<ColumnUInt64 &>(column).insertValue(static_cast<UInt64>(value));
            break;
        }
        case TypeIndex::IPv4: {
            assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(static_cast<UInt32>(value)));
            break;
        }
        case TypeIndex::Enum8:
            [[fallthrough]];
        case TypeIndex::Int8: {
            assert_cast<ColumnInt8 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Enum16:
            [[fallthrough]];
        case TypeIndex::Int16: {
            assert_cast<ColumnInt16 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Date32:
            [[fallthrough]];
        case TypeIndex::Int32: {
            assert_cast<ColumnInt32 &>(column).insertValue(static_cast<Int32>(value));
            break;
        }
        case TypeIndex::Int64: {
            assert_cast<ColumnInt64 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime64: {
            assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Decimal32: {
            assert_cast<ColumnDecimal<Decimal32> &>(column).insertValue(static_cast<Int32>(value));
            break;
        }
        case TypeIndex::Decimal64: {
            assert_cast<ColumnDecimal<Decimal64> &>(column).insertValue(value);
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert ION integer into column with type {}.", type->getName());
    }
}

static void insertFloat(IColumn & column, DataTypePtr type, double value)
{
    if (WhichDataType(type).isFloat32())
        assert_cast<ColumnFloat32 &>(column).insertValue(static_cast<float>(value));
    else if (WhichDataType(type).isFloat64())
        assert_cast<ColumnFloat64 &>(column).insertValue(value);
    else
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert ION float into column with type {}.", type->getName());
}

static void insertBool(IColumn & column, DataTypePtr type, bool value)
{
    if (!WhichDataType(type).isUInt8())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert ION bool into column with type {}.", type->getName());
    assert_cast<ColumnUInt8 &>(column).insertValue(value);
}

static void insertDate(IColumn & column, DataTypePtr type, int year, int month, int day)
{
    LocalDate ld = LocalDate(year, month, day);
    if (WhichDataType(type).isDate())
        assert_cast<ColumnUInt16 &>(column).insertValue(ld.getDayNum());
    else if (WhichDataType(type).isDate32())
        assert_cast<ColumnUInt16 &>(column).insertValue(ld.getExtenedDayNum());
    else
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert ION timestamp (only date) into column with type {}.", type->getName());
}

static void insertDateTime(IColumn & column, DataTypePtr type, int year, int month, int day, int hour, int minute, int second)
{
    LocalDateTime ldt = LocalDateTime(year, month, day, hour, minute, second);
    if (WhichDataType(type).isDateTime())
        assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(ldt.to_time_t()));
    else if (WhichDataType(type).isDateTime64())
        assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(ldt.to_time_t());
    else
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Cannot insert ION timestamp (only date and time without fractions) into column with type {}.",
            type->getName());
}

template <typename ColumnType>
static void insertFromBinaryRepresentation(IColumn & column, DataTypePtr type, const char * value, size_t size)
{
    constexpr size_t column_type_size = sizeof(typename ColumnType::ValueType);
    if (size > column_type_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of {} value: {}", type->getName(), size);
    assert_cast<ColumnType &>(column).insertData(value, size);
}

static void insertBigInteger(IColumn & column, DataTypePtr type, const char * data, size_t size)
{
    switch (type->getTypeId())
    {
        case TypeIndex::IPv6:
            insertFromBinaryRepresentation<ColumnIPv6>(column, type, data, size);
            return;
        case TypeIndex::Int128:
            insertFromBinaryRepresentation<ColumnInt128>(column, type, data, size);
            return;
        case TypeIndex::Int256:
            insertFromBinaryRepresentation<ColumnInt256>(column, type, data, size);
            return;
        case TypeIndex::UInt128:
            insertFromBinaryRepresentation<ColumnUInt128>(column, type, data, size);
            return;
        case TypeIndex::UInt256:
            insertFromBinaryRepresentation<ColumnUInt256>(column, type, data, size);
            return;
        case TypeIndex::Decimal128:
            insertFromBinaryRepresentation<ColumnDecimal<Decimal128>>(column, type, data, size);
            return;
        case TypeIndex::Decimal256:
            insertFromBinaryRepresentation<ColumnDecimal<Decimal256>>(column, type, data, size);
            return;
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert ION big integer into column with type {}.", type->getName());
    }
}

static void insertString(IColumn & column, DataTypePtr type, const std::string & str)
{
    switch (type->getTypeId())
    {
        case TypeIndex::String: {
            assert_cast<ColumnString &>(column).insertData(str.data(), str.size());
            break;
        }
        case TypeIndex::FixedString: {
            assert_cast<ColumnFixedString &>(column).insertData(str.data(), str.size());
            break;
        }
        case TypeIndex::UUID: {
            ReadBufferFromMemory buf(str.data(), str.size());
            UUID uuid;
            readUUIDText(uuid, buf);
            assert_cast<ColumnUUID &>(column).insertValue(uuid);
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert ION string into column with type {}.", type->getName());
    }
}

static void insertNull(IColumn & column, DataTypePtr type)
{
    if (type->isNullable())
        assert_cast<ColumnNullable &>(column).getNullMapColumn().insertValue(0);
    else
        column.insertDefault();
}

static void insertIonInteger(IonIntPtr ion_int_ptr, IColumn & column, DataTypePtr type)
{
    SIZE bytes;
    ion_int_byte_length(ion_int_ptr.get(), &bytes);
    int32_t signum;
    ion_int_signum(ion_int_ptr.get(), &signum);
    if (static_cast<size_t>(bytes) <= sizeof(int64_t))
    {
        int64_t value;
        ion_int_to_int64(ion_int_ptr.get(), &value);
        insertCommonInteger(column, type, value);
    }
    else
    {
        BYTE buffer[bytes];
        ion_int_to_bytes(ion_int_ptr.get(), 0, buffer, bytes, nullptr);
        insertBigInteger(column, type, reinterpret_cast<char *>(buffer), bytes);
    }
}


IonRowInputFormat::IonRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_)
    : IRowInputFormat(header_, in_, std::move(params_)), reader(std::make_unique<IonReader>(in_)), data_types(header_.getDataTypes())
{
}

void IonRowInputFormat::readPrefix()
{
    reader->next();
    if (reader->currentType() == tid_EOF)
        throw Exception(ErrorCodes::INCORRECT_DATA, "There is no data in the buffer to read.");
    if (ION_TYPE_INT(reader->currentType()) != tid_LIST_INT)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not supported ION data schema.");
}

void IonRowInputFormat::deserializeField(IColumn & column, DataTypePtr type) {
    switch (ION_TYPE_INT(reader->currentType()))
    {
        case tid_STRING_INT: {
            if (!reader->isNull())
                insertString(column, type, reader->read<std::string>());
            else
                insertNull(column, type);
            break;
        }
        case tid_INT_INT: {
            if (!reader->isNull())
                insertIonInteger(reader->read<IonIntPtr>(), column, type);
            else
                insertNull(column, type);
            break;
        }
        case tid_FLOAT_INT: {
            if (!reader->isNull())
                insertFloat(column, type, reader->read<double>());
            else
                insertNull(column, type);
            break;
        }
        case tid_DECIMAL_INT: {
            if (!reader->isNull())
            {
                auto dec = reader->read<IonDecimalPtr>();
                auto integral_value_dec = reader->convertDecimalToInt(dec);
                insertIonInteger(reader->read<IonIntPtr>(), column, type);
            }
            else
            {
                insertNull(column, type);
            }
            break;
        }
        case tid_BOOL_INT: {
            if (!reader->isNull())
                insertBool(column, type, reader->read<bool>());
            else
                insertNull(column, type);
            break;
        }
        case tid_NULL_INT: {
            insertNull(column, type);
            break;
        }
        case tid_STRUCT_INT:
        case tid_LIST_INT:
        case tid_SEXP_INT:
        case tid_BLOB_INT:
        case tid_CLOB_INT:
            if (!reader->isNull())
                reader->stepIn();
            else
                insertNull(column, type);
            break;
        case tid_SYMBOL_INT: {
            if (!reader->isNull())
            {
                auto sym = reader->read<Symbol>();
                insertString(column, type, sym.value);
            }
            else
            {
                insertNull(column, type);
            }

            break;
        }
        case tid_TIMESTAMP_INT:
            if (!reader->isNull())
            {
                auto timestamp = reader->read<IonTimestampPtr>();
                int precision;
                ion_timestamp_get_precision(timestamp.get(), &precision);
                if (precision == ION_TS_DAY)
                {
                    int year, month, day;
                    ion_timestamp_get_thru_day(timestamp.get(), &year, &month, &day);
                    insertDate(column, type, year, month, day);
                }
                else if (precision == ION_TS_SEC)
                {
                    int year, month, day, hour, minute, second;
                    ion_timestamp_get_thru_second(timestamp.get(), &year, &month, &day, &hour, &minute, &second);
                    insertDateTime(column, type, year, month, day, hour, minute, second);
                }
                else
                {
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Not supported for ClickHouse timestamp format.");
                }
            }
            else
            {
                insertNull(column, type);
            }
            break;
        case tid_EOF_INT: {
            if (reader->depth() > 0)
                reader->stepOut();
            break;
        }
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown type");
    }
}

bool IonRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    // Read next and check that it is struct or list
    reader->next();
    if (reader->currentType() == tid_EOF)
        return false;
    if (ION_TYPE_INT(reader->currentType()) == tid_LIST_INT || ION_TYPE_INT(reader->currentType()) == tid_STRUCT_INT)
    { // [[], [], ...] or [{}, {}, ...]
        size_t num_obj = 0;
        reader->stepIn(); // go into the structure
        reader->next(); // read first object
        int row_list_depth = reader->depth();
        while (reader->depth() >= row_list_depth) // until we left the list structure - logical row
        {
            if (reader->depth() > 0 && reader->currentType() != tid_EOF)
                std::optional<std::string> name = reader->fieldName();
            deserializeField(*columns[num_obj], data_types[num_obj]);
            ++num_obj;
            reader->next(); // read next object
        }
        // reader->stepOut(); // leaving the structure
        return true;
    }
    else
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not supported ION data schema.");
}

void registerInputFormatIon(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Ion",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings &)
        { return std::make_shared<IonRowInputFormat>(sample, buf, params); });
    factory.registerFileExtension("ion", "Ion"); // text format
    factory.registerFileExtension("10n", "Ion"); // binary format
}
}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatMsgPack(FormatFactory &)
{
}

}

#endif
