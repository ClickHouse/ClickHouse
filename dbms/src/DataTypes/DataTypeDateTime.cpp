#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>

#include <common/DateLUT.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTLiteral.h>

#include <iomanip>

namespace DB
{

template<typename NumberBase>
struct TypeGetter;

template<>
struct TypeGetter<UInt32> {
    using Type = time_t;
    using Column = ColumnUInt32;
    using Convertor = time_t;

    // This is not actually true, which is bad form as it truncates the value from time_t (long int) into uint32_t
    // static_assert(sizeof(Column::value_type) == sizeof(Type));

    static constexpr TypeIndex Index = TypeIndex::DateTime;
    static constexpr const char * Name = "DateTime";
};

template<>
struct TypeGetter<DateTime64::Type> {
    using Type = DateTime64::Type;
    using Column = ColumnUInt64;
    using Convertor = DateTime64;

    static_assert(sizeof(Column::value_type) == sizeof(Type));

    static constexpr TypeIndex Index = TypeIndex::DateTime64;
    static constexpr const char * Name = "DateTime64";
};

template<typename NumberBase>
DataTypeDateTimeBase<NumberBase>::DataTypeDateTimeBase(const std::string & time_zone_name)
    : has_explicit_time_zone(!time_zone_name.empty()),
    time_zone(DateLUT::instance(time_zone_name)),
    utc_time_zone(DateLUT::instance("UTC"))
{
}

template<typename NumberBase>
const char * DataTypeDateTimeBase<NumberBase>::getFamilyName() const
{
    return TypeGetter<NumberBase>::Name;
}

template<typename NumberBase>
std::string DataTypeDateTimeBase<NumberBase>::doGetName() const
{
    if (!has_explicit_time_zone)
        return TypeGetter<NumberBase>::Name;

    WriteBufferFromOwnString out;
    out << TypeGetter<NumberBase>::Name << "(" << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

template<typename NumberBase>
TypeIndex DataTypeDateTimeBase<NumberBase>::getTypeId() const
{
    return TypeGetter<NumberBase>::Index;
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    using TG = TypeGetter<NumberBase>;
    writeDateTimeText(typename TG::Convertor(assert_cast<const typename TG::Column &>(column).getData()[row_num]), ostr, time_zone);
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}


static inline void readText(time_t & x, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTimeText(x, istr, time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            parseDateTimeBestEffort(x, istr, time_zone, utc_time_zone);
            return;
    }
}

static inline void readText(DateTime64 & x, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & /*utc_time_zone*/)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTimeText(x, istr, time_zone);
            return;
        default:
            return;
    }
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    typename TypeGetter<NumberBase>::Type x = 0;
    readText(x, istr, settings, time_zone, utc_time_zone);

    assert_cast<typename TypeGetter<NumberBase>::Column &>(column).getData().push_back(x);
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    typename TypeGetter<NumberBase>::Type x;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }

    assert_cast<typename TypeGetter<NumberBase>::Column &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    typename TypeGetter<NumberBase>::Type x;
    if (checkChar('"', istr))
    {
        readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }

    assert_cast<typename TypeGetter<NumberBase>::Column &>(column).getData().push_back(x);
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    typename TypeGetter<NumberBase>::Type x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    readText(x, istr, settings, time_zone, utc_time_zone);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<typename TypeGetter<NumberBase>::Column &>(column).getData().push_back(x);
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;

    typename TypeGetter<NumberBase>::Type t = assert_cast<const typename TypeGetter<NumberBase>::Column &>(column).getData()[row_num];
    value_index = assert_cast<bool>(protobuf.writeDateTime(t));
}

template<typename NumberBase>
void DataTypeDateTimeBase<NumberBase>::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    typename TypeGetter<NumberBase>::Type t;
    if (!protobuf.readDateTime(t))
        return;

    auto & container = assert_cast<typename TypeGetter<NumberBase>::Column &>(column).getData();
    if (allow_add_row)
    {
        container.emplace_back(t);
        row_added = true;
    }
    else
        container.back() = t;
}

template<typename NumberBase>
bool DataTypeDateTimeBase<NumberBase>::equals(const IDataType & rhs) const
{
    /// DateTime with different timezones are equal, because:
    /// "all types with different time zones are equivalent and may be used interchangingly."
    return typeid(rhs) == typeid(*this);
}


namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<DataTypeDateTime>();

    if (arguments->children.size() != 1)
        throw Exception("DateTime data type can optionally have only one argument - time zone name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * arg = arguments->children[0]->as<ASTLiteral>();
    if (!arg || arg->value.getType() != Field::Types::String)
        throw Exception("Parameter for DateTime data type must be string literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeDateTime>(arg->value.get<String>());
}

static DataTypePtr create64(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<DataTypeDateTime64>();

    if (arguments->children.size() != 1)
        throw Exception("DateTime64 data type can optionally have only one argument - time zone name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * timezone_arg = arguments->children[0]->as<ASTLiteral>();
    if (!timezone_arg || timezone_arg->value.getType() != Field::Types::String)
        throw Exception("Timezone parameter for DateTime64 data type must be string literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeDateTime64>(timezone_arg->value.get<String>());
}

void registerDataTypeDateTime(DataTypeFactory & factory)
{
    factory.registerDataType("DateTime", create, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("DateTime64", create64, DataTypeFactory::CaseInsensitive);

    factory.registerAlias("TIMESTAMP", "DateTime", DataTypeFactory::CaseInsensitive);
}


}
