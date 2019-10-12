#include <DataTypes/DataTypeDateTime.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <common/DateLUT.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Parsers/ASTLiteral.h>

#include <iomanip>

namespace
{
using namespace DB;
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

static inline void readText(DateTime64 & x, UInt32 scale, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTime64Text(x, scale, istr, time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            parseDateTime64BestEffort(x, scale, istr, time_zone, utc_time_zone);
            return;
    }
}
}

namespace DB
{

template <typename T>
bool protobufReadDateTime(ProtobufReader & protobuf, T & date_time)
{
    return protobuf.readDateTime(date_time);
}

template <>
bool protobufReadDateTime<DateTime64>(ProtobufReader & protobuf, DateTime64 & date_time)
{
    // TODO (vnemkov): protobuf.readDecimal ?
    return protobuf.readDateTime(date_time.value);
}

TimezoneMixin::TimezoneMixin(const std::string & time_zone_name)
    : has_explicit_time_zone(!time_zone_name.empty()),
    time_zone(DateLUT::instance(time_zone_name)),
    utc_time_zone(DateLUT::instance("UTC"))
{}

DataTypeDateTime::DataTypeDateTime(const std::string & time_zone_name)
    : TimezoneMixin(time_zone_name)
{
}

std::string DataTypeDateTime::doGetName() const
{
    if (!has_explicit_time_zone)
        return "DateTime";

    WriteBufferFromOwnString out;
    out << "DateTime(" << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

void DataTypeDateTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeDateTimeText(assert_cast<const ColumnType &>(column).getData()[row_num], ostr, time_zone);
}

void DataTypeDateTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void DataTypeDateTime::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void DataTypeDateTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x;
    ::readText(x, istr, settings, time_zone, utc_time_zone);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void DataTypeDateTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        ::readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void DataTypeDateTime::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x;
    if (checkChar('"', istr))
    {
        ::readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    ::readText(x, istr, settings, time_zone, utc_time_zone);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
//    if (value_index)
//        return;
    value_index = static_cast<bool>(protobuf.writeDateTime(assert_cast<time_t>(assert_cast<const ColumnType &>(column).getData()[row_num])));
}

void DataTypeDateTime::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    time_t t;
    if (!protobuf.readDateTime(t))
        return;

    auto & container = assert_cast<ColumnType &>(column).getData();
    if (allow_add_row)
    {
        container.emplace_back(t);
        row_added = true;
    }
    else
        container.back() = t;
}

bool DataTypeDateTime::equals(const IDataType & rhs) const
{
    /// DateTime with different timezones are equal, because:
    /// "all types with different time zones are equivalent and may be used interchangingly."
    return typeid(rhs) == typeid(*this);
}

DataTypeDateTime64::DataTypeDateTime64(UInt32 scale_, const std::string & time_zone_name)
    : DataTypeDecimalBase<DateTime64>(maxDecimalPrecision<DateTime64>() - scale_, scale_),
      TimezoneMixin(time_zone_name)
{
}

std::string DataTypeDateTime64::doGetName() const
{
    if (!has_explicit_time_zone)
        return std::string(getFamilyName()) + "(" + std::to_string(this->scale) + ")";

    WriteBufferFromOwnString out;
    out << "DateTime64(" << this->scale << ", " << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

void DataTypeDateTime64::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const
{
    writeDateTimeText(assert_cast<const ColumnType &>(column).getData()[row_num], scale, ostr, time_zone);
}

void DataTypeDateTime64::deserializeText(IColumn & /*column*/, ReadBuffer & /*istr*/, const FormatSettings &) const
{}

void DataTypeDateTime64::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void DataTypeDateTime64::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void DataTypeDateTime64::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x;
    ::readText(x, scale, istr, settings, time_zone, utc_time_zone);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime64::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void DataTypeDateTime64::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        ::readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void DataTypeDateTime64::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeDateTime64::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x;
    if (checkChar('"', istr))
    {
        ::readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime64::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeDateTime64::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    ::readText(x, scale, istr, settings, time_zone, utc_time_zone);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime64::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;
    value_index = static_cast<bool>(protobuf.writeDateTime64(assert_cast<const ColumnType &>(column).getData()[row_num], scale));
}

void DataTypeDateTime64::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    DateTime64 t;
    if (!protobuf.readDateTime64(t, scale))
        return;

    auto & container = assert_cast<ColumnType &>(column).getData();
    if (allow_add_row)
    {
        container.emplace_back(t);
        row_added = true;
    }
    else
        container.back() = t;
}

bool DataTypeDateTime64::equals(const IDataType & rhs) const
{
    if (auto * ptype = typeid_cast<const DataTypeDateTime64 *>(&rhs))
        return this->scale == ptype->getScale();
    return false;
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

enum class ArgumentKind
{
    Optional,
    Mandatory
};

template <typename T, ArgumentKind Kind>
std::conditional_t<Kind == ArgumentKind::Optional, std::optional<T>, T>
getArgument(const ASTPtr & arguments, size_t argument_index, const char * argument_name, const std::string context_data_type_name)
{
    using NearestResultType = NearestFieldType<T>;
    const auto fieldType = Field::TypeToEnum<NearestResultType>::value;
    const ASTLiteral * argument = nullptr;

    auto exceptionMessage = [=](const String & message)
    {
        return std::string("Parameter #") + std::to_string(argument_index) + " '"
                + argument_name + "' for " + context_data_type_name
                + message
                + ", expected: " + Field::Types::toString(fieldType) + " literal.";
    };

    if (!arguments || arguments->children.size() <= argument_index
        || !(argument = arguments->children[argument_index]->as<ASTLiteral>()))
    {
        if constexpr (Kind == ArgumentKind::Optional)
            return {};
        else
            throw Exception(exceptionMessage(" is missing"),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    if (argument->value.getType() != fieldType)
        throw Exception(exceptionMessage(String(" has wrong type: ") + argument->value.getTypeName()),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return argument->value.get<NearestResultType>();
}

static DataTypePtr create64(const ASTPtr & arguments)
{
    if (!arguments || arguments->size() == 0)
        return std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale);

    const auto scale = getArgument<UInt64, ArgumentKind::Optional>(arguments, 0, "scale", "DateType64");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, !!scale, "timezone", "DateType64");

    return std::make_shared<DataTypeDateTime64>(scale.value_or(DataTypeDateTime64::default_scale), timezone.value_or(String{}));
}

void registerDataTypeDateTime(DataTypeFactory & factory)
{
    factory.registerDataType("DateTime", create, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("DateTime64", create64, DataTypeFactory::CaseInsensitive);

    factory.registerAlias("TIMESTAMP", "DateTime", DataTypeFactory::CaseInsensitive);
}

}
