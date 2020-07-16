#include <Core/SettingsFields.h>

#include <Core/Field.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitors.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
    extern const int CANNOT_PARSE_BOOL;
}


template <typename Type>
String SettingFieldNumber<Type>::toString() const
{
    return DB::toString(value);
}

template <typename Type>
Field SettingFieldNumber<Type>::toField() const
{
    return value;
}

template <typename Type>
void SettingFieldNumber<Type>::set(Type x)
{
    value = x;
    changed = true;
}

template <typename Type>
void SettingFieldNumber<Type>::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(applyVisitor(FieldVisitorConvertToNumber<Type>(), x));
}

template <typename Type>
void SettingFieldNumber<Type>::set(const String & x)
{
    set(parseWithSizeSuffix<Type>(x));
}

template <>
void SettingFieldNumber<bool>::set(const String & x)
{
    if (x.size() == 1)
    {
        if (x[0] == '0')
            set(false);
        else if (x[0] == '1')
            set(true);
        else
            throw Exception("Cannot parse bool from string '" + x + "'", ErrorCodes::CANNOT_PARSE_BOOL);
    }
    else
    {
        ReadBufferFromString buf(x);
        if (checkStringCaseInsensitive("true", buf))
            set(true);
        else if (checkStringCaseInsensitive("false", buf))
            set(false);
        else
            throw Exception("Cannot parse bool from string '" + x + "'", ErrorCodes::CANNOT_PARSE_BOOL);
    }
}


template <typename Type>
void SettingFieldNumber<Type>::writeBinary(WriteBuffer & out) const
{
    if constexpr (is_integral_v<Type> && is_unsigned_v<Type>)
        writeVarUInt(static_cast<UInt64>(value), out);
    else if constexpr (is_integral_v<Type> && is_signed_v<Type>)
        writeVarInt(static_cast<Int64>(value), out);
    else
    {
        static_assert(std::is_floating_point_v<Type>);
        writeStringBinary(toString(), out);
    }
}

template <typename Type>
void SettingFieldNumber<Type>::readBinary(ReadBuffer & in)
{
    if constexpr (is_integral_v<Type> && is_unsigned_v<Type>)
    {
        UInt64 x;
        readVarUInt(x, in);
        set(static_cast<Type>(x));
    }
    else if constexpr (is_integral_v<Type> && is_signed_v<Type>)
    {
        Int64 x;
        readVarInt(x, in);
        set(static_cast<Type>(x));
    }
    else
    {
        static_assert(std::is_floating_point_v<Type>);
        String x;
        readStringBinary(x, in);
        set(x);
    }
}

template struct SettingFieldNumber<UInt64>;
template struct SettingFieldNumber<Int64>;
template struct SettingFieldNumber<float>;
template struct SettingFieldNumber<bool>;


String SettingFieldMaxThreads::toString() const
{
    /// Instead of the `auto` value, we output the actual value to make it easier to see.
    return is_auto ? ("auto(" + DB::toString(value) + ")") : DB::toString(value);
}

Field SettingFieldMaxThreads::toField() const
{
    return is_auto ? 0 : value;
}

void SettingFieldMaxThreads::set(UInt64 x)
{
    value = x ? x : getAutoValue();
    is_auto = x == 0;
    changed = true;
}

void SettingFieldMaxThreads::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), x));
}

void SettingFieldMaxThreads::set(const String & x)
{
    if (startsWith(x, "auto"))
        setAuto();
    else
        set(parse<UInt64>(x));
}

void SettingFieldMaxThreads::setAuto()
{
    value = getAutoValue();
    is_auto = true;
}

UInt64 SettingFieldMaxThreads::getAutoValue()
{
    static auto res = getNumberOfPhysicalCPUCores();
    return res;
}

void SettingFieldMaxThreads::writeBinary(WriteBuffer & out) const
{
    writeVarUInt(is_auto ? 0 : value, out);
}

void SettingFieldMaxThreads::readBinary(ReadBuffer & in)
{
    UInt64 x = 0;
    readVarUInt(x, in);
    set(x);
}


template <SettingFieldTimespanUnit unit>
String SettingFieldTimespan<unit>::toString() const
{
    return DB::toString(value.totalMicroseconds() / microseconds_per_unit);
}

template <SettingFieldTimespanUnit unit>
Field SettingFieldTimespan<unit>::toField() const
{
    return value.totalMicroseconds() / microseconds_per_unit;
}

template <SettingFieldTimespanUnit unit>
void SettingFieldTimespan<unit>::set(const Poco::Timespan & x)
{
    value = x;
    changed = true;
}

template <SettingFieldTimespanUnit unit>
void SettingFieldTimespan<unit>::set(UInt64 x)
{
    set(Poco::Timespan(x * microseconds_per_unit));
}

template <SettingFieldTimespanUnit unit>
void SettingFieldTimespan<unit>::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), x));
}

template <SettingFieldTimespanUnit unit>
void SettingFieldTimespan<unit>::set(const String & x)
{
    set(parse<UInt64>(x));
}

template <SettingFieldTimespanUnit unit>
void SettingFieldTimespan<unit>::writeBinary(WriteBuffer & out) const
{
    writeVarUInt(value.totalMicroseconds() / microseconds_per_unit, out);
}

template <SettingFieldTimespanUnit unit>
void SettingFieldTimespan<unit>::readBinary(ReadBuffer & in)
{
    UInt64 x = 0;
    readVarUInt(x, in);
    set(x);
}

template struct SettingFieldTimespan<SettingFieldTimespanUnit::SECOND>;
template struct SettingFieldTimespan<SettingFieldTimespanUnit::MILLISECOND>;


Field SettingFieldString::toField() const
{
    return value;
}

void SettingFieldString::set(const String & x)
{
    value = x;
    changed = true;
}

void SettingFieldString::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingFieldString::writeBinary(WriteBuffer & out) const
{
    writeStringBinary(value, out);
}

void SettingFieldString::readBinary(ReadBuffer & in)
{
    String s;
    readStringBinary(s, in);
    set(s);
}


String SettingFieldChar::toString() const
{
    return String(1, value);
}

Field SettingFieldChar::toField() const
{
    return toString();
}

void SettingFieldChar::set(char x)
{
    value = x;
    changed = true;
}

void SettingFieldChar::set(const String & x)
{
    if (x.size() > 1)
        throw Exception("A setting's value string has to be an exactly one character long", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
    char c = (x.size() == 1) ? x[0] : '\0';
    set(c);
}

void SettingFieldChar::set(const Field & x)
{
    const String & s = safeGet<const String &>(x);
    set(s);
}

void SettingFieldChar::writeBinary(WriteBuffer & out) const
{
    writeStringBinary(toString(), out);
}

void SettingFieldChar::readBinary(ReadBuffer & in)
{
    String s;
    readStringBinary(s, in);
    set(s);
}


String SettingFieldURI::toString() const
{
    return value.toString();
}

Field SettingFieldURI::toField() const
{
    return value.toString();
}

void SettingFieldURI::set(const Poco::URI & x)
{
    value = x;
    changed = true;
}

void SettingFieldURI::set(const Field & x)
{
    const String & s = safeGet<const String &>(x);
    set(s);
}

void SettingFieldURI::set(const String & x)
{
    set(Poco::URI(x));
}

void SettingFieldURI::writeBinary(WriteBuffer & buf) const
{
    writeStringBinary(toString(), buf);
}

void SettingFieldURI::readBinary(ReadBuffer & buf)
{
    String s;
    readStringBinary(s, buf);
    set(s);
}


void SettingFieldEnumHelpers::writeBinary(const std::string_view & str, WriteBuffer & out)
{
    writeStringBinary(str, out);
}

String SettingFieldEnumHelpers::readBinary(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    return str;
}

}
