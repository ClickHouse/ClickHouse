#include <Core/SettingsFields.h>

#include <Core/Field.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/string/predicate.hpp>

#include <cmath>


namespace DB
{
namespace ErrorCodes
{
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_NUMBER;
}


namespace
{
    template <typename T>
    T stringToNumber(const String & str)
    {
        if constexpr (std::is_same_v<T, bool>)
        {
            if (str == "0")
                return false;
            if (str == "1")
                return true;
            if (boost::iequals(str, "false"))
                return false;
            if (boost::iequals(str, "true"))
                return true;
            throw Exception("Cannot parse bool from string '" + str + "'", ErrorCodes::CANNOT_PARSE_BOOL);
        }
        else
            return parseWithSizeSuffix<T>(str);
    }

    template <typename T>
    T fieldToNumber(const Field & f)
    {
        if (f.getType() == Field::Types::String)
            return stringToNumber<T>(f.get<const String &>());
        else
            return applyVisitor(FieldVisitorConvertToNumber<T>(), f);
    }
}

template <typename T>
SettingFieldNumber<T>::SettingFieldNumber(const Field & f) : SettingFieldNumber(fieldToNumber<T>(f))
{
}

template <typename T>
SettingFieldNumber<T> & SettingFieldNumber<T>::operator=(const Field & f)
{
    *this = fieldToNumber<T>(f);
    return *this;
}

template <typename T>
String SettingFieldNumber<T>::toString() const
{
    return ::DB::toString(value);
}

template <typename T>
void SettingFieldNumber<T>::parseFromString(const String & str)
{
    *this = stringToNumber<T>(str);
}

template <typename T>
void SettingFieldNumber<T>::writeBinary(WriteBuffer & out) const
{
    if constexpr (std::is_integral_v<T> && is_unsigned_v<T>)
        writeVarUInt(static_cast<UInt64>(value), out);
    else if constexpr (std::is_integral_v<T> && is_signed_v<T>)
        writeVarInt(static_cast<Int64>(value), out);
    else
    {
        static_assert(std::is_floating_point_v<T>);
        writeStringBinary(::DB::toString(value), out);
    }
}

template <typename T>
void SettingFieldNumber<T>::readBinary(ReadBuffer & in)
{
    if constexpr (std::is_integral_v<T> && is_unsigned_v<T>)
    {
        UInt64 x;
        readVarUInt(x, in);
        *this = static_cast<T>(x);
    }
    else if constexpr (std::is_integral_v<T> && is_signed_v<T>)
    {
        Int64 x;
        readVarInt(x, in);
        *this = static_cast<T>(value);
    }
    else
    {
        static_assert(std::is_floating_point_v<T>);
        String str;
        readStringBinary(str, in);
        *this = ::DB::parseFromString<T>(str);
    }
}

template struct SettingFieldNumber<UInt64>;
template struct SettingFieldNumber<Int64>;
template struct SettingFieldNumber<float>;
template struct SettingFieldNumber<bool>;


namespace
{
    UInt64 stringToMaxThreads(const String & str)
    {
        if (startsWith(str, "auto"))
            return 0;
        return parseFromString<UInt64>(str);
    }

    UInt64 fieldToMaxThreads(const Field & f)
    {
        if (f.getType() == Field::Types::String)
            return stringToMaxThreads(f.get<const String &>());
        else
            return applyVisitor(FieldVisitorConvertToNumber<UInt64>(), f);
    }
}

SettingFieldMaxThreads::SettingFieldMaxThreads(const Field & f) : SettingFieldMaxThreads(fieldToMaxThreads(f))
{
}

SettingFieldMaxThreads & SettingFieldMaxThreads::operator=(const Field & f)
{
    *this = fieldToMaxThreads(f);
    return *this;
}

String SettingFieldMaxThreads::toString() const
{
    if (is_auto)
        return "'auto(" + ::DB::toString(value) + ")'";
    else
        return ::DB::toString(value);
}

void SettingFieldMaxThreads::parseFromString(const String & str)
{
    *this = stringToMaxThreads(str);
}

void SettingFieldMaxThreads::writeBinary(WriteBuffer & out) const
{
    writeVarUInt(is_auto ? 0 : value, out);
}

void SettingFieldMaxThreads::readBinary(ReadBuffer & in)
{
    UInt64 x = 0;
    readVarUInt(x, in);
    *this = x;
}

UInt64 SettingFieldMaxThreads::getAuto()
{
    return getNumberOfPhysicalCPUCores();
}

namespace
{
    Poco::Timespan::TimeDiff float64AsSecondsToTimespan(Float64 d)
    {
        if (d != 0.0 && !std::isnormal(d))
            throw Exception(
                ErrorCodes::CANNOT_PARSE_NUMBER, "A setting's value in seconds must be a normal floating point number or zero. Got {}", d);
        return static_cast<Poco::Timespan::TimeDiff>(d * 1000000);
    }

}

template <>
SettingFieldSeconds::SettingFieldTimespan(const Field & f)
    : SettingFieldTimespan(Poco::Timespan{float64AsSecondsToTimespan(fieldToNumber<Float64>(f))})
{
}

template <>
SettingFieldMilliseconds::SettingFieldTimespan(const Field & f) : SettingFieldTimespan(fieldToNumber<UInt64>(f))
{
}

template <>
SettingFieldTimespan<SettingFieldTimespanUnit::Second> & SettingFieldSeconds::operator=(const Field & f)
{
    *this = Poco::Timespan{float64AsSecondsToTimespan(fieldToNumber<Float64>(f))};
    return *this;
}

template <>
SettingFieldTimespan<SettingFieldTimespanUnit::Millisecond> & SettingFieldMilliseconds::operator=(const Field & f)
{
    *this = fieldToNumber<UInt64>(f);
    return *this;
}

template <>
String SettingFieldSeconds::toString() const
{
    return ::DB::toString(static_cast<Float64>(value.totalMicroseconds()) / microseconds_per_unit);
}

template <>
String SettingFieldMilliseconds::toString() const
{
    return ::DB::toString(operator UInt64());
}

template <>
SettingFieldSeconds::operator Field() const
{
    return static_cast<Float64>(value.totalMicroseconds()) / microseconds_per_unit;
}

template <>
SettingFieldMilliseconds::operator Field() const
{
    return operator UInt64();
}

template <>
void SettingFieldSeconds::parseFromString(const String & str)
{
    Float64 n = parse<Float64>(str.data(), str.size());
    *this = Poco::Timespan{static_cast<Poco::Timespan::TimeDiff>(n * microseconds_per_unit)};
}

template <>
void SettingFieldMilliseconds::parseFromString(const String & str)
{
    *this = stringToNumber<UInt64>(str);
}

template <SettingFieldTimespanUnit unit_>
void SettingFieldTimespan<unit_>::writeBinary(WriteBuffer & out) const
{
    /// Note that this returns an UInt64 (for both seconds and milliseconds units) for compatibility reasons as the value
    /// for seconds used to be a integer (now a Float64)
    /// This method is only used to communicate with clients or servers older than DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
    /// in which the value was passed as binary (as a UInt64)
    /// Later versions pass the setting values as String (using toString() and parseFromString()) and there passing "1.2" will
    /// lead to `1` on releases with integer seconds or `1.2` on more recent releases
    /// See https://github.com/ClickHouse/ClickHouse/issues/36940 for more details
    auto num_units = operator UInt64();
    writeVarUInt(num_units, out);
}

template <SettingFieldTimespanUnit unit_>
void SettingFieldTimespan<unit_>::readBinary(ReadBuffer & in)
{
    UInt64 num_units = 0;
    readVarUInt(num_units, in);
    *this = num_units;
}

template struct SettingFieldTimespan<SettingFieldTimespanUnit::Second>;
template struct SettingFieldTimespan<SettingFieldTimespanUnit::Millisecond>;


void SettingFieldString::writeBinary(WriteBuffer & out) const
{
    writeStringBinary(value, out);
}

void SettingFieldString::readBinary(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    *this = std::move(str);
}


namespace
{
    char stringToChar(const String & str)
    {
        if (str.size() > 1)
            throw Exception("A setting's value string has to be an exactly one character long", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
        if (str.empty())
            return '\0';
        return str[0];
    }

    char fieldToChar(const Field & f)
    {
        return stringToChar(f.safeGet<const String &>());
    }
}

SettingFieldChar::SettingFieldChar(const Field & f) : SettingFieldChar(fieldToChar(f))
{
}

SettingFieldChar & SettingFieldChar::operator =(const Field & f)
{
    *this = fieldToChar(f);
    return *this;
}

void SettingFieldChar::parseFromString(const String & str)
{
    *this = stringToChar(str);
}

void SettingFieldChar::writeBinary(WriteBuffer & out) const
{
    writeStringBinary(toString(), out);
}

void SettingFieldChar::readBinary(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    *this = stringToChar(str);
}


void SettingFieldURI::writeBinary(WriteBuffer & out) const
{
    writeStringBinary(value.toString(), out);
}

void SettingFieldURI::readBinary(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    *this = Poco::URI{str};
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


String SettingFieldCustom::toString() const
{
    return value.dump();
}

void SettingFieldCustom::parseFromString(const String & str)
{
    *this = Field::restoreFromDump(str);
}

void SettingFieldCustom::writeBinary(WriteBuffer & out) const
{
    writeStringBinary(toString(), out);
}

void SettingFieldCustom::readBinary(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    parseFromString(str);
}

}
