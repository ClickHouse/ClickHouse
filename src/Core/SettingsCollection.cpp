#include <Core/SettingsCollection.h>
#include <Core/SettingsCollectionImpl.h>

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
    extern const int UNKNOWN_LOAD_BALANCING;
    extern const int UNKNOWN_OVERFLOW_MODE;
    extern const int UNKNOWN_TOTALS_MODE;
    extern const int UNKNOWN_DISTRIBUTED_PRODUCT_MODE;
    extern const int UNKNOWN_JOIN;
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_SETTING;
    extern const int CANNOT_PARSE_BOOL;
}


template <typename Type>
String SettingNumber<Type>::toString() const
{
    return DB::toString(value);
}

template <typename Type>
Field SettingNumber<Type>::toField() const
{
    return value;
}

template <typename Type>
void SettingNumber<Type>::set(Type x)
{
    value = x;
    changed = true;
}

template <typename Type>
void SettingNumber<Type>::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(applyVisitor(FieldVisitorConvertToNumber<Type>(), x));
}

template <typename Type>
void SettingNumber<Type>::set(const String & x)
{
    set(parseWithSizeSuffix<Type>(x));
}

template <>
void SettingNumber<bool>::set(const String & x)
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
void SettingNumber<Type>::serialize(WriteBuffer & buf, SettingsBinaryFormat format) const
{
    if (format >= SettingsBinaryFormat::STRINGS)
    {
         writeStringBinary(toString(), buf);
         return;
    }

    if constexpr (is_integral_v<Type> && is_unsigned_v<Type>)
        writeVarUInt(static_cast<UInt64>(value), buf);
    else if constexpr (is_integral_v<Type> && is_signed_v<Type>)
        writeVarInt(static_cast<Int64>(value), buf);
    else
    {
        static_assert(std::is_floating_point_v<Type>);
        writeStringBinary(toString(), buf);
    }
}

template <typename Type>
void SettingNumber<Type>::deserialize(ReadBuffer & buf, SettingsBinaryFormat format)
{
    if (format >= SettingsBinaryFormat::STRINGS)
    {
        String x;
        readStringBinary(x, buf);
        set(x);
        return;
    }

    if constexpr (is_integral_v<Type> && is_unsigned_v<Type>)
    {
        UInt64 x;
        readVarUInt(x, buf);
        set(static_cast<Type>(x));
    }
    else if constexpr (is_integral_v<Type> && is_signed_v<Type>)
    {
        Int64 x;
        readVarInt(x, buf);
        set(static_cast<Type>(x));
    }
    else
    {
        static_assert(std::is_floating_point_v<Type>);
        String x;
        readStringBinary(x, buf);
        set(x);
    }
}

template struct SettingNumber<UInt64>;
template struct SettingNumber<Int64>;
template struct SettingNumber<float>;
template struct SettingNumber<bool>;


String SettingMaxThreads::toString() const
{
    /// Instead of the `auto` value, we output the actual value to make it easier to see.
    return is_auto ? ("auto(" + DB::toString(value) + ")") : DB::toString(value);
}

Field SettingMaxThreads::toField() const
{
    return is_auto ? 0 : value;
}

void SettingMaxThreads::set(UInt64 x)
{
    value = x ? x : getAutoValue();
    is_auto = x == 0;
    changed = true;
}

void SettingMaxThreads::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), x));
}

void SettingMaxThreads::set(const String & x)
{
    if (startsWith(x, "auto"))
        setAuto();
    else
        set(parse<UInt64>(x));
}

void SettingMaxThreads::serialize(WriteBuffer & buf, SettingsBinaryFormat format) const
{
    if (format >= SettingsBinaryFormat::STRINGS)
    {
        writeStringBinary(is_auto ? "auto" : DB::toString(value), buf);
        return;
    }

    writeVarUInt(is_auto ? 0 : value, buf);
}

void SettingMaxThreads::deserialize(ReadBuffer & buf, SettingsBinaryFormat format)
{
    if (format >= SettingsBinaryFormat::STRINGS)
    {
        String x;
        readStringBinary(x, buf);
        set(x);
        return;
    }

    UInt64 x = 0;
    readVarUInt(x, buf);
    set(x);
}

void SettingMaxThreads::setAuto()
{
    value = getAutoValue();
    is_auto = true;
}

UInt64 SettingMaxThreads::getAutoValue()
{
    static auto res = getNumberOfPhysicalCPUCores();
    return res;
}


template <SettingTimespanIO io_unit>
String SettingTimespan<io_unit>::toString() const
{
    return DB::toString(value.totalMicroseconds() / microseconds_per_io_unit);
}

template <SettingTimespanIO io_unit>
Field SettingTimespan<io_unit>::toField() const
{
    return value.totalMicroseconds() / microseconds_per_io_unit;
}

template <SettingTimespanIO io_unit>
void SettingTimespan<io_unit>::set(const Poco::Timespan & x)
{
    value = x;
    changed = true;
}

template <SettingTimespanIO io_unit>
void SettingTimespan<io_unit>::set(UInt64 x)
{
    set(Poco::Timespan(x * microseconds_per_io_unit));
}

template <SettingTimespanIO io_unit>
void SettingTimespan<io_unit>::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), x));
}

template <SettingTimespanIO io_unit>
void SettingTimespan<io_unit>::set(const String & x)
{
    set(parse<UInt64>(x));
}

template <SettingTimespanIO io_unit>
void SettingTimespan<io_unit>::serialize(WriteBuffer & buf, SettingsBinaryFormat format) const
{
    if (format >= SettingsBinaryFormat::STRINGS)
    {
        writeStringBinary(toString(), buf);
        return;
    }

    writeVarUInt(value.totalMicroseconds() / microseconds_per_io_unit, buf);
}

template <SettingTimespanIO io_unit>
void SettingTimespan<io_unit>::deserialize(ReadBuffer & buf, SettingsBinaryFormat format)
{
    if (format >= SettingsBinaryFormat::STRINGS)
    {
        String x;
        readStringBinary(x, buf);
        set(x);
        return;
    }

    UInt64 x = 0;
    readVarUInt(x, buf);
    set(x);
}

template struct SettingTimespan<SettingTimespanIO::SECOND>;
template struct SettingTimespan<SettingTimespanIO::MILLISECOND>;


String SettingString::toString() const
{
    return value;
}

Field SettingString::toField() const
{
    return value;
}

void SettingString::set(const String & x)
{
    value = x;
    changed = true;
}

void SettingString::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingString::serialize(WriteBuffer & buf, SettingsBinaryFormat) const
{
    writeStringBinary(value, buf);
}

void SettingString::deserialize(ReadBuffer & buf, SettingsBinaryFormat)
{
    String s;
    readStringBinary(s, buf);
    set(s);
}


String SettingChar::toString() const
{
    return String(1, value);
}

Field SettingChar::toField() const
{
    return toString();
}

void SettingChar::set(char x)
{
    value = x;
    changed = true;
}

void SettingChar::set(const String & x)
{
    if (x.size() > 1)
        throw Exception("A setting's value string has to be an exactly one character long", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
    char c = (x.size() == 1) ? x[0] : '\0';
    set(c);
}

void SettingChar::set(const Field & x)
{
    const String & s = safeGet<const String &>(x);
    set(s);
}

void SettingChar::serialize(WriteBuffer & buf, SettingsBinaryFormat) const
{
    writeStringBinary(toString(), buf);
}

void SettingChar::deserialize(ReadBuffer & buf, SettingsBinaryFormat)
{
    String s;
    readStringBinary(s, buf);
    set(s);
}


template <typename EnumType, typename Tag>
void SettingEnum<EnumType, Tag>::serialize(WriteBuffer & buf, SettingsBinaryFormat) const
{
    writeStringBinary(toString(), buf);
}

template <typename EnumType, typename Tag>
void SettingEnum<EnumType, Tag>::deserialize(ReadBuffer & buf, SettingsBinaryFormat)
{
    String s;
    readStringBinary(s, buf);
    set(s);
}

template <typename EnumType, typename Tag>
Field SettingEnum<EnumType, Tag>::toField() const
{
    return toString();
}

template <typename EnumType, typename Tag>
void SettingEnum<EnumType, Tag>::set(const Field & x)
{
    set(safeGet<const String &>(x));
}


String SettingURI::toString() const
{
    return value.toString();
}

Field SettingURI::toField() const
{
    return value.toString();
}

void SettingURI::set(const Poco::URI & x)
{
    value = x;
    changed = true;
}

void SettingURI::set(const Field & x)
{
    const String & s = safeGet<const String &>(x);
    set(s);
}

void SettingURI::set(const String & x)
{
    set(Poco::URI(x));
}

void SettingURI::serialize(WriteBuffer & buf, SettingsBinaryFormat) const
{
    writeStringBinary(toString(), buf);
}

void SettingURI::deserialize(ReadBuffer & buf, SettingsBinaryFormat)
{
    String s;
    readStringBinary(s, buf);
    set(s);
}


#define IMPLEMENT_SETTING_ENUM(ENUM_NAME, LIST_OF_NAMES_MACRO, ERROR_CODE_FOR_UNEXPECTED_NAME) \
    IMPLEMENT_SETTING_ENUM_WITH_TAG(ENUM_NAME, void, LIST_OF_NAMES_MACRO, ERROR_CODE_FOR_UNEXPECTED_NAME)

#define IMPLEMENT_SETTING_ENUM_WITH_TAG(ENUM_NAME, TAG, LIST_OF_NAMES_MACRO, ERROR_CODE_FOR_UNEXPECTED_NAME) \
    template <> \
    String SettingEnum<ENUM_NAME, TAG>::toString() const \
    { \
        using EnumType = ENUM_NAME; \
        using UnderlyingType = std::underlying_type<EnumType>::type; \
        switch (static_cast<UnderlyingType>(value)) \
        { \
            LIST_OF_NAMES_MACRO(IMPLEMENT_SETTING_ENUM_TO_STRING_HELPER_) \
        } \
        throw Exception("Unknown " #ENUM_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME); \
    } \
    \
    template <> \
    void SettingEnum<ENUM_NAME, TAG>::set(const String & s) \
    { \
        using EnumType = ENUM_NAME; \
        LIST_OF_NAMES_MACRO(IMPLEMENT_SETTING_ENUM_FROM_STRING_HELPER_) \
        \
        String all_io_names; \
        LIST_OF_NAMES_MACRO(IMPLEMENT_SETTING_ENUM_CONCAT_NAMES_HELPER_) \
        throw Exception("Unknown " #ENUM_NAME " : '" + s + "', must be one of " + all_io_names, \
            ERROR_CODE_FOR_UNEXPECTED_NAME); \
    } \
    \
    template struct SettingEnum<ENUM_NAME, TAG>;

#define IMPLEMENT_SETTING_ENUM_TO_STRING_HELPER_(NAME, IO_NAME) \
    case static_cast<UnderlyingType>(EnumType::NAME): return IO_NAME;

#define IMPLEMENT_SETTING_ENUM_FROM_STRING_HELPER_(NAME, IO_NAME) \
    if (s == (IO_NAME)) \
    { \
        set(EnumType::NAME); \
        return; \
    }

#define IMPLEMENT_SETTING_ENUM_CONCAT_NAMES_HELPER_(NAME, IO_NAME) \
    if (!all_io_names.empty()) \
        all_io_names += ", "; \
    all_io_names += String("'") + (IO_NAME) + "'";


#define LOAD_BALANCING_LIST_OF_NAMES(M) \
    M(RANDOM, "random") \
    M(NEAREST_HOSTNAME, "nearest_hostname") \
    M(IN_ORDER, "in_order") \
    M(FIRST_OR_RANDOM, "first_or_random") \
    M(ROUND_ROBIN, "round_robin")
IMPLEMENT_SETTING_ENUM(LoadBalancing, LOAD_BALANCING_LIST_OF_NAMES, ErrorCodes::UNKNOWN_LOAD_BALANCING)


#define SPECIAL_SORT_ALGORITHM_NAMES(M) \
    M(NOT_SPECIFIED, "not_specified") \
    M(OPENCL_BITONIC, "opencl_bitonic")
IMPLEMENT_SETTING_ENUM(SpecialSort, SPECIAL_SORT_ALGORITHM_NAMES, ErrorCodes::UNKNOWN_JOIN)


#define JOIN_STRICTNESS_LIST_OF_NAMES(M) \
    M(Unspecified, "") \
    M(ALL, "ALL") \
    M(ANY, "ANY")
IMPLEMENT_SETTING_ENUM(JoinStrictness, JOIN_STRICTNESS_LIST_OF_NAMES, ErrorCodes::UNKNOWN_JOIN) // NOLINT

#define JOIN_ALGORITHM_NAMES(M) \
    M(AUTO, "auto") \
    M(HASH, "hash") \
    M(PARTIAL_MERGE, "partial_merge") \
    M(PREFER_PARTIAL_MERGE, "prefer_partial_merge")
IMPLEMENT_SETTING_ENUM(JoinAlgorithm, JOIN_ALGORITHM_NAMES, ErrorCodes::UNKNOWN_JOIN)

#define TOTALS_MODE_LIST_OF_NAMES(M) \
    M(BEFORE_HAVING, "before_having") \
    M(AFTER_HAVING_EXCLUSIVE, "after_having_exclusive") \
    M(AFTER_HAVING_INCLUSIVE, "after_having_inclusive") \
    M(AFTER_HAVING_AUTO, "after_having_auto")
IMPLEMENT_SETTING_ENUM(TotalsMode, TOTALS_MODE_LIST_OF_NAMES, ErrorCodes::UNKNOWN_TOTALS_MODE)


#define OVERFLOW_MODE_LIST_OF_NAMES(M) \
    M(THROW, "throw") \
    M(BREAK, "break")
IMPLEMENT_SETTING_ENUM(OverflowMode, OVERFLOW_MODE_LIST_OF_NAMES, ErrorCodes::UNKNOWN_OVERFLOW_MODE)


#define OVERFLOW_MODE_LIST_OF_NAMES_WITH_ANY(M) \
    M(THROW, "throw") \
    M(BREAK, "break") \
    M(ANY, "any")
IMPLEMENT_SETTING_ENUM_WITH_TAG(OverflowMode, SettingOverflowModeGroupByTag, OVERFLOW_MODE_LIST_OF_NAMES_WITH_ANY, ErrorCodes::UNKNOWN_OVERFLOW_MODE)


#define DISTRIBUTED_PRODUCT_MODE_LIST_OF_NAMES(M) \
    M(DENY, "deny") \
    M(LOCAL, "local") \
    M(GLOBAL, "global") \
    M(ALLOW, "allow")
IMPLEMENT_SETTING_ENUM(DistributedProductMode, DISTRIBUTED_PRODUCT_MODE_LIST_OF_NAMES, ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE)


#define DATE_TIME_INPUT_FORMAT_LIST_OF_NAMES(M) \
    M(Basic, "basic") \
    M(BestEffort, "best_effort")
IMPLEMENT_SETTING_ENUM(FormatSettings::DateTimeInputFormat, DATE_TIME_INPUT_FORMAT_LIST_OF_NAMES, ErrorCodes::BAD_ARGUMENTS)


#define LOGS_LEVEL_LIST_OF_NAMES(M) \
    M(none, "none") \
    M(fatal, "fatal") \
    M(error, "error") \
    M(warning, "warning") \
    M(information, "information") \
    M(debug, "debug") \
    M(trace, "trace")
IMPLEMENT_SETTING_ENUM(LogsLevel, LOGS_LEVEL_LIST_OF_NAMES, ErrorCodes::BAD_ARGUMENTS)

#define LOG_QUERIES_TYPE_LIST_OF_NAMES(M) \
    M(QUERY_START, "QUERY_START") \
    M(QUERY_FINISH, "QUERY_FINISH") \
    M(EXCEPTION_BEFORE_START, "EXCEPTION_BEFORE_START") \
    M(EXCEPTION_WHILE_PROCESSING, "EXCEPTION_WHILE_PROCESSING")
IMPLEMENT_SETTING_ENUM(QueryLogElementType, LOG_QUERIES_TYPE_LIST_OF_NAMES, ErrorCodes::BAD_ARGUMENTS)


#define DEFAULT_DATABASE_ENGINE_LIST_OF_NAMES(M) \
    M(Ordinary, "Ordinary") \
    M(Atomic, "Atomic")
IMPLEMENT_SETTING_ENUM(DefaultDatabaseEngine , DEFAULT_DATABASE_ENGINE_LIST_OF_NAMES, ErrorCodes::BAD_ARGUMENTS)


namespace details
{
    void SettingsCollectionUtils::serializeName(const StringRef & name, WriteBuffer & buf)
    {
        writeStringBinary(name, buf);
    }

    String SettingsCollectionUtils::deserializeName(ReadBuffer & buf)
    {
        String name;
        readStringBinary(name, buf);
        return name;
    }

    void SettingsCollectionUtils::serializeFlag(bool flag, WriteBuffer & buf)
    {
        buf.write(flag);
    }

    bool SettingsCollectionUtils::deserializeFlag(ReadBuffer & buf)
    {
        char c;
        buf.readStrict(c);
        return c;
    }

    void SettingsCollectionUtils::skipValue(ReadBuffer & buf)
    {
        /// Ignore a string written by the function writeStringBinary().
        UInt64 size;
        readVarUInt(size, buf);
        buf.ignore(size);
    }

    void SettingsCollectionUtils::warningNameNotFound(const StringRef & name)
    {
        static auto * log = &Poco::Logger::get("Settings");
        LOG_WARNING(log, "Unknown setting {}, skipping", name);
    }

    void SettingsCollectionUtils::throwNameNotFound(const StringRef & name)
    {
        throw Exception("Unknown setting " + name.toString(), ErrorCodes::UNKNOWN_SETTING);
    }
}
}
