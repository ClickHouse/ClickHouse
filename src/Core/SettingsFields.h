#pragma once

#include <Poco/Timespan.h>
#include <Poco/URI.h>
#include <Core/Types.h>
#include <Core/Field.h>
#include <boost/range/adaptor/map.hpp>
#include <chrono>
#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class ReadBuffer;
class WriteBuffer;


/** One setting for any type.
  * Stores a value within itself, as well as a flag - whether the value was changed.
  * This is done so that you can send to the remote servers only changed settings (or explicitly specified in the config) values.
  * That is, if the configuration was not specified in the config and was not dynamically changed, it is not sent to the remote server,
  *  and the remote server will use its default value.
  */

template <typename Type>
struct SettingFieldNumber
{
    Type value;
    bool changed = false;

    SettingFieldNumber(Type x = 0) : value(x) {}

    operator Type() const { return value; }
    SettingFieldNumber & operator=(Type x) { set(x); return *this; }

    /// Serialize to a test string.
    String toString() const;

    /// Converts to a field.
    Field toField() const;

    void set(Type x);

    /// Read from SQL literal.
    void set(const Field & x);

    /// Read from text string.
    void set(const String & x);

    /// Serialize to binary stream.
    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

using SettingFieldUInt64 = SettingFieldNumber<UInt64>;
using SettingFieldInt64 = SettingFieldNumber<Int64>;
using SettingFieldFloat = SettingFieldNumber<float>;
using SettingFieldBool = SettingFieldNumber<bool>;


/** Unlike SettingFieldUInt64, supports the value of 'auto' - the number of processor cores without taking into account SMT.
  * A value of 0 is also treated as auto.
  * When serializing, `auto` is written in the same way as 0.
  */
struct SettingFieldMaxThreads
{
    UInt64 value;
    bool is_auto;
    bool changed = false;

    SettingFieldMaxThreads(UInt64 x = 0) : value(x ? x : getAutoValue()), is_auto(x == 0) {}

    operator UInt64() const { return value; }
    SettingFieldMaxThreads & operator=(UInt64 x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);

    void setAuto();
    static UInt64 getAutoValue();

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};


enum class SettingFieldTimespanUnit { MILLISECOND, SECOND };

template <SettingFieldTimespanUnit unit>
struct SettingFieldTimespan
{
    static constexpr UInt64 microseconds_per_unit = (unit == SettingFieldTimespanUnit::MILLISECOND) ? 1000 : 1000000;

    Poco::Timespan value;
    bool changed = false;

    SettingFieldTimespan(UInt64 x = 0) : value(x * microseconds_per_unit) {}

    operator Poco::Timespan() const { return value; }
    SettingFieldTimespan & operator=(const Poco::Timespan & x) { set(x); return *this; }

    template <class Rep, class Period = std::ratio<1>>
    operator std::chrono::duration<Rep, Period>() const { return std::chrono::duration_cast<std::chrono::duration<Rep, Period>>(std::chrono::microseconds(value.totalMicroseconds())); }

    template <class Rep, class Period = std::ratio<1>>
    SettingFieldTimespan & operator=(const std::chrono::duration<Rep, Period> & x) { set(x); return *this; }

    Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }
    Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }

    String toString() const;
    Field toField() const;

    void set(const Poco::Timespan & x);

    template <class Rep, class Period = std::ratio<1>>
    void set(const std::chrono::duration<Rep, Period> & duration) { set(static_cast<UInt64>(std::chrono::duration_cast<std::chrono::microseconds>(duration).count())); }

    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

using SettingFieldSeconds = SettingFieldTimespan<SettingFieldTimespanUnit::SECOND>;
using SettingFieldMilliseconds = SettingFieldTimespan<SettingFieldTimespanUnit::MILLISECOND>;


struct SettingFieldString
{
    String value;
    bool changed = false;

    SettingFieldString(const String & x = String{}) : value(x) {}

    operator String() const { return value; }
    SettingFieldString & operator=(const String & x) { set(x); return *this; }

    const String & toString() const { return value; }
    Field toField() const;

    void set(const String & x);
    void set(const Field & x);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};


struct SettingFieldChar
{
public:
    char value;
    bool changed = false;
    SettingFieldChar(char x = '\0') : value(x) {}

    operator char() const { return value; }
    SettingFieldChar & operator=(char x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(char x);
    void set(const String & x);
    void set(const Field & x);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};


struct SettingFieldURI
{
    Poco::URI value;
    bool changed = false;

    SettingFieldURI(const Poco::URI & x = Poco::URI{}) : value(x) {}

    operator Poco::URI() const { return value; }
    SettingFieldURI & operator=(const Poco::URI & x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(const Poco::URI & x);
    void set(const Field & x);
    void set(const String & x);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};


/** Template class to define enum-based settings.
  * Example of usage:
  *
  * mysettings.h:
  * enum Gender { Male, Female };
  * DECLARE_SETTING_ENUM(SettingFieldGender, Gender)
  *
  * mysettings.cpp:
  * IMPLEMENT_SETTING_ENUM(SettingFieldGender, ErrorCodes::BAD_ARGUMENTS,
  *                        {{"Male", Gender::Male}, {"Female", Gender::Female}})
  */
template <typename EnumType, typename NameValueConverter>
struct SettingFieldEnum
{
    EnumType value;
    bool changed = false;

    SettingFieldEnum(EnumType x) : value(x) {}

    operator EnumType() const { return value; }
    SettingFieldEnum & operator=(EnumType x) { set(x); return *this; }

    const String & toString() const { return NameValueConverter::toString(value); }
    Field toField() const { return toString(); }

    void set(EnumType x) { value = x; changed = true; }
    void set(const Field & x) { set(safeGet<const String &>(x)); }
    void set(const String & x) { set(NameValueConverter::fromString(x)); }

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

struct SettingFieldEnumHelpers
{
    static void writeBinary(const std::string_view & str, WriteBuffer & out);
    static String readBinary(ReadBuffer & in);
};

template <typename EnumType, typename NameValueConverter>
inline void SettingFieldEnum<EnumType, NameValueConverter>::writeBinary(WriteBuffer & out) const
{
    SettingFieldEnumHelpers::writeBinary(toString(), out);
}

template <typename EnumType, typename NameValueConverter>
inline void SettingFieldEnum<EnumType, NameValueConverter>::readBinary(ReadBuffer & in)
{
    set(SettingFieldEnumHelpers::readBinary(in));
}

#define DECLARE_SETTING_ENUM(ENUM_TYPE) \
    DECLARE_SETTING_ENUM_WITH_RENAME(ENUM_TYPE, ENUM_TYPE)

#define IMPLEMENT_SETTING_ENUM(ENUM_TYPE, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    IMPLEMENT_SETTING_ENUM_WITH_RENAME(ENUM_TYPE, ERROR_CODE_FOR_UNEXPECTED_NAME, __VA_ARGS__)

#define DECLARE_SETTING_ENUM_WITH_RENAME(NEW_NAME, ENUM_TYPE) \
    struct SettingField##NEW_NAME##NameValueConverter \
    { \
        using EnumType = ENUM_TYPE; \
        static const String & toString(EnumType value); \
        static EnumType fromString(const std::string_view & str); \
    }; \
    \
    using SettingField##NEW_NAME = SettingFieldEnum<ENUM_TYPE, SettingField##NEW_NAME##NameValueConverter>;

#define IMPLEMENT_SETTING_ENUM_WITH_RENAME(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    const String & SettingField##NEW_NAME##NameValueConverter::toString(typename SettingField##NEW_NAME##NameValueConverter::EnumType value) \
    { \
        static const std::unordered_map<EnumType, String> map = [] { \
            std::unordered_map<EnumType, String> res; \
            constexpr std::pair<const char *, EnumType> pairs[] = __VA_ARGS__; \
            for (const auto & [name, val] : pairs) \
                res.emplace(val, name); \
            return res; \
        }(); \
        auto it = map.find(value); \
        if (it != map.end()) \
            return it->second; \
        throw Exception( \
            "Unexpected value of " #NEW_NAME ":" + std::to_string(std::underlying_type<EnumType>::type(value)), \
            ERROR_CODE_FOR_UNEXPECTED_NAME); \
    } \
    \
    typename SettingField##NEW_NAME##NameValueConverter::EnumType SettingField##NEW_NAME##NameValueConverter::fromString(const std::string_view & str) \
    { \
        static const std::unordered_map<std::string_view, EnumType> map = [] { \
            std::unordered_map<std::string_view, EnumType> res; \
            constexpr std::pair<const char *, EnumType> pairs[] = __VA_ARGS__; \
            for (const auto & [name, val] : pairs) \
                res.emplace(name, val); \
            return res; \
        }(); \
        auto it = map.find(str); \
        if (it != map.end()) \
            return it->second; \
        String msg = "Unexpected value of " #NEW_NAME ": '" + String{str} + "'. Must be one of ["; \
        bool need_comma = false; \
        for (auto & name : map | boost::adaptors::map_keys) \
        { \
            if (std::exchange(need_comma, true)) \
                msg += ", "; \
            msg += "'" + String{name} + "'"; \
        } \
        msg += "]"; \
        throw Exception(msg, ERROR_CODE_FOR_UNEXPECTED_NAME); \
    }
}
