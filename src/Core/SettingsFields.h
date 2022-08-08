#pragma once

#include <Poco/Timespan.h>
#include <Poco/URI.h>
#include <base/types.h>
#include <Core/Field.h>
#include <Core/MultiEnum.h>
#include <boost/range/adaptor/map.hpp>
#include <chrono>
#include <unordered_map>
#include <string_view>


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

template <typename T>
struct SettingFieldNumber
{
    using Type = T;

    Type value;
    bool changed = false;

    explicit SettingFieldNumber(Type x = 0) : value(x) {}
    explicit SettingFieldNumber(const Field & f);

    SettingFieldNumber & operator=(Type x) { value = x; changed = true; return *this; }
    SettingFieldNumber & operator=(const Field & f);

    operator Type() const { return value; } /// NOLINT
    explicit operator Field() const { return value; }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

using SettingFieldUInt64 = SettingFieldNumber<UInt64>;
using SettingFieldInt64 = SettingFieldNumber<Int64>;
using SettingFieldFloat = SettingFieldNumber<float>;
using SettingFieldBool = SettingFieldNumber<bool>;


/** Like SettingFieldNumber, but also supports the value of 'auto'.
  * Note: 0 and 'auto' are not equal. By default when 'auto' is set the value is set to 0.
  * But if you need to distinguish between 0 and 'auto', you can use the 'is_auto' flag.
  * Serialized as string.
  */
template <typename T>
struct SettingFieldNumberWithAuto
{
    using Type = T;

    bool is_auto;
    T value;
    bool changed = false;

    explicit SettingFieldNumberWithAuto() : is_auto(true), value(0)  {}
    explicit SettingFieldNumberWithAuto(T x) : is_auto(false), value(x)  {}
    explicit SettingFieldNumberWithAuto(const Field & f);

    SettingFieldNumberWithAuto & operator=(T x) { is_auto = false; value = x; changed = true; return *this; }
    SettingFieldNumberWithAuto & operator=(const Field & f);

    operator T() const { return value; } /// NOLINT
    explicit operator Field() const { return is_auto ? Field("auto") : Field(value); }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

    T valueOr(T default_value) const { return is_auto ? default_value : value; }
};

using SettingFieldUInt64WithAuto = SettingFieldNumberWithAuto<UInt64>;
using SettingFieldInt64WithAuto = SettingFieldNumberWithAuto<Int64>;

/* Similar to SettingFieldNumberWithAuto with small differences to behave like regular UInt64, supported to compatibility.
 * When setting to 'auto' it becames equal to  the number of processor cores without taking into account SMT.
 * A value of 0 is also treated as 'auto', so 'auto' is parsed and serialized in the same way as 0.
 */
struct SettingFieldMaxThreads
{
    bool is_auto;
    UInt64 value;
    bool changed = false;

    explicit SettingFieldMaxThreads(UInt64 x = 0) : is_auto(!x), value(is_auto ? getAuto() : x)  {}
    explicit SettingFieldMaxThreads(const Field & f);

    SettingFieldMaxThreads & operator=(UInt64 x) { is_auto = !x; value = is_auto ? getAuto() : x; changed = true; return *this; }
    SettingFieldMaxThreads & operator=(const Field & f);

    operator UInt64() const { return value; } /// NOLINT
    explicit operator Field() const { return value; }

    /// Writes "auto(<number>)" instead of simple "<number>" if `is_auto==true`.
    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

private:
    static UInt64 getAuto();
};


enum class SettingFieldTimespanUnit { Millisecond, Second };

template <SettingFieldTimespanUnit unit_>
struct SettingFieldTimespan
{
    using Unit = SettingFieldTimespanUnit;
    static constexpr Unit unit = unit_;
    static constexpr UInt64 microseconds_per_unit = (unit == SettingFieldTimespanUnit::Millisecond) ? 1000 : 1000000;
    Poco::Timespan value;
    bool changed = false;

    explicit SettingFieldTimespan(Poco::Timespan x = {}) : value(x) {}

    template <class Rep, class Period = std::ratio<1>>
    explicit SettingFieldTimespan(const std::chrono::duration<Rep, Period> & x)
        : SettingFieldTimespan(Poco::Timespan{static_cast<Poco::Timespan::TimeDiff>(std::chrono::duration_cast<std::chrono::microseconds>(x).count())}) {}

    explicit SettingFieldTimespan(UInt64 x) : SettingFieldTimespan(Poco::Timespan{static_cast<Poco::Timespan::TimeDiff>(x * microseconds_per_unit)}) {}
    explicit SettingFieldTimespan(const Field & f);

    SettingFieldTimespan & operator =(Poco::Timespan x) { value = x; changed = true; return *this; }

    template <class Rep, class Period = std::ratio<1>>
    SettingFieldTimespan & operator =(const std::chrono::duration<Rep, Period> & x) { *this = Poco::Timespan{static_cast<Poco::Timespan::TimeDiff>(std::chrono::duration_cast<std::chrono::microseconds>(x).count())}; return *this; }

    SettingFieldTimespan & operator =(UInt64 x) { *this = Poco::Timespan{static_cast<Poco::Timespan::TimeDiff>(x * microseconds_per_unit)}; return *this; }
    SettingFieldTimespan & operator =(const Field & f);

    operator Poco::Timespan() const { return value; } /// NOLINT

    template <class Rep, class Period = std::ratio<1>>
    operator std::chrono::duration<Rep, Period>() const { return std::chrono::duration_cast<std::chrono::duration<Rep, Period>>(std::chrono::microseconds(value.totalMicroseconds())); } /// NOLINT

    explicit operator UInt64() const { return value.totalMicroseconds() / microseconds_per_unit; }
    explicit operator Field() const;

    Poco::Timespan::TimeDiff totalMicroseconds() const { return value.totalMicroseconds(); }
    Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }
    Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

using SettingFieldSeconds = SettingFieldTimespan<SettingFieldTimespanUnit::Second>;
using SettingFieldMilliseconds = SettingFieldTimespan<SettingFieldTimespanUnit::Millisecond>;


struct SettingFieldString
{
    String value;
    bool changed = false;

    explicit SettingFieldString(std::string_view str = {}) : value(str) {}
    explicit SettingFieldString(const String & str) : SettingFieldString(std::string_view{str}) {}
    explicit SettingFieldString(String && str) : value(std::move(str)) {}
    explicit SettingFieldString(const char * str) : SettingFieldString(std::string_view{str}) {}
    explicit SettingFieldString(const Field & f) : SettingFieldString(f.safeGet<const String &>()) {}

    SettingFieldString & operator =(std::string_view str) { value = str; changed = true; return *this; }
    SettingFieldString & operator =(const String & str) { *this = std::string_view{str}; return *this; }
    SettingFieldString & operator =(String && str) { value = std::move(str); changed = true; return *this; }
    SettingFieldString & operator =(const char * str) { *this = std::string_view{str}; return *this; }
    SettingFieldString & operator =(const Field & f) { *this = f.safeGet<const String &>(); return *this; }

    operator const String &() const { return value; } /// NOLINT
    explicit operator Field() const { return value; }

    const String & toString() const { return value; }
    void parseFromString(const String & str) { *this = str; }

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

#ifndef KEEPER_STANDALONE_BUILD

struct SettingFieldMap
{
public:
    Map value;
    bool changed = false;

    explicit SettingFieldMap(const Map & map = {}) : value(map) {}
    explicit SettingFieldMap(Map && map) : value(std::move(map)) {}
    explicit SettingFieldMap(const Field & f);

    SettingFieldMap & operator =(const Map & map) { value = map; changed = true; return *this; }
    SettingFieldMap & operator =(const Field & f);

    operator const Map &() const { return value; } /// NOLINT
    explicit operator Field() const { return value; }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

#endif

struct SettingFieldChar
{
public:
    char value;
    bool changed = false;

    explicit SettingFieldChar(char c = '\0') : value(c) {}
    explicit SettingFieldChar(const Field & f);

    SettingFieldChar & operator =(char c) { value = c; changed = true; return *this; }
    SettingFieldChar & operator =(const Field & f);

    operator char() const { return value; } /// NOLINT
    explicit operator Field() const { return toString(); }

    String toString() const { return String(&value, 1); }
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};


struct SettingFieldURI
{
    Poco::URI value;
    bool changed = false;

    explicit SettingFieldURI(const Poco::URI & uri = {}) : value(uri) {}
    explicit SettingFieldURI(const String & str) : SettingFieldURI(Poco::URI{str}) {}
    explicit SettingFieldURI(const char * str) : SettingFieldURI(Poco::URI{str}) {}
    explicit SettingFieldURI(const Field & f) : SettingFieldURI(f.safeGet<String>()) {}

    SettingFieldURI & operator =(const Poco::URI & x) { value = x; changed = true; return *this; }
    SettingFieldURI & operator =(const String & str) { *this = Poco::URI{str}; return *this; }
    SettingFieldURI & operator =(const char * str) { *this = Poco::URI{str}; return *this; }
    SettingFieldURI & operator =(const Field & f) { *this = f.safeGet<const String &>(); return *this; }

    operator const Poco::URI &() const { return value; } /// NOLINT
    explicit operator String() const { return toString(); }
    explicit operator Field() const { return toString(); }

    String toString() const { return value.toString(); }
    void parseFromString(const String & str) { *this = str; }

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
template <typename EnumT, typename Traits>
struct SettingFieldEnum
{
    using EnumType = EnumT;

    EnumType value;
    bool changed = false;

    explicit SettingFieldEnum(EnumType x = EnumType{0}) : value(x) {}
    explicit SettingFieldEnum(const Field & f) : SettingFieldEnum(Traits::fromString(f.safeGet<const String &>())) {}

    SettingFieldEnum & operator =(EnumType x) { value = x; changed = true; return *this; }
    SettingFieldEnum & operator =(const Field & f) { *this = Traits::fromString(f.safeGet<const String &>()); return *this; }

    operator EnumType() const { return value; } /// NOLINT
    explicit operator Field() const { return toString(); }

    String toString() const { return Traits::toString(value); }
    void parseFromString(const String & str) { *this = Traits::fromString(str); }

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

struct SettingFieldEnumHelpers
{
    static void writeBinary(std::string_view str, WriteBuffer & out);
    static String readBinary(ReadBuffer & in);
};

template <typename EnumT, typename Traits>
void SettingFieldEnum<EnumT, Traits>::writeBinary(WriteBuffer & out) const
{
    SettingFieldEnumHelpers::writeBinary(toString(), out);
}

template <typename EnumT, typename Traits>
void SettingFieldEnum<EnumT, Traits>::readBinary(ReadBuffer & in)
{
    *this = Traits::fromString(SettingFieldEnumHelpers::readBinary(in));
}

/// NOLINTNEXTLINE
#define DECLARE_SETTING_ENUM(ENUM_TYPE) \
    DECLARE_SETTING_ENUM_WITH_RENAME(ENUM_TYPE, ENUM_TYPE)

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_ENUM(ENUM_TYPE, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    IMPLEMENT_SETTING_ENUM_WITH_RENAME(ENUM_TYPE, ERROR_CODE_FOR_UNEXPECTED_NAME, __VA_ARGS__)

/// NOLINTNEXTLINE
#define DECLARE_SETTING_ENUM_WITH_RENAME(NEW_NAME, ENUM_TYPE) \
    struct SettingField##NEW_NAME##Traits \
    { \
        using EnumType = ENUM_TYPE; \
        static const String & toString(EnumType value); \
        static EnumType fromString(std::string_view str); \
    }; \
    \
    using SettingField##NEW_NAME = SettingFieldEnum<ENUM_TYPE, SettingField##NEW_NAME##Traits>;

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_ENUM_WITH_RENAME(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    const String & SettingField##NEW_NAME##Traits::toString(typename SettingField##NEW_NAME::EnumType value) \
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
    typename SettingField##NEW_NAME::EnumType SettingField##NEW_NAME##Traits::fromString(std::string_view str) \
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

// Mostly like SettingFieldEnum, but can have multiple enum values (or none) set at once.
template <typename Enum, typename Traits>
struct SettingFieldMultiEnum
{
    using EnumType = Enum;
    using ValueType = MultiEnum<Enum>;
    using StorageType = typename ValueType::StorageType;

    ValueType value;
    bool changed = false;

    explicit SettingFieldMultiEnum(ValueType v = ValueType{}) : value{v} {}
    explicit SettingFieldMultiEnum(EnumType e) : value{e} {}
    explicit SettingFieldMultiEnum(StorageType s) : value(s) {}
    explicit SettingFieldMultiEnum(const Field & f) : value(parseValueFromString(f.safeGet<const String &>())) {}

    operator ValueType() const { return value; } /// NOLINT
    explicit operator StorageType() const { return value.getValue(); }
    explicit operator Field() const { return toString(); }

    SettingFieldMultiEnum & operator= (StorageType x) { changed = x != value.getValue(); value.setValue(x); return *this; }
    SettingFieldMultiEnum & operator= (ValueType x) { changed = !(x == value); value = x; return *this; }
    SettingFieldMultiEnum & operator= (const Field & x) { parseFromString(x.safeGet<const String &>()); return *this; }

    String toString() const
    {
        static const String separator = ",";
        String result;
        for (StorageType i = 0; i < Traits::getEnumSize(); ++i)
        {
            const auto v = static_cast<Enum>(i);
            if (value.isSet(v))
            {
                result += Traits::toString(v);
                result += separator;
            }
        }

        if (!result.empty())
            result.erase(result.size() - separator.size());

        return result;
    }
    void parseFromString(const String & str) { *this = parseValueFromString(str); }

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

private:
    static ValueType parseValueFromString(const std::string_view str)
    {
        static const String separators=", ";

        ValueType result;

        //to avoid allocating memory on substr()
        const std::string_view str_view{str};

        auto value_start = str_view.find_first_not_of(separators);
        while (value_start != std::string::npos)
        {
            auto value_end = str_view.find_first_of(separators, value_start + 1);
            if (value_end == std::string::npos)
                value_end = str_view.size();

            result.set(Traits::fromString(str_view.substr(value_start, value_end - value_start)));
            value_start = str_view.find_first_not_of(separators, value_end);
        }

        return result;
    }
};

template <typename EnumT, typename Traits>
void SettingFieldMultiEnum<EnumT, Traits>::writeBinary(WriteBuffer & out) const
{
    SettingFieldEnumHelpers::writeBinary(toString(), out);
}

template <typename EnumT, typename Traits>
void SettingFieldMultiEnum<EnumT, Traits>::readBinary(ReadBuffer & in)
{
    parseFromString(SettingFieldEnumHelpers::readBinary(in));
}

/// NOLINTNEXTLINE
#define DECLARE_SETTING_MULTI_ENUM(ENUM_TYPE) \
    DECLARE_SETTING_MULTI_ENUM_WITH_RENAME(ENUM_TYPE, ENUM_TYPE)

/// NOLINTNEXTLINE
#define DECLARE_SETTING_MULTI_ENUM_WITH_RENAME(ENUM_TYPE, NEW_NAME) \
    struct SettingField##NEW_NAME##Traits \
    { \
        using EnumType = ENUM_TYPE; \
        static size_t getEnumSize(); \
        static const String & toString(EnumType value); \
        static EnumType fromString(std::string_view str); \
    }; \
    \
    using SettingField##NEW_NAME = SettingFieldMultiEnum<ENUM_TYPE, SettingField##NEW_NAME##Traits>;

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_MULTI_ENUM(ENUM_TYPE, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    IMPLEMENT_SETTING_MULTI_ENUM_WITH_RENAME(ENUM_TYPE, ERROR_CODE_FOR_UNEXPECTED_NAME, __VA_ARGS__)

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_MULTI_ENUM_WITH_RENAME(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    IMPLEMENT_SETTING_ENUM_WITH_RENAME(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, __VA_ARGS__)\
    size_t SettingField##NEW_NAME##Traits::getEnumSize() {\
        return std::initializer_list<std::pair<const char*, NEW_NAME>> __VA_ARGS__ .size();\
    }

/// Can keep a value of any type. Used for user-defined settings.
struct SettingFieldCustom
{
    Field value;
    bool changed = false;

    explicit SettingFieldCustom(const Field & f = {}) : value(f) {}
    SettingFieldCustom & operator =(const Field & f) { value = f; changed = true; return *this; }
    explicit operator Field() const { return value; }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

}
