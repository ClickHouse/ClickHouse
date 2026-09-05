#pragma once

#include <Core/Field.h>
#include <Core/MultiEnum.h>
#include <base/types.h>
#include <Poco/Timespan.h>
#include <Poco/URI.h>

#include <string_view>

namespace DB
{
class ReadBuffer;
class WriteBuffer;


/** Type-erased operation table for setting fields.
  * One static instance per concrete SettingField type (not per setting).
  * Used by the Accessor in BaseSettings.h for runtime dispatch without virtual methods.
  */
struct SettingFieldOps
{
    void (*assign_from_field)(void * setting, const Field & value);
    Field (*to_field)(const void * setting);
    bool (*is_changed)(const void * setting);
    void (*set_changed)(void * setting, bool);
    String (*to_string)(const void * setting);
    void (*parse_from_string)(void * setting, const String & str);
    void (*write_binary)(const void * setting, WriteBuffer & out);
    void (*read_binary)(void * setting, ReadBuffer & in);
    /// Typed copy, used to reset a setting to its declared default by copying from a canonical
    /// default-constructed Data instance. A typed `operator=` preserves all members of the source
    /// (e.g. `SettingFieldMaxThreads::is_auto`, see issue #103120) that would be lost through a
    /// `Field` round-trip when `operator Field` is non-invertible.
    void (*typed_copy)(void * dst, const void * src);
    /// Type-level utilities (operate on temporaries, no Data instance needed)
    Field (*cast_value)(const Field & value);
    String (*value_to_string)(const Field & value);
    Field (*string_to_value)(const String & str);
};

/// Named template functions for type-erased dispatch.
/// Using named functions instead of lambdas avoids creating unique closure types per instantiation,
/// which reduces compilation overhead.
template <typename T> void settingFieldAssignFromField(void * p, const Field & f) { *static_cast<T *>(p) = f; }
template <typename T> Field settingFieldToField(const void * p) { return static_cast<Field>(*static_cast<const T *>(p)); }
template <typename T> bool settingFieldIsChanged(const void * p) { return static_cast<const T *>(p)->isChanged(); }
template <typename T> void settingFieldSetChanged(void * p, bool v) { static_cast<T *>(p)->setChanged(v); }
template <typename T> String settingFieldToString(const void * p) { return static_cast<const T *>(p)->toString(); }
template <typename T> void settingFieldParseFromString(void * p, const String & s) { static_cast<T *>(p)->parseFromString(s); }
template <typename T> void settingFieldWriteBinary(const void * p, WriteBuffer & out) { static_cast<const T *>(p)->writeBinary(out); }
template <typename T> void settingFieldReadBinary(void * p, ReadBuffer & in) { static_cast<T *>(p)->readBinary(in); }
template <typename T> void settingFieldTypedCopy(void * dst, const void * src) { *static_cast<T *>(dst) = *static_cast<const T *>(src); }
template <typename T> Field settingFieldCastValue(const Field & f) { T temp; temp = f; return static_cast<Field>(temp); }
template <typename T> String settingFieldValueToString(const Field & f) { T temp; temp = f; return temp.toString(); }
template <typename T> Field settingFieldStringToValue(const String & s) { T temp; temp.parseFromString(s); return static_cast<Field>(temp); }

/// Generate a SettingFieldOps instance for a concrete SettingField type.
/// Instantiated once per type (not per setting) — the static local is shared.
template <typename T>
const SettingFieldOps & settingFieldOps()
{
    static const SettingFieldOps ops =
    {
        &settingFieldAssignFromField<T>, &settingFieldToField<T>, &settingFieldIsChanged<T>, &settingFieldSetChanged<T>,
        &settingFieldToString<T>, &settingFieldParseFromString<T>, &settingFieldWriteBinary<T>, &settingFieldReadBinary<T>,
        &settingFieldTypedCopy<T>,
        &settingFieldCastValue<T>, &settingFieldValueToString<T>, &settingFieldStringToValue<T>,
    };
    return ops;
}

/** One setting for any type.
  * Stores a value within itself, as well as a flag - whether the value was changed.
  * This is done so that you can send to the remote servers only changed settings (or explicitly specified in the config) values.
  * That is, if the configuration was not specified in the config and was not dynamically changed, it is not sent to the remote server,
  *  and the remote server will use its default value.
  *
  * Empty base class — concrete types provide their own methods with matching signatures.
  * Type-erased dispatch is done through SettingFieldOps function pointers, not virtual methods.
  */
template <typename T>
struct SettingFieldNumber
{
    using Type = T;
    using ValueType = T;

    Type value;
    bool changed = false;

    explicit SettingFieldNumber(Type x = 0);
    explicit SettingFieldNumber(const Field & f);
    SettingFieldNumber(const SettingFieldNumber &) = default;
    SettingFieldNumber & operator=(const SettingFieldNumber &) = default;

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    SettingFieldNumber & operator=(Type x);
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
using SettingFieldUInt32 = SettingFieldNumber<UInt32>;
using SettingFieldInt32 = SettingFieldNumber<Int32>;
using SettingFieldFloat = SettingFieldNumber<float>;
using SettingFieldDouble = SettingFieldNumber<double>;
using SettingFieldBool = SettingFieldNumber<bool>;

/** Wraps any SettingField to support special value 'auto' that can be checked with `is_auto` flag.
  * Note about serialization:
  * The new versions with `SettingsWriteFormat::STRINGS_WITH_FLAGS` serialize values as a string.
  * In legacy SettingsWriteFormat mode, functions `read/writeBinary` would serialize values as a binary, and 'is_auto' would be ignored.
  * It's possible to upgrade settings from regular type to wrapped ones and keep compatibility with old versions,
  * but when serializing 'auto' old version will see binary representation of the default value.
  */
template <typename Base>
struct SettingAutoWrapper final
{
    constexpr static auto keyword = "auto";
    static bool isAuto(const Field & f) { return f.getType() == Field::Types::String && f.safeGet<String>() == keyword; }
    static bool isAuto(const String & str) { return str == keyword; }

    using Type = Base::Type;

    Base base;
    bool is_auto = false;
    bool changed = false;

    explicit SettingAutoWrapper() : is_auto(true) {}
    explicit SettingAutoWrapper(Type val) : is_auto(false) { base = Base(val); }
    SettingAutoWrapper(const SettingAutoWrapper &) = default;
    SettingAutoWrapper & operator=(const SettingAutoWrapper &) = default;

    explicit SettingAutoWrapper(const Field & f)
        : is_auto(isAuto(f))
    {
        if (!is_auto)
            base = Base(f);
    }

    SettingAutoWrapper & operator=(const Field & f)
    {
        changed = true;
        if (is_auto = isAuto(f); !is_auto)
            base = f;
        return *this;
    }

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    explicit operator Field() const { return is_auto ? Field(keyword) : Field(base); }

    String toString() const { return is_auto ? keyword : base.toString(); }

    void parseFromString(const String & str)
    {
        changed = true;
        if (is_auto = isAuto(str); !is_auto)
            base.parseFromString(str);
    }

    void writeBinary(WriteBuffer & out) const
    {
        if (is_auto)
            Base().writeBinary(out); /// serialize default value
        else
            base.writeBinary(out);
    }

    /*
     * That it is fine to reset `is_auto` here and to use default value in case `is_auto`
     * because settings will be serialized only if changed.
     * If they were changed they were requested to use explicit value instead of `auto`.
     * And so interactions between client-server, and server-server (distributed queries), should be OK.
     */
    void readBinary(ReadBuffer & in) { changed = true; is_auto = false; base.readBinary(in); }

    Type valueOr(Type default_value) const { return is_auto ? default_value : base.value; }
    std::optional<Type> valueOrNullopt() const { return is_auto ? std::optional<Type>(std::nullopt) : base.value; }
};

using SettingFieldBoolAuto = SettingAutoWrapper<SettingFieldBool>;
using SettingFieldUInt64Auto = SettingAutoWrapper<SettingFieldUInt64>;
using SettingFieldInt64Auto = SettingAutoWrapper<SettingFieldInt64>;
using SettingFieldFloatAuto = SettingAutoWrapper<SettingFieldFloat>;
using SettingFieldUInt32Auto = SettingAutoWrapper<SettingFieldUInt32>;
using SettingFieldInt32Auto = SettingAutoWrapper<SettingFieldInt32>;
using SettingFieldDoubleAuto = SettingAutoWrapper<SettingFieldDouble>;

/* Similar to SettingFieldUInt64Auto with small differences to behave like regular UInt64, supported to compatibility.
 * When setting to 'auto' it becomes equal to  the number of processor cores without taking into account SMT.
 * A value of 0 is also treated as 'auto', so 'auto' is parsed and serialized in the same way as 0.
 */
struct SettingFieldMaxThreads final
{
    bool is_auto;
    UInt64 value;
    bool changed = false;

    using ValueType = UInt64;

    explicit SettingFieldMaxThreads(UInt64 x = 0) : is_auto(!x), value(is_auto ? getAuto() : x)  {}
    explicit SettingFieldMaxThreads(const Field & f);
    SettingFieldMaxThreads(const SettingFieldMaxThreads &) = default;
    SettingFieldMaxThreads & operator=(const SettingFieldMaxThreads &) = default;

    SettingFieldMaxThreads & operator=(UInt64 x) { is_auto = !x; value = is_auto ? getAuto() : x; changed = true; return *this; }
    SettingFieldMaxThreads & operator=(const Field & f);

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator UInt64() const { return value; } /// NOLINT
    explicit operator Field() const { return value; }

    /// Writes "auto(<number>)" instead of simple "<number>" if `is_auto == true`.
    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

private:
    static UInt64 getAuto();
};


enum class SettingFieldTimespanUnit : uint8_t
{
    Millisecond,
    Second
};

template <SettingFieldTimespanUnit unit_>
struct SettingFieldTimespan final
{
    using Unit = SettingFieldTimespanUnit;
    static constexpr Unit unit = unit_;
    static constexpr UInt64 microseconds_per_unit = (unit == SettingFieldTimespanUnit::Millisecond) ? 1000 : 1000000;
    Int64 microseconds = 0;
    bool changed = false;

    explicit SettingFieldTimespan() = default;
    explicit SettingFieldTimespan(const Poco::Timespan & x) : microseconds(x.totalMicroseconds()) {}
    SettingFieldTimespan(const SettingFieldTimespan &) = default;
    SettingFieldTimespan & operator=(const SettingFieldTimespan &) = default;

    explicit SettingFieldTimespan(UInt64 x) : microseconds(static_cast<Int64>(x * microseconds_per_unit)) {}
    explicit SettingFieldTimespan(const Field & f);

    SettingFieldTimespan & operator =(const Poco::Timespan & x) { microseconds = x.totalMicroseconds(); changed = true; return *this; }

    SettingFieldTimespan & operator =(UInt64 x) { microseconds = static_cast<Int64>(x * microseconds_per_unit); changed = true; return *this; }
    SettingFieldTimespan & operator =(const Field & f);

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator Poco::Timespan() const { return Poco::Timespan(microseconds); } /// NOLINT

    explicit operator UInt64() const { return microseconds / static_cast<Int64>(microseconds_per_unit); }
    explicit operator Field() const;

    Int64 totalMicroseconds() const { return microseconds; }
    Int64 totalMilliseconds() const { return microseconds / 1000; }
    Int64 totalSeconds() const { return microseconds / 1000000; }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

using SettingFieldSeconds = SettingFieldTimespan<SettingFieldTimespanUnit::Second>;
using SettingFieldMilliseconds = SettingFieldTimespan<SettingFieldTimespanUnit::Millisecond>;


struct SettingFieldString final
{
    String value;
    bool changed = false;
    using ValueType = String;

    explicit SettingFieldString(std::string_view str = {}) : value(str) {}
    explicit SettingFieldString(const String & str) : SettingFieldString(std::string_view{str}) {}
    explicit SettingFieldString(String && str) : value(std::move(str)) {}
    explicit SettingFieldString(const char * str) : SettingFieldString(std::string_view{str}) {}
    explicit SettingFieldString(const Field & f) : SettingFieldString(f.safeGet<String>()) {}
    SettingFieldString(const SettingFieldString &) = default;
    SettingFieldString & operator=(const SettingFieldString &) = default;

    SettingFieldString & operator =(std::string_view str) { value = str; changed = true; return *this; }
    SettingFieldString & operator =(const String & str) { *this = std::string_view{str}; return *this; }
    SettingFieldString & operator =(String && str) { value = std::move(str); changed = true; return *this; }
    SettingFieldString & operator =(const char * str) { *this = std::string_view{str}; return *this; }
    SettingFieldString & operator =(const Field & f) { *this = f.safeGet<String>(); return *this; }

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator const String &() const { return value; } /// NOLINT
    explicit operator Field() const { return value; }

    String toString() const { return value; }
    void parseFromString(const String & str) { *this = str; }

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

struct SettingFieldMap final
{
public:
    Map value;
    bool changed = false;

    explicit SettingFieldMap(const Map & map = {}) : value(map) {}
    explicit SettingFieldMap(Map && map) : value(std::move(map)) {}
    explicit SettingFieldMap(const Field & f);
    SettingFieldMap(const SettingFieldMap &) = default;
    SettingFieldMap & operator=(const SettingFieldMap &) = default;

    SettingFieldMap & operator =(const Map & map) { value = map; changed = true; return *this; }
    SettingFieldMap & operator =(const Field & f);

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator const Map &() const { return value; } /// NOLINT
    explicit operator Field() const { return value; }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

struct SettingFieldChar final
{
public:
    char value;
    bool changed = false;

    explicit SettingFieldChar(char c = '\0') : value(c) {}
    explicit SettingFieldChar(const Field & f);
    SettingFieldChar(const SettingFieldChar &) = default;
    SettingFieldChar & operator=(const SettingFieldChar &) = default;

    SettingFieldChar & operator =(char c) { value = c; changed = true; return *this; }
    SettingFieldChar & operator =(const Field & f);

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator char() const { return value; } /// NOLINT
    explicit operator Field() const { return toString(); }

    String toString() const { return String(&value, 1); }
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};


struct SettingFieldURI final
{
    Poco::URI value;
    bool changed = false;

    explicit SettingFieldURI(const Poco::URI & uri = {}) : value(uri) {}
    explicit SettingFieldURI(const String & str) : SettingFieldURI(Poco::URI{str}) {}
    explicit SettingFieldURI(const char * str) : SettingFieldURI(Poco::URI{str}) {}
    explicit SettingFieldURI(const Field & f) : SettingFieldURI(f.safeGet<String>()) {}
    SettingFieldURI(const SettingFieldURI &) = default;
    SettingFieldURI & operator=(const SettingFieldURI &) = default;

    SettingFieldURI & operator =(const Poco::URI & x) { value = x; changed = true; return *this; }
    SettingFieldURI & operator =(const String & str) { *this = Poco::URI{str}; return *this; }
    SettingFieldURI & operator =(const char * str) { *this = Poco::URI{str}; return *this; }
    SettingFieldURI & operator =(const Field & f) { *this = f.safeGet<String>(); return *this; }

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

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
  * IMPLEMENT_SETTING_ENUM(SettingFieldGender, ExceptionType,
  *                        {{"Male", Gender::Male}, {"Female", Gender::Female}})
  */
template <typename EnumT, typename Traits>
struct SettingFieldEnum final
{
    using EnumType = EnumT;
    using ValueType = EnumT;

    EnumType value;
    bool changed = false;

    explicit SettingFieldEnum(EnumType x = EnumType{}) : value(x) {}
    explicit SettingFieldEnum(const Field & f) : SettingFieldEnum(Traits::fromString(f.safeGet<String>())) {}
    SettingFieldEnum(const SettingFieldEnum &) = default;
    SettingFieldEnum & operator=(const SettingFieldEnum &) = default;

    SettingFieldEnum & operator =(EnumType x) { value = x; changed = true; return *this; }
    SettingFieldEnum & operator =(const Field & f) { *this = Traits::fromString(f.safeGet<String>()); return *this; }

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

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

// Mostly like SettingFieldEnum, but can have multiple enum values (or none) set at once.
template <typename Enum, typename Traits>
struct SettingFieldMultiEnum final
{
    using EnumType = Enum;
    using ValueType = std::vector<Enum>;

    ValueType value;
    bool changed = false;

    explicit SettingFieldMultiEnum(ValueType v = ValueType{}) : value{v} {}
    explicit SettingFieldMultiEnum(EnumType e) : value{e} {}
    explicit SettingFieldMultiEnum(const Field & f) : value(parseValueFromString(f.safeGet<String>())) {}
    SettingFieldMultiEnum(const SettingFieldMultiEnum &) = default;
    SettingFieldMultiEnum & operator=(const SettingFieldMultiEnum &) = default;

    SettingFieldMultiEnum & operator= (ValueType x) { changed = true; value = x; return *this; }
    SettingFieldMultiEnum & operator= (const Field & x) { parseFromString(x.safeGet<String>()); return *this; }

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator ValueType() const { return value; } /// NOLINT
    explicit operator Field() const { return toString(); }
    operator MultiEnum<EnumType>() const /// NOLINT
    {
        MultiEnum<EnumType> res;
        for (const auto & v : value)
            res.set(v);
        return res;
    }

    String toString() const
    {
        constexpr String separator = ",";
        String result;
        for (const auto & v : value)
        {
            result += Traits::toString(v);
            result += separator;
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
        constexpr String separators=", ";

        ValueType result;
        std::unordered_set<EnumType> values_set;

        //to avoid allocating memory on substr()
        const std::string_view str_view{str};

        auto value_start = str_view.find_first_not_of(separators);
        while (value_start != std::string::npos)
        {
            auto value_end = str_view.find_first_of(separators, value_start + 1);
            if (value_end == std::string::npos)
                value_end = str_view.size();

            auto value = Traits::fromString(str_view.substr(value_start, value_end - value_start));
            /// Deduplicate values
            auto [_, inserted] = values_set.emplace(value);
            if (inserted)
                result.push_back(value);

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

/// Setting field for specifying user-defined timezone. It is basically a string, but it needs validation.
struct SettingFieldTimezone final
{
    String value;
    bool changed = false;

    explicit SettingFieldTimezone(std::string_view str = {}) { validateTimezone(std::string(str)); value = str; }
    explicit SettingFieldTimezone(const String & str) { validateTimezone(str); value = str; }
    explicit SettingFieldTimezone(String && str) { validateTimezone(str); value = std::move(str); }
    explicit SettingFieldTimezone(const char * str) { validateTimezone(str); value = str; }
    explicit SettingFieldTimezone(const Field & f) { const String & str = f.safeGet<String>(); validateTimezone(str); value = str; }
    SettingFieldTimezone(const SettingFieldTimezone &) = default;
    SettingFieldTimezone & operator=(const SettingFieldTimezone &) = default;

    SettingFieldTimezone & operator =(std::string_view str) { validateTimezone(std::string(str)); value = str; changed = true; return *this; }
    SettingFieldTimezone & operator =(const String & str) { *this = std::string_view{str}; return *this; }
    SettingFieldTimezone & operator =(String && str) { validateTimezone(str); value = std::move(str); changed = true; return *this; }
    SettingFieldTimezone & operator =(const char * str) { *this = std::string_view{str}; return *this; }
    SettingFieldTimezone & operator =(const Field & f) { *this = f.safeGet<String>(); return *this; }

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator const String &() const { return value; } /// NOLINT
    explicit operator Field() const { return value; }

    String toString() const { return value; }
    void parseFromString(const String & str) { *this = str; }

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

private:
    void validateTimezone(const std::string & tz_str);
};

/// Can keep a value of any type. Used for user-defined settings.
struct SettingFieldCustom final
{
    Field value;
    bool changed = false;

    explicit SettingFieldCustom(const Field & f = {}) : value(f) {}
    SettingFieldCustom(const SettingFieldCustom &) = default;
    SettingFieldCustom & operator=(const SettingFieldCustom &) = default;

    SettingFieldCustom & operator =(const Field & f) { value = f; changed = true; return *this; }

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    explicit operator Field() const { return value; }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

struct SettingFieldNonZeroUInt64 : public SettingFieldUInt64
{
public:
    explicit SettingFieldNonZeroUInt64(UInt64 x = 1);
    explicit SettingFieldNonZeroUInt64(const Field & f);

    SettingFieldNonZeroUInt64 & operator=(UInt64 x);
    SettingFieldNonZeroUInt64 & operator=(const Field & f);

    void parseFromString(const String & str);

private:
    void checkValueNonZero() const;
};

bool stringToBool(const String & str);

}
