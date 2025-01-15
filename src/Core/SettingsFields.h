#pragma once

#include <chrono>
#include <string_view>
#include <Core/Field.h>
#include <Core/MultiEnum.h>
#include <base/types.h>
#include <Poco/Timespan.h>
#include <Poco/URI.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class ReadBuffer;
class WriteBuffer;

struct SettingFieldBase
{
    bool changed = false;
    SettingFieldBase() { }
    SettingFieldBase(bool changed_) : changed(changed_) { }
    virtual ~SettingFieldBase() = default;

    virtual String toString() const = 0;
    virtual void parseFromString(const String & str) = 0;

    virtual void writeBinary(WriteBuffer & out) const = 0;
    virtual void readBinary(ReadBuffer & in) = 0;

    virtual SettingFieldBase & operator=(const Field & f) = 0;
    virtual explicit operator Field() const = 0;

    virtual std::unique_ptr<SettingFieldBase> clone() const = 0;
};


/** One setting for any type.
  * Stores a value within itself, as well as a flag - whether the value was changed.
  * This is done so that you can send to the remote servers only changed settings (or explicitly specified in the config) values.
  * That is, if the configuration was not specified in the config and was not dynamically changed, it is not sent to the remote server,
  *  and the remote server will use its default value.
  */

template <typename T>
struct SettingFieldNumber : public SettingFieldBase
{
    using Type = T;
    Type value;

    SettingFieldNumber(const SettingFieldNumber & o) : SettingFieldBase(o.changed), value(o.value) { }
    explicit SettingFieldNumber(Type x = 0) : value(x) {}
    explicit SettingFieldNumber(const Field & f);

    SettingFieldNumber & operator=(Type x) { value = x; changed = true; return *this; }
    SettingFieldNumber & operator=(const Field & f) override;
    SettingFieldNumber & operator=(const SettingFieldNumber & o)
    {
        value = o.value;
        changed = o.changed;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldNumber>(*this); }

    operator Type() const { return value; } /// NOLINT
    explicit operator Field() const override { return value; }

    String toString() const override;
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
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
struct SettingAutoWrapper final : public SettingFieldBase
{
    constexpr static auto keyword = "auto";
    static bool isAuto(const Field & f) { return f.getType() == Field::Types::String && f.safeGet<const String &>() == keyword; }
    static bool isAuto(const String & str) { return str == keyword; }

    using Type = typename Base::Type;

    Base base;
    bool is_auto = false;

    explicit SettingAutoWrapper() : is_auto(true) {}
    explicit SettingAutoWrapper(Type val) : is_auto(false) { base = Base(val); }
    SettingAutoWrapper(const SettingAutoWrapper & o) : SettingFieldBase(o.changed), base(o.base), is_auto(o.is_auto) { }

    explicit SettingAutoWrapper(const Field & f)
        : is_auto(isAuto(f))
    {
        if (!is_auto)
            base = Base(f);
    }

    SettingAutoWrapper & operator=(const Field & f) override
    {
        changed = true;
        if (is_auto = isAuto(f); !is_auto)
            base = f;
        return *this;
    }

    SettingAutoWrapper & operator=(const SettingAutoWrapper & o)
    {
        changed = o.changed;
        if (is_auto = o.is_auto; !is_auto)
            base = o.base;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingAutoWrapper>(*this); }

    explicit operator Field() const override { return is_auto ? Field(keyword) : Field(base); }

    String toString() const override { return is_auto ? keyword : base.toString(); }

    void parseFromString(const String & str) override
    {
        changed = true;
        if (is_auto = isAuto(str); !is_auto)
            base.parseFromString(str);
    }

    void writeBinary(WriteBuffer & out) const override
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
    void readBinary(ReadBuffer & in) override
    {
        changed = true;
        is_auto = false;
        base.readBinary(in);
    }

    Type valueOr(Type default_value) const { return is_auto ? default_value : base.value; }
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
struct SettingFieldMaxThreads final : public SettingFieldBase
{
    bool is_auto;
    UInt64 value;

    explicit SettingFieldMaxThreads(UInt64 x = 0) : is_auto(!x), value(is_auto ? getAuto() : x)  {}
    explicit SettingFieldMaxThreads(const Field & f);
    SettingFieldMaxThreads(const SettingFieldMaxThreads & o) : SettingFieldBase(o.changed), is_auto(o.is_auto), value(o.value) { }

    SettingFieldMaxThreads & operator=(UInt64 x) { is_auto = !x; value = is_auto ? getAuto() : x; changed = true; return *this; }
    SettingFieldMaxThreads & operator=(const Field & f) override;
    SettingFieldMaxThreads & operator=(const SettingFieldMaxThreads & o)
    {
        is_auto = o.is_auto;
        value = o.value;
        changed = o.changed;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldMaxThreads>(*this); }

    operator UInt64() const { return value; } /// NOLINT
    explicit operator Field() const override { return value; }

    /// Writes "auto(<number>)" instead of simple "<number>" if `is_auto == true`.
    String toString() const override;
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;

private:
    static UInt64 getAuto();
};


enum class SettingFieldTimespanUnit : uint8_t
{
    Millisecond,
    Second
};

template <SettingFieldTimespanUnit unit_>
struct SettingFieldTimespan final : public SettingFieldBase
{
    using Unit = SettingFieldTimespanUnit;
    static constexpr Unit unit = unit_;
    static constexpr UInt64 microseconds_per_unit = (unit == SettingFieldTimespanUnit::Millisecond) ? 1000 : 1000000;
    Poco::Timespan value;

    SettingFieldTimespan(const SettingFieldTimespan & o) : SettingFieldBase(o.changed), value(o.value) { }

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
    SettingFieldTimespan & operator=(const Field & f) override;
    SettingFieldTimespan & operator=(const SettingFieldTimespan & o)
    {
        changed = o.changed;
        value = o.value;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldTimespan>(*this); }

    operator Poco::Timespan() const { return value; } /// NOLINT

    template <class Rep, class Period = std::ratio<1>>
    operator std::chrono::duration<Rep, Period>() const { return std::chrono::duration_cast<std::chrono::duration<Rep, Period>>(std::chrono::microseconds(value.totalMicroseconds())); } /// NOLINT

    explicit operator UInt64() const { return value.totalMicroseconds() / microseconds_per_unit; }
    explicit operator Field() const override;

    Poco::Timespan::TimeDiff totalMicroseconds() const { return value.totalMicroseconds(); }
    Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }
    Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }

    String toString() const override;
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
};

using SettingFieldSeconds = SettingFieldTimespan<SettingFieldTimespanUnit::Second>;
using SettingFieldMilliseconds = SettingFieldTimespan<SettingFieldTimespanUnit::Millisecond>;


struct SettingFieldString final : public SettingFieldBase
{
    String value;

    explicit SettingFieldString(std::string_view str = {}) : value(str) {}
    explicit SettingFieldString(const String & str) : SettingFieldString(std::string_view{str}) {}
    explicit SettingFieldString(String && str) : value(std::move(str)) {}
    explicit SettingFieldString(const char * str) : SettingFieldString(std::string_view{str}) {}
    explicit SettingFieldString(const Field & f) : SettingFieldString(f.safeGet<const String &>()) {}
    SettingFieldString(const SettingFieldString & o) : SettingFieldBase(o.changed), value(o.value) { }

    SettingFieldString & operator =(std::string_view str) { value = str; changed = true; return *this; }
    SettingFieldString & operator =(const String & str) { *this = std::string_view{str}; return *this; }
    SettingFieldString & operator =(String && str) { value = std::move(str); changed = true; return *this; }
    SettingFieldString & operator =(const char * str) { *this = std::string_view{str}; return *this; }
    SettingFieldString & operator=(const Field & f) override
    {
        *this = f.safeGet<const String &>();
        return *this;
    }
    SettingFieldString & operator=(const SettingFieldString & o)
    {
        value = o.value;
        changed = o.changed;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldString>(*this); }

    operator const String &() const { return value; } /// NOLINT
    explicit operator Field() const override { return value; }

    String toString() const override { return value; }
    void parseFromString(const String & str) override { *this = str; }

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
};

struct SettingFieldMap final : public SettingFieldBase
{
    Map value;

    explicit SettingFieldMap(const Map & map = {}) : value(map) {}
    explicit SettingFieldMap(Map && map) : value(std::move(map)) {}
    explicit SettingFieldMap(const Field & f);
    SettingFieldMap(const SettingFieldMap & o) : SettingFieldBase(o.changed), value(o.value) { }

    SettingFieldMap & operator =(const Map & map) { value = map; changed = true; return *this; }
    SettingFieldMap & operator=(const Field & f) override;
    SettingFieldMap & operator=(const SettingFieldMap & o)
    {
        changed = o.changed;
        value = o.value;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldMap>(*this); }

    operator const Map &() const { return value; } /// NOLINT
    explicit operator Field() const override { return value; }

    String toString() const override;
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
};

struct SettingFieldChar : public SettingFieldBase
{
    char value;

    explicit SettingFieldChar(char c = '\0') : value(c) {}
    explicit SettingFieldChar(const Field & f);
    SettingFieldChar(const SettingFieldChar & o) : SettingFieldBase(o.changed), value(o.value) { }

    SettingFieldChar & operator =(char c) { value = c; changed = true; return *this; }
    SettingFieldChar & operator=(const Field & f) override;
    SettingFieldChar & operator=(const SettingFieldChar & o)
    {
        value = o.value;
        changed = o.changed;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldChar>(*this); }

    operator char() const { return value; } /// NOLINT
    explicit operator Field() const override { return toString(); }

    String toString() const override { return String(&value, 1); }
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
};


struct SettingFieldURI final : public SettingFieldBase
{
    Poco::URI value;

    explicit SettingFieldURI(const Poco::URI & uri = {}) : value(uri) {}
    explicit SettingFieldURI(const String & str) : SettingFieldURI(Poco::URI{str}) {}
    explicit SettingFieldURI(const char * str) : SettingFieldURI(Poco::URI{str}) {}
    explicit SettingFieldURI(const Field & f) : SettingFieldURI(f.safeGet<String>()) {}
    SettingFieldURI(const SettingFieldURI & o) : SettingFieldBase(o.changed), value(o.value) { }

    SettingFieldURI & operator =(const Poco::URI & x) { value = x; changed = true; return *this; }
    SettingFieldURI & operator =(const String & str) { *this = Poco::URI{str}; return *this; }
    SettingFieldURI & operator =(const char * str) { *this = Poco::URI{str}; return *this; }
    SettingFieldURI & operator=(const Field & f) override
    {
        *this = f.safeGet<const String &>();
        return *this;
    }
    SettingFieldURI & operator=(const SettingFieldURI & o)
    {
        value = o.value;
        changed = o.changed;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldURI>(*this); }

    operator const Poco::URI &() const { return value; } /// NOLINT
    explicit operator String() const { return toString(); }
    explicit operator Field() const override { return toString(); }

    String toString() const override { return value.toString(); }
    void parseFromString(const String & str) override { *this = str; }

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
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
struct SettingFieldEnum final : public SettingFieldBase
{
    using EnumType = EnumT;

    EnumType value;

    explicit SettingFieldEnum(EnumType x = EnumType{0}) : value(x) {}
    explicit SettingFieldEnum(const Field & f) : SettingFieldEnum(Traits::fromString(f.safeGet<const String &>())) {}
    SettingFieldEnum(const SettingFieldEnum & o) : SettingFieldBase(o.changed), value(o.value) { }

    SettingFieldEnum & operator =(EnumType x) { value = x; changed = true; return *this; }
    SettingFieldEnum & operator=(const Field & f) override
    {
        *this = Traits::fromString(f.safeGet<const String &>());
        return *this;
    }
    SettingFieldEnum & operator=(const SettingFieldEnum & o)
    {
        value = o.value;
        changed = o.changed;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldEnum>(*this); }

    operator EnumType() const { return value; } /// NOLINT
    explicit operator Field() const override { return toString(); }

    String toString() const override { return Traits::toString(value); }
    void parseFromString(const String & str) override { *this = Traits::fromString(str); }

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
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
struct SettingFieldMultiEnum final : public SettingFieldBase
{
    using EnumType = Enum;
    using ValueType = std::vector<Enum>;

    ValueType value;

    explicit SettingFieldMultiEnum(ValueType v = ValueType{}) : value{v} {}
    explicit SettingFieldMultiEnum(EnumType e) : value{e} {}
    explicit SettingFieldMultiEnum(const Field & f) : value(parseValueFromString(f.safeGet<const String &>())) {}
    SettingFieldMultiEnum(const SettingFieldMultiEnum & o) : SettingFieldBase(o.changed), value(o.value) { }

    operator ValueType() const { return value; } /// NOLINT
    explicit operator Field() const override { return toString(); }
    operator MultiEnum<EnumType>() const /// NOLINT
    {
        MultiEnum<EnumType> res;
        for (const auto & v : value)
            res.set(v);
        return res;
    }

    SettingFieldMultiEnum & operator= (ValueType x) { changed = true; value = x; return *this; }
    SettingFieldMultiEnum & operator=(const Field & x) override
    {
        parseFromString(x.safeGet<const String &>());
        return *this;
    }
    SettingFieldMultiEnum & operator=(const SettingFieldMultiEnum & o)
    {
        changed = o.changed;
        value = o.value;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldMultiEnum>(*this); }

    String toString() const override
    {
        static const String separator = ",";
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
    void parseFromString(const String & str) override { *this = parseValueFromString(str); }

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;

private:
    static ValueType parseValueFromString(const std::string_view str)
    {
        static const String separators=", ";

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
struct SettingFieldTimezone final : public SettingFieldBase
{
    String value;

    explicit SettingFieldTimezone(std::string_view str = {}) { validateTimezone(std::string(str)); value = str; }
    explicit SettingFieldTimezone(const String & str) { validateTimezone(str); value = str; }
    explicit SettingFieldTimezone(String && str) { validateTimezone(str); value = std::move(str); }
    explicit SettingFieldTimezone(const char * str) { validateTimezone(str); value = str; }
    explicit SettingFieldTimezone(const Field & f) { const String & str = f.safeGet<const String &>(); validateTimezone(str); value = str; }
    SettingFieldTimezone(const SettingFieldTimezone & o) : SettingFieldBase(o.changed), value(o.value) { }

    SettingFieldTimezone & operator =(std::string_view str) { validateTimezone(std::string(str)); value = str; changed = true; return *this; }
    SettingFieldTimezone & operator =(const String & str) { *this = std::string_view{str}; return *this; }
    SettingFieldTimezone & operator =(String && str) { validateTimezone(str); value = std::move(str); changed = true; return *this; }
    SettingFieldTimezone & operator =(const char * str) { *this = std::string_view{str}; return *this; }
    SettingFieldTimezone & operator=(const Field & f) override
    {
        *this = f.safeGet<const String &>();
        return *this;
    }
    SettingFieldTimezone & operator=(const SettingFieldTimezone & o)
    {
        changed = o.changed;
        value = o.value;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldTimezone>(*this); }

    operator const String &() const { return value; } /// NOLINT
    explicit operator Field() const override { return value; }

    String toString() const override { return value; }
    void parseFromString(const String & str) override { *this = str; }

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;

private:
    void validateTimezone(const std::string & tz_str);
};

/// Can keep a value of any type. Used for user-defined settings.
struct SettingFieldCustom final : public SettingFieldBase
{
    Field value;

    explicit SettingFieldCustom(const Field & f = {}) : value(f) {}
    SettingFieldCustom(const SettingFieldCustom & o) : SettingFieldBase(o.changed), value(o.value) { }
    SettingFieldCustom(SettingFieldCustom && o) : SettingFieldBase(o.changed), value(std::move(o.value)) { }
    SettingFieldCustom & operator=(const Field & f) override
    {
        value = f;
        changed = true;
        return *this;
    }
    SettingFieldCustom & operator=(const SettingFieldCustom & o)
    {
        value = o.value;
        changed = o.changed;
        return *this;
    }

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldCustom>(*this); }

    explicit operator Field() const override { return value; }

    String toString() const override;
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
};

struct SettingFieldNonZeroUInt64 final : public SettingFieldUInt64
{
public:
    explicit SettingFieldNonZeroUInt64(UInt64 x = 1);
    explicit SettingFieldNonZeroUInt64(const Field & f);

    SettingFieldNonZeroUInt64 & operator=(UInt64 x);
    SettingFieldNonZeroUInt64 & operator=(const Field & f) override;

    std::unique_ptr<SettingFieldBase> clone() const override { return std::make_unique<SettingFieldNonZeroUInt64>(*this); }

    void parseFromString(const String & str) override;

private:
    void checkValueNonZero() const;
};

}
