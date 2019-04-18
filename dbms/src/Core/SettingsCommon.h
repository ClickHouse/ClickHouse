#pragma once

#include <Poco/Timespan.h>
#include <DataStreams/SizeLimits.h>
#include <Formats/FormatSettings.h>
#include <common/StringRef.h>
#include <Common/SettingsChanges.h>
#include <Compression/CompressionInfo.h>
#include <Core/Types.h>
#include <ext/singleton.h>
#include <unordered_map>


namespace DB
{

class Field;
class ReadBuffer;
class WriteBuffer;


/** One setting for any type.
  * Stores a value within itself, as well as a flag - whether the value was changed.
  * This is done so that you can send to the remote servers only changed settings (or explicitly specified in the config) values.
  * That is, if the configuration was not specified in the config and was not dynamically changed, it is not sent to the remote server,
  *  and the remote server will use its default value.
  */

template <typename IntType>
struct SettingInt
{
    IntType value;
    bool changed = false;

    SettingInt(IntType x = 0) : value(x) {}

    operator IntType() const { return value; }
    SettingInt & operator= (IntType x) { set(x); return *this; }

    /// Serialize to a test string.
    String toString() const;

    /// Converts to a field.
    Field toField() const;

    void set(IntType x);

    /// Read from SQL literal.
    void set(const Field & x);

    /// Read from text string.
    void set(const String & x);

    /// Serialize to binary stream suitable for transfer over network.
    void serialize(WriteBuffer & buf) const;

    /// Read from binary stream.
    void deserialize(ReadBuffer & buf);
};

using SettingUInt64 = SettingInt<UInt64>;
using SettingInt64 = SettingInt<Int64>;
using SettingBool = SettingUInt64;


/** Unlike SettingUInt64, supports the value of 'auto' - the number of processor cores without taking into account SMT.
  * A value of 0 is also treated as auto.
  * When serializing, `auto` is written in the same way as 0.
  */
struct SettingMaxThreads
{
    UInt64 value;
    bool is_auto;
    bool changed = false;

    SettingMaxThreads(UInt64 x = 0) : value(x ? x : getAutoValue()), is_auto(x == 0) {}

    operator UInt64() const { return value; }
    SettingMaxThreads & operator= (UInt64 x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);

    void setAuto();
    UInt64 getAutoValue() const;

    /// Executed once for all time. Executed from one thread.
    UInt64 getAutoValueImpl() const;
};


struct SettingSeconds
{
    Poco::Timespan value;
    bool changed = false;

    SettingSeconds(UInt64 seconds = 0) : value(seconds, 0) {}

    operator Poco::Timespan() const { return value; }
    SettingSeconds & operator= (const Poco::Timespan & x) { set(x); return *this; }

    Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }

    String toString() const;
    Field toField() const;

    void set(const Poco::Timespan & x);

    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


struct SettingMilliseconds
{
    Poco::Timespan value;
    bool changed = false;

    SettingMilliseconds(UInt64 milliseconds = 0) : value(milliseconds * 1000) {}

    operator Poco::Timespan() const { return value; }
    SettingMilliseconds & operator= (const Poco::Timespan & x) { set(x); return *this; }

    Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }

    String toString() const;
    Field toField() const;

    void set(const Poco::Timespan & x);
    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


struct SettingFloat
{
    float value;
    bool changed = false;

    SettingFloat(float x = 0) : value(x) {}

    operator float() const { return value; }
    SettingFloat & operator= (float x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(float x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


/// TODO: X macro
enum class LoadBalancing
{
    /// among replicas with a minimum number of errors selected randomly
    RANDOM = 0,
    /// a replica is selected among the replicas with the minimum number of errors
    /// with the minimum number of distinguished characters in the replica name and local hostname
    NEAREST_HOSTNAME,
    /// replicas are walked through strictly in order; the number of errors does not matter
    IN_ORDER,
    /// if first replica one has higher number of errors,
    ///   pick a random one from replicas with minimum number of errors
    FIRST_OR_RANDOM,
};

struct SettingLoadBalancing
{
    LoadBalancing value;
    bool changed = false;

    SettingLoadBalancing(LoadBalancing x) : value(x) {}

    operator LoadBalancing() const { return value; }
    SettingLoadBalancing & operator= (LoadBalancing x) { set(x); return *this; }

    static LoadBalancing getLoadBalancing(const String & s);

    String toString() const;
    Field toField() const;

    void set(LoadBalancing x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


enum class JoinStrictness
{
    Unspecified = 0, /// Query JOIN without strictness will throw Exception.
    ALL, /// Query JOIN without strictness -> ALL JOIN ...
    ANY, /// Query JOIN without strictness -> ANY JOIN ...
};


struct SettingJoinStrictness
{
    JoinStrictness value;
    bool changed = false;

    SettingJoinStrictness(JoinStrictness x) : value(x) {}

    operator JoinStrictness() const { return value; }
    SettingJoinStrictness & operator= (JoinStrictness x) { set(x); return *this; }

    static JoinStrictness getJoinStrictness(const String & s);

    String toString() const;
    Field toField() const;

    void set(JoinStrictness x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


/// Which rows should be included in TOTALS.
enum class TotalsMode
{
    BEFORE_HAVING            = 0, /// Count HAVING for all read rows;
                                  ///  including those not in max_rows_to_group_by
                                  ///  and have not passed HAVING after grouping.
    AFTER_HAVING_INCLUSIVE    = 1, /// Count on all rows except those that have not passed HAVING;
                                   ///  that is, to include in TOTALS all the rows that did not pass max_rows_to_group_by.
    AFTER_HAVING_EXCLUSIVE    = 2, /// Include only the rows that passed and max_rows_to_group_by, and HAVING.
    AFTER_HAVING_AUTO         = 3, /// Automatically select between INCLUSIVE and EXCLUSIVE,
};

struct SettingTotalsMode
{
    TotalsMode value;
    bool changed = false;

    SettingTotalsMode(TotalsMode x) : value(x) {}

    operator TotalsMode() const { return value; }
    SettingTotalsMode & operator= (TotalsMode x) { set(x); return *this; }

    static TotalsMode getTotalsMode(const String & s);

    String toString() const;
    Field toField() const;

    void set(TotalsMode x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


template <bool enable_mode_any>
struct SettingOverflowMode
{
    OverflowMode value;
    bool changed = false;

    SettingOverflowMode(OverflowMode x = OverflowMode::THROW) : value(x) {}

    operator OverflowMode() const { return value; }
    SettingOverflowMode & operator= (OverflowMode x) { set(x); return *this; }

    static OverflowMode getOverflowModeForGroupBy(const String & s);
    static OverflowMode getOverflowMode(const String & s);

    String toString() const;
    Field toField() const;

    void set(OverflowMode x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};

/// The setting for executing distributed subqueries inside IN or JOIN sections.
enum class DistributedProductMode
{
    DENY = 0,    /// Disable
    LOCAL,       /// Convert to local query
    GLOBAL,      /// Convert to global query
    ALLOW        /// Enable
};

struct SettingDistributedProductMode
{
    DistributedProductMode value;
    bool changed = false;

    SettingDistributedProductMode(DistributedProductMode x) : value(x) {}

    operator DistributedProductMode() const { return value; }
    SettingDistributedProductMode & operator= (DistributedProductMode x) { set(x); return *this; }

    static DistributedProductMode getDistributedProductMode(const String & s);

    String toString() const;
    Field toField() const;

    void set(DistributedProductMode x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


struct SettingString
{
    String value;
    bool changed = false;

    SettingString(const String & x = String{}) : value(x) {}

    operator String() const { return value; }
    SettingString & operator= (const String & x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(const String & x);
    void set(const Field & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


struct SettingChar
{
public:
    char value;
    bool changed = false;

    SettingChar(char x = '\0') : value(x) {}

    operator char() const { return value; }
    SettingChar & operator= (char x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(char x);
    void set(const String & x);
    void set(const Field & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


struct SettingDateTimeInputFormat
{
    using Value = FormatSettings::DateTimeInputFormat;

    Value value;
    bool changed = false;

    SettingDateTimeInputFormat(Value x) : value(x) {}

    operator Value() const { return value; }
    SettingDateTimeInputFormat & operator= (Value x) { set(x); return *this; }

    static Value getValue(const String & s);

    String toString() const;
    Field toField() const;

    void set(Value x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


enum class LogsLevel
{
    none = 0,    /// Disable
    error,
    warning,
    information,
    debug,
    trace,
};

class SettingLogsLevel
{
public:
    using Value = LogsLevel;

    Value value;
    bool changed = false;

    SettingLogsLevel(Value x) : value(x) {}

    operator Value() const { return value; }
    SettingLogsLevel & operator= (Value x) { set(x); return *this; }

    static Value getValue(const String & s);

    String toString() const;
    Field toField() const;

    void set(Value x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};


namespace details
{
    struct SettingsCollectionUtils
    {
        static void serializeName(const StringRef & name, WriteBuffer & buf);
        static String deserializeName(ReadBuffer & buf);
        static void throwNameNotFound(const StringRef & name);
    };
}


/** Template class to define collections of settings.
  * Example of usage:
  *
  * mysettings.h:
  * struct MySettings : public SettingsCollection<MySettings>
  * {
  * #   define APPLY_FOR_MYSETTINGS(M) \
  *         M(SettingUInt64, a, 100, "Description of a") \
  *         M(SettingFloat, f, 3.11, "Description of f") \
  *         M(SettingString, s, "default", "Description of s")
  *
  *     DECLARE_SETTINGS_COLLECTION(MySettings, APPLY_FOR_MYSETTINGS)
  * };
  *
  * mysettings.cpp:
  * IMPLEMENT_SETTINGS_COLLECTION(MySettings, APPLY_FOR_MYSETTINGS)
  */
template <typename T>
class SettingsCollection
{
private:
    using Type = T;
    using GetStringFunction = String (*)(const Type &);
    using GetFieldFunction = Field (*)(const Type &);
    using SetStringFunction = void (*)(Type &, const String &);
    using SetFieldFunction = void (*)(Type &, const Field &);
    using SerializeFunction = void (*)(const Type &, WriteBuffer & buf);
    using DeserializeFunction = void (*)(Type &, ReadBuffer & buf);

    struct MemberInfo
    {
        size_t offset_of_changed;
        StringRef name;
        StringRef description;
        GetStringFunction get_string;
        GetFieldFunction get_field;
        SetStringFunction set_string;
        SetFieldFunction set_field;
        SerializeFunction serialize;
        DeserializeFunction deserialize;
        bool isChanged(const Type & collection) const { return *reinterpret_cast<const bool*>(reinterpret_cast<const UInt8*>(&collection) + offset_of_changed); }
    };

    using MemberInfos = std::vector<MemberInfo>;

    class Layout
    {
    public:
        size_t size() const { return infos.size(); }
        const MemberInfo & operator[](size_t index) const { return infos[index]; }

        static constexpr size_t npos = static_cast<size_t>(-1);

        size_t find(const StringRef & name) const
        {
            auto map_it = by_name_map.find(name);
            if (map_it == by_name_map.end())
                return npos;
            else
                return map_it->second;
        }

        size_t findStrict(const StringRef & name) const
        {
            auto map_it = by_name_map.find(name);
            if (map_it == by_name_map.end())
                details::SettingsCollectionUtils::throwNameNotFound(name);
            return map_it->second;
        }

        void addMemberInfo(MemberInfo && info)
        {
            size_t index = infos.size();
            infos.emplace_back(info);
            by_name_map.emplace(infos.back().name, index);
        }

    private:
        MemberInfos infos;
        std::unordered_map<StringRef, size_t> by_name_map;
    };

    static const Layout & getLayout();
    Type & castThis() { return static_cast<Type &>(*this); }
    const Type & castThis() const { return static_cast<const Type &>(*this); }

public:
    /// Provides access to a setting.
    class reference
    {
    public:
        const StringRef & getName() const { return member.name; }
        const StringRef & getDescription() const { return member.description; }
        bool isChanged() const { return member.isChanged(collection); }
        Field getValue() const { return member.get_field(collection); }
        String getValueAsString() const { return member.get_string(collection); }
        void setValue(const Field & value) { member.set_field(collection, value); }
        void setValue(const String & value) { member.set_string(collection, value); }

        reference(Type & collection_, const MemberInfo & member_) : collection(collection_), member(member_) {}
    private:
        Type & collection;
        const MemberInfo & member;
    };

    /// Provides read-only access to a setting.
    class const_reference
    {
    public:
        const StringRef & getName() const { return member.name; }
        const StringRef & getDescription() const { return member.description; }
        bool isChanged() const { return member.isChanged(collection); }
        Field getValue() const { return member.get_field(collection); }
        String getValueAsString() const { return member.get_string(collection); }

        const_reference(const Type & collection_, const MemberInfo & member_) : collection(collection_), member(member_) {}
    private:
        const Type & collection;
        const MemberInfo & member;
    };

    /// Returns number of settings.
    size_t size() const { return getLayout().size(); }

    /// Returns a setting by its zero-based index.
    reference operator [](size_t index) { return reference(castThis(), getLayout()[index]); }
    const_reference operator [](size_t index) const { return const_reference(castThis(), getLayout()[index]); }

    /// Returns a setting by its name. Throws an exception if there is not setting with such name.
    reference operator [](const String & name)
    {
        const Layout & layout = getLayout();
        return reference(castThis(), layout[layout.findStrict(name)]);
    }

    const_reference operator [](const String & name) const
    {
        const Layout & layout = getLayout();
        return const_reference(castThis(), layout[layout.findStrict(name)]);
    }

    /// Finds a setting by name and returns its index. Returns npos if not found.
    size_t find(const String & name) const { return getLayout().find(name); }
    static constexpr size_t npos = static_cast<size_t>(-1);

    /// Finds a setting by name and returns its index. Throws an exception if not found.
    size_t findStrict(const String & name) const { return getLayout().findStrict(name); }

    /// Sets setting by name.
    void set(const String & name, const Field & value) { (*this)[name].setValue(value); }

    /// Sets setting by name. Read value in text form from string (for example, from configuration file or from URL parameter).
    void set(const String & name, const String & value) { (*this)[name].setValue(value); }

    /// Returns the value of a setting.
    Field get(const String & name) const { return (*this)[name].getValue(); }

    /// Returns the value of a setting converted to string.
    String getAsString(const String & name) const { return (*this)[name].getValueAsString(); }

    /// Returns the value of a setting.
    bool tryGet(const String & name, Field & value) const
    {
        const Layout & layout = getLayout();
        size_t index = layout.find(name);
        if (index == npos)
            return false;
        value = layout[index].get_field(castThis());
        return true;
    }

    /// Returns the value of a setting converted to string.
    bool tryGet(const String & name, String & value) const
    {
        const Layout & layout = getLayout();
        size_t index = layout.find(name);
        if (index == npos)
            return false;
        value = layout[index].get_string(castThis());
        return true;
    }

    /// Compares two collections of settings.
    bool operator ==(const Type & rhs) const
    {
        const Layout & layout = getLayout();
        for (size_t i = 0; i != layout.size(); ++i)
        {
            const auto & member = layout[i];
            bool left_changed = member.isChanged(castThis());
            bool right_changed = member.isChanged(rhs);
            if (left_changed || right_changed)
            {
                if (left_changed != right_changed)
                    return false;
                if (member.get_field(castThis()) != member.get_field(rhs))
                    return false;
            }
        }
        return true;
    }

    bool operator !=(const Type & rhs) const
    {
        return !(*this == rhs);
    }

    /// Gathers all changed values (e.g. for applying them later to another collection of settings).
    SettingsChanges changes() const
    {
        SettingsChanges found_changes;
        const Layout & layout = getLayout();
        for (size_t i = 0; i != layout.size(); ++i)
        {
            const auto & member = layout[i];
            if (member.isChanged(castThis()))
                found_changes.emplace_back(member.name.toString(), member.get_field(castThis()));
        }
        return found_changes;
    }

    /// Applies changes to the settings.
    void applyChange(const SettingChange & change)
    {
        set(change.name, change.value);
    }

    void applyChanges(const SettingsChanges & changes)
    {
        for (const SettingChange & change : changes)
            applyChange(change);
    }

    void copyChangesFrom(const Type & src)
    {
        const Layout & layout = getLayout();
        for (size_t i = 0; i != layout.size(); ++i)
        {
            const auto & member = layout[i];
            if (member.isChanged(src))
                member.set_field(castThis(), member.get_field(src));
        }
    }

    void copyChangesTo(Type & dest) const
    {
        dest.copyChangesFrom(castThis());
    }

    /// Writes the settings to buffer (e.g. to be sent to remote server).
    /// Only changed settings are written. They are written as list of contiguous name-value pairs,
    /// finished with empty name.
    void serialize(WriteBuffer & buf) const
    {
        const Layout & layout = getLayout();
        for (size_t i = 0; i != layout.size(); ++i)
        {
            const auto & member = layout[i];
            if (member.isChanged(castThis()))
            {
                details::SettingsCollectionUtils::serializeName(member.name, buf);
                member.serialize(castThis(), buf);
            }
        }
        details::SettingsCollectionUtils::serializeName(StringRef{} /* empty string is a marker of the end of settings */, buf);
    }

    /// Reads the settings from buffer.
    void deserialize(ReadBuffer & buf)
    {
        const Layout & layout = getLayout();
        while (true)
        {
            String name = details::SettingsCollectionUtils::deserializeName(buf);
            if (name.empty() /* empty string is a marker of the end of settings */)
                break; \
            const auto & member = layout[layout.findStrict(name)];
            member.deserialize(castThis(), buf); \
        }
    }
};

#define DECLARE_SETTINGS_COLLECTION(APPLY_MACRO) \
    APPLY_MACRO(DECLARE_SETTINGS_COLLECTION_DECLARE_VARIABLES_HELPER_)


#define IMPLEMENT_SETTINGS_COLLECTION(CLASS_NAME, APPLY_MACRO) \
    template<> \
    const SettingsCollection<CLASS_NAME>::Layout & SettingsCollection<CLASS_NAME>::getLayout() \
    { \
        struct Functions \
        { \
            APPLY_MACRO(IMPLEMENT_SETTINGS_COLLECTION_DEFINE_FUNCTIONS_HELPER_) \
        }; \
        static const SettingsCollection<Type>::Layout single_instance = [] \
        { \
            SettingsCollection<Type>::Layout layout; \
            APPLY_MACRO(IMPLEMENT_SETTINGS_COLLECTION_ADD_MEMBER_INFO_HELPER_) \
            return layout; \
        }(); \
        return single_instance; \
    }


#define DECLARE_SETTINGS_COLLECTION_DECLARE_VARIABLES_HELPER_(TYPE, NAME, DEFAULT, DESCRIPTION) \
    TYPE NAME {DEFAULT};


#define IMPLEMENT_SETTINGS_COLLECTION_DEFINE_FUNCTIONS_HELPER_(TYPE, NAME, DEFAULT, DESCRIPTION) \
    static String NAME##_getString(const Type & collection) { return collection.NAME.toString(); } \
    static Field NAME##_getField(const Type & collection) { return collection.NAME.toField(); } \
    static void NAME##_setString(Type & collection, const String & value) { collection.NAME.set(value); } \
    static void NAME##_setField(Type & collection, const Field & value) { collection.NAME.set(value); } \
    static void NAME##_serialize(const Type & collection, WriteBuffer & buf) { collection.NAME.serialize(buf); } \
    static void NAME##_deserialize(Type & collection, ReadBuffer & buf) { collection.NAME.deserialize(buf); }


#define IMPLEMENT_SETTINGS_COLLECTION_ADD_MEMBER_INFO_HELPER_(TYPE, NAME, DEFAULT, DESCRIPTION) \
    static_assert(std::is_same_v<decltype(std::declval<TYPE>().changed), bool>); \
    layout.addMemberInfo({offsetof(Type, NAME.changed), \
                          StringRef(#NAME, strlen(#NAME)), StringRef(#DESCRIPTION, strlen(#DESCRIPTION)), \
                          &Functions::NAME##_getString, &Functions::NAME##_getField, \
                          &Functions::NAME##_setString, &Functions::NAME##_setField, \
                          &Functions::NAME##_serialize, &Functions::NAME##_deserialize });

}
