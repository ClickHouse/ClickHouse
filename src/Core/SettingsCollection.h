#pragma once

#include <Poco/Timespan.h>
#include <Poco/URI.h>
#include <DataStreams/SizeLimits.h>
#include <Formats/FormatSettings.h>
#include <common/StringRef.h>
#include <Core/Types.h>
#include <unordered_map>


namespace DB
{

class Field;
struct SettingChange;
using SettingsChanges = std::vector<SettingChange>;
class ReadBuffer;
class WriteBuffer;
enum class SettingsBinaryFormat;


/** One setting for any type.
  * Stores a value within itself, as well as a flag - whether the value was changed.
  * This is done so that you can send to the remote servers only changed settings (or explicitly specified in the config) values.
  * That is, if the configuration was not specified in the config and was not dynamically changed, it is not sent to the remote server,
  *  and the remote server will use its default value.
  */

template <typename Type>
struct SettingNumber
{
    Type value;
    bool changed = false;

    SettingNumber(Type x = 0) : value(x) {}

    operator Type() const { return value; }
    SettingNumber & operator= (Type x) { set(x); return *this; }

    /// Serialize to a test string.
    String toString() const;

    /// Converts to a field.
    Field toField() const;

    void set(Type x);

    /// Read from SQL literal.
    void set(const Field & x);

    /// Read from text string.
    void set(const String & x);

    /// Serialize to binary stream suitable for transfer over network.
    void serialize(WriteBuffer & buf, SettingsBinaryFormat format) const;

    /// Read from binary stream.
    void deserialize(ReadBuffer & buf, SettingsBinaryFormat format);
};

using SettingUInt64 = SettingNumber<UInt64>;
using SettingInt64 = SettingNumber<Int64>;
using SettingFloat = SettingNumber<float>;
using SettingBool = SettingNumber<bool>;


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

    void serialize(WriteBuffer & buf, SettingsBinaryFormat format) const;
    void deserialize(ReadBuffer & buf, SettingsBinaryFormat format);

    void setAuto();
    static UInt64 getAutoValue();
};


enum class SettingTimespanIO { MILLISECOND, SECOND };

template <SettingTimespanIO io_unit>
struct SettingTimespan
{
    Poco::Timespan value;
    bool changed = false;

    SettingTimespan(UInt64 x = 0) : value(x * microseconds_per_io_unit) {}

    operator Poco::Timespan() const { return value; }
    SettingTimespan & operator= (const Poco::Timespan & x) { set(x); return *this; }

    Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }
    Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }

    String toString() const;
    Field toField() const;

    void set(const Poco::Timespan & x);

    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf, SettingsBinaryFormat format) const;
    void deserialize(ReadBuffer & buf, SettingsBinaryFormat format);

    static constexpr UInt64 microseconds_per_io_unit = (io_unit == SettingTimespanIO::MILLISECOND) ? 1000 : 1000000;
};

using SettingSeconds = SettingTimespan<SettingTimespanIO::SECOND>;
using SettingMilliseconds = SettingTimespan<SettingTimespanIO::MILLISECOND>;


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

    void serialize(WriteBuffer & buf, SettingsBinaryFormat format) const;
    void deserialize(ReadBuffer & buf, SettingsBinaryFormat format);
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

    void serialize(WriteBuffer & buf, SettingsBinaryFormat format) const;
    void deserialize(ReadBuffer & buf, SettingsBinaryFormat format);
};


/// Template class to define enum-based settings.
template <typename EnumType, typename Tag = void>
struct SettingEnum
{
    EnumType value;
    bool changed = false;

    SettingEnum(EnumType x) : value(x) {}

    operator EnumType() const { return value; }
    SettingEnum & operator= (EnumType x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(EnumType x) { value = x; changed = true; }
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf, SettingsBinaryFormat format) const;
    void deserialize(ReadBuffer & buf, SettingsBinaryFormat format);
};

struct SettingURI
{
    Poco::URI value;
    bool changed = false;

    SettingURI(const Poco::URI & x = Poco::URI{}) : value(x) {}

    operator Poco::URI() const { return value; }
    SettingURI & operator= (const Poco::URI & x) { set(x); return *this; }

    String toString() const;
    Field toField() const;

    void set(const Poco::URI & x);
    void set(const Field & x);
    void set(const String & x);

    void serialize(WriteBuffer & buf, SettingsBinaryFormat format) const;
    void deserialize(ReadBuffer & buf, SettingsBinaryFormat format);
};

enum class LoadBalancing
{
    /// among replicas with a minimum number of errors selected randomly
    RANDOM = 0,
    /// a replica is selected among the replicas with the minimum number of errors
    /// with the minimum number of distinguished characters in the replica name and local hostname
    NEAREST_HOSTNAME,
    // replicas with the same number of errors are accessed in the same order
    // as they are specified in the configuration.
    IN_ORDER,
    /// if first replica one has higher number of errors,
    ///   pick a random one from replicas with minimum number of errors
    FIRST_OR_RANDOM,
    // round robin across replicas with the same number of errors.
    ROUND_ROBIN,
};
using SettingLoadBalancing = SettingEnum<LoadBalancing>;


enum class JoinStrictness
{
    Unspecified = 0, /// Query JOIN without strictness will throw Exception.
    ALL, /// Query JOIN without strictness -> ALL JOIN ...
    ANY, /// Query JOIN without strictness -> ANY JOIN ...
};
using SettingJoinStrictness = SettingEnum<JoinStrictness>;

enum class JoinAlgorithm
{
    AUTO = 0,
    HASH,
    PARTIAL_MERGE,
    PREFER_PARTIAL_MERGE,
};
using SettingJoinAlgorithm = SettingEnum<JoinAlgorithm>;


enum class SpecialSort
{
    NOT_SPECIFIED = 0,
    OPENCL_BITONIC,
};
using SettingSpecialSort = SettingEnum<SpecialSort>;


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
using SettingTotalsMode = SettingEnum<TotalsMode>;


/// The settings keeps OverflowMode which cannot be OverflowMode::ANY.
using SettingOverflowMode = SettingEnum<OverflowMode>;
struct SettingOverflowModeGroupByTag;

/// The settings keeps OverflowMode which can be OverflowMode::ANY.
using SettingOverflowModeGroupBy = SettingEnum<OverflowMode, SettingOverflowModeGroupByTag>;


/// The setting for executing distributed subqueries inside IN or JOIN sections.
enum class DistributedProductMode
{
    DENY = 0,    /// Disable
    LOCAL,       /// Convert to local query
    GLOBAL,      /// Convert to global query
    ALLOW        /// Enable
};
using SettingDistributedProductMode = SettingEnum<DistributedProductMode>;


using SettingDateTimeInputFormat = SettingEnum<FormatSettings::DateTimeInputFormat>;


enum class LogsLevel
{
    none = 0,    /// Disable
    fatal,
    error,
    warning,
    information,
    debug,
    trace,
};
using SettingLogsLevel = SettingEnum<LogsLevel>;

enum class DefaultDatabaseEngine
{
    Ordinary,
    Atomic,
};
using SettingDefaultDatabaseEngine = SettingEnum<DefaultDatabaseEngine>;

// Make it signed for compatibility with DataTypeEnum8
enum QueryLogElementType : int8_t
{
    QUERY_START = 1,
    QUERY_FINISH = 2,
    EXCEPTION_BEFORE_START = 3,
    EXCEPTION_WHILE_PROCESSING = 4,
};
using SettingLogQueriesType = SettingEnum<QueryLogElementType>;


enum class SettingsBinaryFormat
{
    OLD,     /// Part of the settings are serialized as strings, and other part as varints. This is the old behaviour.
    STRINGS, /// All settings are serialized as strings. Before each value the flag `is_ignorable` is serialized.
    DEFAULT = STRINGS,
};


/** Template class to define collections of settings.
  * Example of usage:
  *
  * mysettings.h:
  * struct MySettings : public SettingsCollection<MySettings>
  * {
  * #   define APPLY_FOR_MYSETTINGS(M) \
  *         M(SettingUInt64, a, 100, "Description of a", 0) \
  *         M(SettingFloat, f, 3.11, "Description of f", IMPORTANT) // IMPORTANT - means the setting can't be ignored by older versions) \
  *         M(SettingString, s, "default", "Description of s", 0)
  *
  *     DECLARE_SETTINGS_COLLECTION(MySettings, APPLY_FOR_MYSETTINGS)
  * };
  *
  * mysettings.cpp:
  * IMPLEMENT_SETTINGS_COLLECTION(MySettings, APPLY_FOR_MYSETTINGS)
  */
template <class Derived>
class SettingsCollection
{
private:
    Derived & castToDerived() { return *static_cast<Derived *>(this); }
    const Derived & castToDerived() const { return *static_cast<const Derived *>(this); }

    struct MemberInfo
    {
        using IsChangedFunction = bool (*)(const Derived &);
        using GetStringFunction = String (*)(const Derived &);
        using GetFieldFunction = Field (*)(const Derived &);
        using SetStringFunction = void (*)(Derived &, const String &);
        using SetFieldFunction = void (*)(Derived &, const Field &);
        using SerializeFunction = void (*)(const Derived &, WriteBuffer & buf, SettingsBinaryFormat);
        using DeserializeFunction = void (*)(Derived &, ReadBuffer & buf, SettingsBinaryFormat);
        using ValueToStringFunction = String (*)(const Field &);
        using ValueToCorrespondingTypeFunction = Field (*)(const Field &);

        StringRef name;
        StringRef description;
        StringRef type;
        bool is_important;
        IsChangedFunction is_changed;
        GetStringFunction get_string;
        GetFieldFunction get_field;
        SetStringFunction set_string;
        SetFieldFunction set_field;
        SerializeFunction serialize;
        DeserializeFunction deserialize;
        ValueToStringFunction value_to_string;
        ValueToCorrespondingTypeFunction value_to_corresponding_type;
    };

    class MemberInfos
    {
    public:
        MemberInfos();

        size_t size() const { return infos.size(); }
        const MemberInfo * data() const { return infos.data(); }
        const MemberInfo & operator[](size_t index) const { return infos[index]; }

        const MemberInfo * find(const StringRef & name) const;
        const MemberInfo & findStrict(const StringRef & name) const;
        size_t findIndex(const StringRef & name) const;
        size_t findIndexStrict(const StringRef & name) const;

    private:
        void add(MemberInfo && member);

        std::vector<MemberInfo> infos;
        std::unordered_map<StringRef, size_t> by_name_map;
    };

    static const MemberInfos & members();

public:
    class const_iterator;

    /// Provides read-only access to a setting.
    class const_reference
    {
    public:
        const_reference(const Derived & collection_, const MemberInfo & member_) : collection(&collection_), member(&member_) {}
        const_reference(const const_reference & src) = default;
        const StringRef & getName() const { return member->name; }
        const StringRef & getDescription() const { return member->description; }
        const StringRef & getType() const { return member->type; }
        bool isChanged() const { return member->is_changed(*collection); }
        Field getValue() const;
        String getValueAsString() const { return member->get_string(*collection); }

    protected:
        friend class SettingsCollection<Derived>::const_iterator;
        const_reference() : collection(nullptr), member(nullptr) {}
        const_reference & operator=(const const_reference &) = default;
        const Derived * collection;
        const MemberInfo * member;
    };

    /// Provides access to a setting.
    class reference : public const_reference
    {
    public:
        reference(Derived & collection_, const MemberInfo & member_) : const_reference(collection_, member_) {}
        reference(const const_reference & src) : const_reference(src) {}
        void setValue(const Field & value) { this->member->set_field(*const_cast<Derived *>(this->collection), value); }
        void setValue(const String & value) { this->member->set_string(*const_cast<Derived *>(this->collection), value); }
    };

    /// Iterator to iterating through all the settings.
    class const_iterator
    {
    public:
        const_iterator(const Derived & collection_, const MemberInfo * member_) : ref(const_cast<Derived &>(collection_), *member_) {}
        const_iterator() = default;
        const_iterator(const const_iterator & src) = default;
        const_iterator & operator =(const const_iterator & src) = default;
        const const_reference & operator *() const { return ref; }
        const const_reference * operator ->() const { return &ref; }
        const_iterator & operator ++() { ++ref.member; return *this; }
        const_iterator operator ++(int) { const_iterator tmp = *this; ++*this; return tmp; }
        bool operator ==(const const_iterator & rhs) const { return ref.member == rhs.ref.member && ref.collection == rhs.ref.collection; }
        bool operator !=(const const_iterator & rhs) const { return !(*this == rhs); }
    protected:
        mutable reference ref;
    };

    class iterator : public const_iterator
    {
    public:
        iterator(Derived & collection_, const MemberInfo * member_) : const_iterator(collection_, member_) {}
        iterator() = default;
        iterator(const const_iterator & src) : const_iterator(src) {}
        iterator & operator =(const const_iterator & src) { const_iterator::operator =(src); return *this; }
        reference & operator *() const { return this->ref; }
        reference * operator ->() const { return &this->ref; }
        iterator & operator ++() { const_iterator::operator ++(); return *this; }
        iterator operator ++(int) { iterator tmp = *this; ++*this; return tmp; }
    };

    /// Returns the number of settings.
    static size_t size() { return members().size(); }

    /// Returns name of a setting by its index (0..size()-1).
    static StringRef getName(size_t index) { return members()[index].name; }

    /// Returns description of a setting.
    static StringRef getDescription(size_t index) { return members()[index].description; }
    static StringRef getDescription(const String & name) { return members().findStrict(name).description; }

    /// Searches a setting by its name; returns `npos` if not found.
    static size_t findIndex(const StringRef & name) { return members().findIndex(name); }
    static constexpr size_t npos = static_cast<size_t>(-1);

    /// Searches a setting by its name; throws an exception if not found.
    static size_t findIndexStrict(const StringRef & name) { return members().findIndexStrict(name); }

    /// Casts a value to a string according to a specified setting without actual changing this settings.
    static String valueToString(size_t index, const Field & value) { return members()[index].value_to_string(value); }
    static String valueToString(const StringRef & name, const Field & value) { return members().findStrict(name).value_to_string(value); }

    /// Casts a value to a type according to a specified setting without actual changing this settings.
    /// E.g. for SettingInt64 it casts Field to Field::Types::Int64.
    static Field valueToCorrespondingType(size_t index, const Field & value);
    static Field valueToCorrespondingType(const StringRef & name, const Field & value);

    iterator begin() { return iterator(castToDerived(), members().data()); }
    const_iterator begin() const { return const_iterator(castToDerived(), members().data()); }
    iterator end() { const auto & the_members = members(); return iterator(castToDerived(), the_members.data() + the_members.size()); }
    const_iterator end() const { const auto & the_members = members(); return const_iterator(castToDerived(), the_members.data() + the_members.size()); }

    /// Returns a proxy object for accessing to a setting. Throws an exception if there is not setting with such name.
    reference operator[](size_t index) { return reference(castToDerived(), members()[index]); }
    reference operator[](const StringRef & name) { return reference(castToDerived(), members().findStrict(name)); }
    const_reference operator[](size_t index) const { return const_reference(castToDerived(), members()[index]); }
    const_reference operator[](const StringRef & name) const { return const_reference(castToDerived(), members().findStrict(name)); }

    /// Searches a setting by its name; returns end() if not found.
    iterator find(const StringRef & name);
    const_iterator find(const StringRef & name) const;

    /// Searches a setting by its name; throws an exception if not found.
    iterator findStrict(const StringRef & name);
    const_iterator findStrict(const StringRef & name) const;

    /// Sets setting's value.
    void set(size_t index, const Field & value) { (*this)[index].setValue(value); }
    void set(const StringRef & name, const Field & value) { (*this)[name].setValue(value); }

    /// Sets setting's value. Read value in text form from string (for example, from configuration file or from URL parameter).
    void set(size_t index, const String & value) { (*this)[index].setValue(value); }
    void set(const StringRef & name, const String & value) { (*this)[name].setValue(value); }

    /// Returns value of a setting.
    Field get(size_t index) const;
    Field get(const StringRef & name) const;

    /// Returns value of a setting converted to string.
    String getAsString(size_t index) const { return (*this)[index].getValueAsString(); }
    String getAsString(const StringRef & name) const { return (*this)[name].getValueAsString(); }

    /// Returns value of a setting; returns false if there is no setting with the specified name.
    bool tryGet(const StringRef & name, Field & value) const;

    /// Returns value of a setting converted to string; returns false if there is no setting with the specified name.
    bool tryGet(const StringRef & name, String & value) const;

    /// Compares two collections of settings.
    bool operator ==(const SettingsCollection & rhs) const;
    bool operator!=(const SettingsCollection & rhs) const { return !(*this == rhs); }

    /// Gathers all changed values (e.g. for applying them later to another collection of settings).
    SettingsChanges changes() const;

    /// Applies change to concrete setting.
    void applyChange(const SettingChange & change);

    /// Applies changes to the settings.
    void applyChanges(const SettingsChanges & changes);

    void copyChangesFrom(const Derived & src);

    void copyChangesTo(Derived & dest) const;

    /// Writes the settings to buffer (e.g. to be sent to remote server).
    /// Only changed settings are written. They are written as list of contiguous name-value pairs,
    /// finished with empty name.
    void serialize(WriteBuffer & buf, SettingsBinaryFormat format = SettingsBinaryFormat::DEFAULT) const;

    /// Reads the settings from buffer.
    void deserialize(ReadBuffer & buf, SettingsBinaryFormat format = SettingsBinaryFormat::DEFAULT);
};


#define DECLARE_SETTINGS_COLLECTION(LIST_OF_SETTINGS_MACRO) \
    LIST_OF_SETTINGS_MACRO(DECLARE_SETTINGS_COLLECTION_DECLARE_VARIABLES_HELPER_)

#define DECLARE_SETTINGS_COLLECTION_DECLARE_VARIABLES_HELPER_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    TYPE NAME {DEFAULT};
}
