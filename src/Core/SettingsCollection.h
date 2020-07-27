#pragma once

#include <common/StringRef.h>
#include <Core/SettingsFields.h>
#include <unordered_map>


namespace DB
{

class Field;
struct SettingChange;
class SettingsChanges;
class ReadBuffer;
class WriteBuffer;

enum class SettingsBinaryFormat
{
    OLD,     /// Part of the settings are serialized as strings, and other part as variants. This is the old behaviour.
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
  *         M(UInt64, a, 100, "Description of a", 0) \
  *         M(Float, f, 3.11, "Description of f", IMPORTANT) // IMPORTANT - means the setting can't be ignored by older versions) \
  *         M(String, s, "default", "Description of s", 0)
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
        using GetValueFunction = Field (*)(const Derived &);
        using SetValueFunction = void (*)(Derived &, const Field &);
        using GetValueAsStringFunction = String (*)(const Derived &);
        using ParseValueFromStringFunction = void (*)(Derived &, const String &);
        using WriteBinaryFunction = void (*)(const Derived &, WriteBuffer & buf);
        using ReadBinaryFunction = void (*)(Derived &, ReadBuffer & buf);
        using CastValueFunction = Field (*)(const Field &);
        using StringToValueFunction = Field (*)(const String &);
        using ValueToStringFunction = String (*)(const Field &);

        StringRef name;
        StringRef description;
        StringRef type;
        bool is_important;
        IsChangedFunction is_changed;
        GetValueFunction get_value;
        SetValueFunction set_value;
        GetValueAsStringFunction get_value_as_string;
        ParseValueFromStringFunction parse_value_from_string;
        WriteBinaryFunction write_binary;
        ReadBinaryFunction read_binary;
        CastValueFunction cast_value;
        StringToValueFunction string_to_value;
        ValueToStringFunction value_to_string;
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
        String getValueAsString() const { return member->get_value_as_string(*collection); }

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
        void setValue(const Field & value) { this->member->set_value(*const_cast<Derived *>(this->collection), value); }
        void parseFromString(const String & value) { this->member->parse_value_from_string(*const_cast<Derived *>(this->collection), value); }
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

    /// Casts a value to a type according to a specified setting without actual changing this settings.
    /// E.g. for SettingInt64 it casts Field to Field::Types::Int64.
    static Field castValue(size_t index, const Field & value);
    static Field castValue(const StringRef & name, const Field & value);

    /// Casts a value to a string according to a specified setting without actual changing this settings.
    static Field stringToValue(size_t index, const String & str) { return members()[index].string_to_value(str); }
    static Field stringToValue(const StringRef & name, const String & str) { return members().findStrict(name).string_to_value(str); }
    static String valueToString(size_t index, const Field & value) { return members()[index].value_to_string(value); }
    static String valueToString(const StringRef & name, const Field & value) { return members().findStrict(name).value_to_string(value); }

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

    // A debugging aid.
    std::string dumpChangesToString() const;

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
    SettingField##TYPE NAME {DEFAULT};
}
