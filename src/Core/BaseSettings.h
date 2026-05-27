#pragma once

#include <Core/SettingIndex.h>
#include <Core/SettingsFields.h>
#include <Core/SettingsTierType.h>
#include <Core/SettingsWriteFormat.h>
#include <IO/Operators.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SettingsChanges.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <string_view>
#include <type_traits>
#include <unordered_map>

#include <boost/blank.hpp>


namespace boost::program_options
{
    class options_description;
}

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Utility functions and types used by the BaseSettings template class.
/// These are implementation details that handle serialization, error reporting,
/// and metadata management for settings.
struct BaseSettingsHelpers
{
    /// Error handling
    [[noreturn]] static void throwSettingNotFound(std::string_view name);
    static void warningSettingNotFound(std::string_view name);
    static void flushWarnings();

    /// Serialization helpers
    static void writeString(std::string_view str, WriteBuffer & out);
    static String readString(ReadBuffer & in);

    /// Setting metadata flags
    enum Flags : UInt64
    {
        IMPORTANT = 0x01,  /// Setting affects query results, cannot be ignored by older versions
        CUSTOM = 0x02,     /// User-defined custom setting
        TIER = 0x0c,       /// 0b1100 == 2 bits for tier level (PRODUCTION/BETA/EXPERIMENTAL)
        /// Flag indicating that changes from config can be picked up without server restart.
        /// Currently only works in CoordinationSettings.
        HOT_RELOAD = 0x80,
    };

    static SettingsTierType getTier(UInt64 flags);
    static void writeFlags(Flags flags, WriteBuffer & out);
    static UInt64 readFlags(ReadBuffer & in);

private:
    /// For logging the summary of unknown settings instead of logging each one separately.
    inline static thread_local Strings unknown_settings;
    inline static thread_local bool unknown_settings_warning_logged = false;
};

/// Maps a Traits type to its owning settings class (e.g. `SettingsTraits` -> `Settings`,
/// `MergeTreeSettingsTraits` -> `MergeTreeSettings`). The specialization is emitted by
/// `IMPLEMENT_SETTINGS_EXTERN_` next to the per-setting `SettingIndex` extern variables,
/// so it is visible exactly where `(*impl)[Setting::name]` is used.
///
/// `BaseSettings::operator[]` uses this to constrain the SettingIndex parameter to the
/// owning class, so a `SettingIndex<MergeTreeSettings, ...>` cannot be applied to a
/// `BaseSettings<SettingsTraits>` instance (which would otherwise reinterpret bytes at
/// the wrong layout offset).
template <typename Traits>
struct SettingsOwner;

/** Template class to define collections of settings with compile-time metadata.
  *
  * OVERVIEW:
  * This template provides a type-safe, compile-time-checked settings system.
  * Settings are defined via macros that generate metadata and accessor functions.
  *
  * DESIGN RATIONALE:
  * - Type safety: Each setting has a specific type (UInt64, Bool, String, etc.)
  * - Performance: No runtime lookups for direct member access
  * - Metadata: Descriptions, default values, and flags are compile-time constants
  * - Serialization: Built-in support for reading/writing settings
  *
  * USAGE EXAMPLE:
  * See the detailed example in the comments below.
  *
  * WORKFLOW:
  * 1. Define your settings list with DECLARE() macros in a .cpp file
  * 2. Use DECLARE_SETTINGS_TRAITS() to generate the trait struct
  * 3. Use IMPLEMENT_SETTINGS_TRAITS() to implement the accessor
  * 4. Derive your SettingsImpl from BaseSettings<YourTraits>
  * 5. Use IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR to enable settings.member syntax
  *
  * VALIDATION:
  * If you create a new setting, please also add it to:
  * ./utils/check-style/check-settings-style for validation
  *
  * DETAILED EXAMPLE:
  *
  * mysettings.h:
  * -------------
  * #include <Core/BaseSettingsFwdMacros.h>
  * #include <Core/SettingsFields.h>
  *
  * #define MY_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
  *      M(CLASS_NAME, Float) \
  *      M(CLASS_NAME, String) \
  *      M(CLASS_NAME, UInt64)
  *
  * MY_SETTINGS_SUPPORTED_TYPES(MySettings, DECLARE_SETTING_TRAIT)
  *
  * struct MySettings
  * {
  *     MySettings();
  *     ~MySettings();
  *
  *     MY_SETTINGS_SUPPORTED_TYPES(MySettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)
  * private:
  *     std::unique_ptr<MySettingsImpl> impl;
  * };
  *
  * mysettings.cpp:
  * ---------------
  * #include <Core/BaseSettings.h>
  * #include <Core/BaseSettingsFwdMacrosImpl.h>
  *
  * #define APPLY_FOR_MYSETTINGS(DECLARE, DECLARE_WITH_ALIAS) \
  *     DECLARE(UInt64, a, 100, "Description of a", 0) \
  *     DECLARE(Float, f, 3.11, "Description of f", IMPORTANT) \
  *     DECLARE(String, s, "default", "Description of s", 0) \
  *     DECLARE_WITH_ALIAS(String, experimental, "default", "Description", 0, stable)
  *
  * DECLARE_SETTINGS_TRAITS(MySettingsTraits, APPLY_FOR_MYSETTINGS, MY_SETTINGS_SUPPORTED_TYPES)
  * IMPLEMENT_SETTINGS_TRAITS(MySettingsTraits, APPLY_FOR_MYSETTINGS, MySettings, MySetting)
  *
  * MY_SETTINGS_SUPPORTED_TYPES(MySettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)
  */
template <class TTraits>
class BaseSettings : public TTraits::Data
{
    /// Hash function for efficient string lookups in custom settings map
    struct StringHash
    {
        using is_transparent = void;
        size_t operator()(std::string_view txt) const { return std::hash<std::string_view>{}(txt); }
    };
    using CustomSettingMap = UnorderedMapWithMemoryTracking<String, SettingFieldCustom, StringHash, std::equal_to<>>;

public:
    BaseSettings() = default;
    BaseSettings(const BaseSettings &) = default;
    BaseSettings(BaseSettings &&) noexcept = default;

    /// Direct field access via SettingIndex offset. Allows (*impl)[Setting::name] syntax
    /// inside settings .cpp files where Impl methods need to read/write individual fields.
    /// The Owner template parameter is checked against the owning class of TTraits at
    /// function-instantiation time, so a SettingIndex from a different settings class is a
    /// hard compile error rather than silently reinterpreting bytes at the wrong layout offset.
    /// (The check lives in the body, not the signature, so the SettingsOwner specialization
    /// only needs to be visible at the call site, not at every BaseSettings<TTraits> class
    /// instantiation point.)
    template <typename Owner, typename FieldType>
    const FieldType & operator[](SettingIndex<Owner, FieldType> index) const
    {
        static_assert(std::is_same_v<Owner, typename SettingsOwner<TTraits>::type>,
                      "SettingIndex belongs to a different settings class than this BaseSettings");
        /* `index.offset` is relative to `Traits::Data`; convert from `this` (a real Impl object) */
        /* to its `Data` base subobject via a well-defined static_cast, then add the field offset. */
        const auto * data = static_cast<const typename TTraits::Data *>(this);
        return *reinterpret_cast<const FieldType *>(reinterpret_cast<const char *>(data) + index.offset);
    }
    template <typename Owner, typename FieldType>
    FieldType & operator[](SettingIndex<Owner, FieldType> index)
    {
        static_assert(std::is_same_v<Owner, typename SettingsOwner<TTraits>::type>,
                      "SettingIndex belongs to a different settings class than this BaseSettings");
        auto * data = static_cast<typename TTraits::Data *>(this);
        return *reinterpret_cast<FieldType *>(reinterpret_cast<char *>(data) + index.offset);
    }
    BaseSettings & operator=(const BaseSettings &) = default;
    BaseSettings & operator=(BaseSettings &&) noexcept = default;
    virtual ~BaseSettings() = default;

    using Traits = TTraits;

    /// Set a setting by name
    virtual void set(std::string_view name, const Field & value);

    /// Get the value of a setting
    Field get(std::string_view name) const;

    /// Try to get a setting value; returns false if setting doesn't exist
    bool tryGet(std::string_view name, Field & value) const;

    /// Check if a setting has been changed from its default value
    bool isChanged(std::string_view name) const;

    /// Get all changed settings as a SettingsChanges struct
    SettingsChanges changes() const;

    /// Apply a single setting change
    void applyChange(const SettingChange & change);

    /// Apply multiple setting changes
    void applyChanges(const SettingsChanges & changes);

    /// Resets all the settings to their default values
    void resetToDefault();

    /// Resets specified setting to its default value
    void resetToDefault(std::string_view name);

    /// Check if a setting exists (either built-in or custom)
    bool has(std::string_view name) const { return hasBuiltin(name) || hasCustom(name); }

    /// Check if a built-in setting exists (excludes custom settings)
    static bool hasBuiltin(std::string_view name);

    /// Check if a custom setting exists
    bool hasCustom(std::string_view name) const;

    /// Get the type name of a setting (e.g., "UInt64", "String")
    std::string_view getTypeName(std::string_view name) const;

    /// Get the description of a setting
    std::string_view getDescription(std::string_view name) const;

    /// Get the tier (PRODUCTION/BETA/EXPERIMENTAL) of a setting
    SettingsTierType getTier(std::string_view name) const;

    // ========================================================================
    // VALIDATION & CONVERSION (static utilities)
    // ========================================================================

    /// Checks if it's possible to assign a field to a specified value and throws an exception if not.
    /// This function doesn't change the fields, it performs check only.
    static void checkCanSet(std::string_view name, const Field & value);

    /// Conversions without changing the fields.
    static Field castValueUtil(std::string_view name, const Field & value);
    static String valueToStringUtil(std::string_view name, const Field & value);
    static Field stringToValueUtil(std::string_view name, const String & str);

    /// Write all settings (or only changed ones depending on format)
    void write(WriteBuffer & out, SettingsWriteFormat format = SettingsWriteFormat::DEFAULT) const;

    /// Read settings from a buffer
    void read(ReadBuffer & in, SettingsWriteFormat format = SettingsWriteFormat::DEFAULT);

    /// Write only changed settings in binary format
    void writeChangedBinary(WriteBuffer & out) const;

    /// Read settings in binary format
    void readBinary(ReadBuffer & in);

    /// Copy settings with HOT_RELOAD flag from `new_settings` into `this`.
    /// Leave other settings unchanged.
    void updateHotReloadableSettings(const BaseSettings & new_settings);

    /// Convert all settings to a human-readable string (for debugging)
    std::string toString() const;

    /// Represents a reference to a setting field.
    class SettingFieldRef
    {
    public:
        const String & getName() const;
        std::string_view getPath() const;
        Field getValue() const;
        void setValue(const Field & value);
        String getValueString() const;
        String getDefaultValueString() const;
        bool isValueChanged() const;
        std::string_view getTypeName() const;
        std::string_view getDescription() const;
        bool isCustom() const;
        SettingsTierType getTier() const;
        bool isHotReload() const;

        bool operator==(const SettingFieldRef & other) const { return (getName() == other.getName()) && (getValue() == other.getValue()); }
        bool operator!=(const SettingFieldRef & other) const { return !(*this == other); }

    private:
        friend class BaseSettings;
        BaseSettings * settings;
        const typename Traits::Accessor * accessor;
        size_t index;
        std::conditional_t<Traits::allow_custom_settings, typename CustomSettingMap::iterator *, boost::blank> custom_setting;
    };

    enum SkipFlags
    {
        SKIP_NONE = 0,
        SKIP_CHANGED = 0x01,
        SKIP_UNCHANGED = 0x02,
        SKIP_BUILTIN = 0x04,
        SKIP_CUSTOM = 0x08,
        SKIP_ALL = SKIP_CHANGED | SKIP_UNCHANGED | SKIP_CUSTOM | SKIP_BUILTIN,
    };

    class Iterator
    {
    public:
        Iterator & operator++();
        Iterator operator++(int); /// NOLINT
        const SettingFieldRef & operator *() const { return field_ref; }
        SettingFieldRef & operator *() { return field_ref; }

        bool operator ==(const Iterator & other) const;
        bool operator !=(const Iterator & other) const { return !(*this == other); }

    private:
        friend class BaseSettings;
        Iterator(BaseSettings & settings_, const typename Traits::Accessor & accessor_, SkipFlags skip_flags_);
        void doSkip();
        void setPointerToCustomSetting();

        SettingFieldRef field_ref;
        std::conditional_t<Traits::allow_custom_settings, typename CustomSettingMap::iterator, boost::blank> custom_settings_iterator;
        SkipFlags skip_flags;
    };

    class Range
    {
    public:
        Range(BaseSettings & settings_, SkipFlags skip_flags_) : settings(settings_), accessor(Traits::Accessor::instance()), skip_flags(skip_flags_) {}
        Iterator begin() const { return Iterator(settings, accessor, skip_flags); }
        Iterator end() const { return Iterator(settings, accessor, SKIP_ALL); }

    private:
        BaseSettings & settings;
        const typename Traits::Accessor & accessor;
        SkipFlags skip_flags;
    };

    class MutableRange
    {
    public:
        MutableRange(BaseSettings & settings_, SkipFlags skip_flags_) : settings(settings_), accessor(Traits::Accessor::instance()), skip_flags(skip_flags_) {}
        Iterator begin() { return Iterator(settings, accessor, skip_flags); }
        Iterator end() { return Iterator(settings, accessor, SKIP_ALL); }

    private:
        BaseSettings & settings;
        const typename Traits::Accessor & accessor;
        SkipFlags skip_flags;
    };

    Range all(SkipFlags skip_flags = SKIP_NONE) const { return Range{const_cast<BaseSettings<Traits> &>(*this), skip_flags}; }
    MutableRange allMutable(SkipFlags skip_flags = SKIP_NONE) { return MutableRange{*this, skip_flags}; }
    Range allChanged() const { return all(SKIP_UNCHANGED); }
    Range allUnchanged() const { return all(SKIP_CHANGED); }

    Iterator begin() const { return allChanged().begin(); }
    Iterator end() const { return allChanged().end(); }

private:
    SettingFieldCustom & getCustomSetting(std::string_view name);
    const SettingFieldCustom & getCustomSetting(std::string_view name) const;
    const SettingFieldCustom * tryGetCustomSetting(std::string_view name) const;

protected:
    std::conditional_t<Traits::allow_custom_settings, CustomSettingMap, boost::blank> custom_settings_map;
};

template <typename TTraits>
void BaseSettings<TTraits>::set(std::string_view name, const Field & value)
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        accessor.setValue(*this, index, value);
    else
        getCustomSetting(name) = value;
}

template <typename TTraits>
Field BaseSettings<TTraits>::get(std::string_view name) const
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.getValue(*this, index);
    return static_cast<Field>(getCustomSetting(name));
}

template <typename TTraits>
bool BaseSettings<TTraits>::tryGet(std::string_view name, Field & value) const
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
    {
        value = accessor.getValue(*this, index);
        return true;
    }
    if (const auto * custom_setting = tryGetCustomSetting(name))
    {
        value = static_cast<Field>(*custom_setting);
        return true;
    }
    return false;
}

template <typename TTraits>
bool BaseSettings<TTraits>::isChanged(std::string_view name) const
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.isValueChanged(*this, index);
    return tryGetCustomSetting(name) != nullptr;
}

template <typename TTraits>
SettingsChanges BaseSettings<TTraits>::changes() const
{
    SettingsChanges res;
    for (const auto & field : *this)
        res.emplace_back(field.getName(), field.getValue());
    return res;
}

template <typename TTraits>
void BaseSettings<TTraits>::applyChange(const SettingChange & change)
{
    set(change.name, change.value);
}

template <typename TTraits>
void BaseSettings<TTraits>::applyChanges(const SettingsChanges & changes)
{
    for (const auto & change : changes)
        applyChange(change);
}

template <typename TTraits>
void BaseSettings<TTraits>::resetToDefault()
{
    const auto & accessor = Traits::Accessor::instance();
    for (size_t i = 0; i < accessor.size(); i++)
    {
        if (accessor.isValueChanged(*this, i))
            accessor.resetValueToDefault(*this, i);
    }

    if constexpr (Traits::allow_custom_settings)
        custom_settings_map.clear();
}

template <typename TTraits>
void BaseSettings<TTraits>::resetToDefault(std::string_view name)
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
    {
        accessor.resetValueToDefault(*this, index);
        return;
    }

    if constexpr (Traits::allow_custom_settings)
        custom_settings_map.erase(String{name});
}

template <typename TTraits>
bool BaseSettings<TTraits>::hasBuiltin(std::string_view name)
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    return (accessor.find(name) != static_cast<size_t>(-1));
}

template <typename TTraits>
bool BaseSettings<TTraits>::hasCustom(std::string_view name) const
{
    name = TTraits::resolveName(name);
    return tryGetCustomSetting(name);
}

template <typename TTraits>
std::string_view BaseSettings<TTraits>::getTypeName(std::string_view name) const
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.getTypeName(index);
    if (tryGetCustomSetting(name))
        return "Custom";
    BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
std::string_view BaseSettings<TTraits>::getDescription(std::string_view name) const
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.getDescription(index);
    if (tryGetCustomSetting(name))
        return "Custom";
    BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
SettingsTierType BaseSettings<TTraits>::getTier(std::string_view name) const
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.getTier(index);
    if (tryGetCustomSetting(name))
        return SettingsTierType::PRODUCTION;
    BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
void BaseSettings<TTraits>::checkCanSet(std::string_view name, const Field & value)
{
    name = TTraits::resolveName(name);
    castValueUtil(name, value);
}

template <typename TTraits>
Field BaseSettings<TTraits>::castValueUtil(std::string_view name, const Field & value)
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.castValueUtil(index, value);
    if constexpr (Traits::allow_custom_settings)
        return value;
    else
        BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
String BaseSettings<TTraits>::valueToStringUtil(std::string_view name, const Field & value)
{
    name = TTraits::resolveName(name);
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.valueToStringUtil(index, value);
    if constexpr (Traits::allow_custom_settings)
        return value.dump();
    else
        BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
Field BaseSettings<TTraits>::stringToValueUtil(std::string_view name, const String & str)
{
    name = TTraits::resolveName(name);
    try
    {
        const auto & accessor = Traits::Accessor::instance();
        if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
            return accessor.stringToValueUtil(index, str);
        if constexpr (Traits::allow_custom_settings)
            return Field::restoreFromDump(str);
        else
            BaseSettingsHelpers::throwSettingNotFound(name);
    }
    catch (Exception & e)
    {
        e.addMessage("while parsing value '{}' for setting '{}'", str, name);
        throw;
    }
}

template <typename TTraits>
void BaseSettings<TTraits>::write(WriteBuffer & out, SettingsWriteFormat format) const
{
    const auto & accessor = Traits::Accessor::instance();

    for (const auto & field : *this)
    {
        bool is_custom = field.isCustom();
        bool is_important = !is_custom && accessor.isImportant(field.index);

        BaseSettingsHelpers::writeString(field.getName(), out);

        if ((format >= SettingsWriteFormat::STRINGS_WITH_FLAGS) || is_custom)
        {
            using Flags = BaseSettingsHelpers::Flags;
            Flags flags{};
            if (is_custom)
                flags = static_cast<Flags>(flags | Flags::CUSTOM);
            else if (is_important)
                flags = static_cast<Flags>(flags | Flags::IMPORTANT);
            BaseSettingsHelpers::writeFlags(flags, out);

            BaseSettingsHelpers::writeString(field.getValueString(), out);
        }
        else
            accessor.writeBinary(*this, field.index, out);
    }

    /// Empty string is a marker of the end of settings.
    BaseSettingsHelpers::writeString(std::string_view{}, out);
}

template <typename TTraits>
void BaseSettings<TTraits>::writeChangedBinary(WriteBuffer & out) const
{
    const auto & accessor = Traits::Accessor::instance();

    size_t num_settings = 0;
    for (auto it = this->begin(); it != this->end(); ++it)
        ++num_settings;

    writeVarUInt(num_settings, out);

    for (const auto & field : *this)
    {
        BaseSettingsHelpers::writeString(field.getName(), out);
        using Flags = BaseSettingsHelpers::Flags;
        Flags flags{};
        BaseSettingsHelpers::writeFlags(flags, out);
        accessor.writeBinary(*this, field.index, out);
    }
}

template <typename TTraits>
void BaseSettings<TTraits>::readBinary(ReadBuffer & in)
{
    const auto & accessor = Traits::Accessor::instance();

    size_t num_settings = 0;
    readVarUInt(num_settings, in);

    for (size_t i = 0; i < num_settings; ++i)
    {
        String read_name = BaseSettingsHelpers::readString(in);
        std::string_view name = TTraits::resolveName(read_name);
        size_t index = accessor.find(name);

        std::ignore = BaseSettingsHelpers::readFlags(in);

        if (index == static_cast<size_t>(-1))
            BaseSettingsHelpers::throwSettingNotFound(name);

        accessor.readBinary(*this, index, in);
    }
}

template <typename TTraits>
void BaseSettings<TTraits>::read(ReadBuffer & in, SettingsWriteFormat format)
{
    resetToDefault();
    const auto & accessor = Traits::Accessor::instance();
    while (true)
    {
        String read_name = BaseSettingsHelpers::readString(in);
        if (read_name.empty() /* empty string is a marker of the end of settings */)
            break;

        std::string_view name = TTraits::resolveName(read_name);
        size_t index = accessor.find(name);

        using Flags = BaseSettingsHelpers::Flags;
        UInt64 flags{};
        if (format >= SettingsWriteFormat::STRINGS_WITH_FLAGS)
            flags = BaseSettingsHelpers::readFlags(in);
        bool is_important = (flags & Flags::IMPORTANT);
        bool is_custom = (flags & Flags::CUSTOM);

        if (index != static_cast<size_t>(-1))
        {
            if (is_custom)
            {
                SettingFieldCustom temp;
                temp.parseFromString(BaseSettingsHelpers::readString(in));
                accessor.setValue(*this, index, static_cast<Field>(temp));
            }
            else if (format >= SettingsWriteFormat::STRINGS_WITH_FLAGS)
                accessor.setValueString(*this, index, BaseSettingsHelpers::readString(in));
            else
                accessor.readBinary(*this, index, in);
        }
        else if (is_custom && Traits::allow_custom_settings)
        {
            getCustomSetting(name).parseFromString(BaseSettingsHelpers::readString(in));
        }
        else if (is_important)
        {
            BaseSettingsHelpers::throwSettingNotFound(name);
        }
        else
        {
            BaseSettingsHelpers::warningSettingNotFound(name);
            BaseSettingsHelpers::readString(in);
        }
    }
    BaseSettingsHelpers::flushWarnings();
}

template <typename TTraits>
void BaseSettings<TTraits>::updateHotReloadableSettings(const BaseSettings & new_settings)
{
    const auto & accessor = Traits::Accessor::instance();
    for (size_t index = 0; index < accessor.size(); ++index)
    {
        if (!accessor.isHotReload(index))
            continue;
        Field value = accessor.getValue(new_settings, index);
        accessor.setValue(*this, index, value);
    }
}

template <typename TTraits>
String BaseSettings<TTraits>::toString() const
{
    WriteBufferFromOwnString out;
    bool first = true;
    for (const auto & setting : *this)
    {
        if (!first)
            out << ", ";
        out << setting.getName() << " = " << applyVisitor(FieldVisitorToString(), setting.getValue());
        first = false;
    }
    return out.str();
}

template <typename TTraits>
bool operator==(const BaseSettings<TTraits> & left, const BaseSettings<TTraits> & right)
{
    auto l = left.begin();
    for (const auto & r : right)
    {
        if ((l == left.end()) || (*l != r))
            return false;
        ++l;
    }
    return l == left.end();
}

template <typename TTraits>
bool operator!=(const BaseSettings<TTraits> & left, const BaseSettings<TTraits> & right)
{
    return !(left == right);
}

template <typename TTraits>
SettingFieldCustom & BaseSettings<TTraits>::getCustomSetting(std::string_view name)
{
    if constexpr (Traits::allow_custom_settings)
    {
        auto it = custom_settings_map.find(name);
        if (it == custom_settings_map.end())
            it = custom_settings_map.emplace(String{name}, SettingFieldCustom{}).first;
        return it->second;
    }
    BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
const SettingFieldCustom & BaseSettings<TTraits>::getCustomSetting(std::string_view name) const
{
    if constexpr (Traits::allow_custom_settings)
    {
        auto it = custom_settings_map.find(name);
        if (it != custom_settings_map.end())
            return it->second;
    }
    BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
const SettingFieldCustom * BaseSettings<TTraits>::tryGetCustomSetting(std::string_view name) const
{
    if constexpr (Traits::allow_custom_settings)
    {
        auto it = custom_settings_map.find(name);
        if (it != custom_settings_map.end())
            return &it->second;
    }
    return nullptr;
}

template <typename TTraits>
BaseSettings<TTraits>::Iterator::Iterator(BaseSettings & settings_, const typename Traits::Accessor & accessor_, SkipFlags skip_flags_)
    : skip_flags(skip_flags_)
{
    field_ref.settings = &settings_;
    field_ref.accessor = &accessor_;
    auto & index = field_ref.index;

    if (skip_flags == SKIP_ALL)
    {
        index = accessor_.size();
        if constexpr (Traits::allow_custom_settings)
        {
            custom_settings_iterator = settings_.custom_settings_map.end();
            field_ref.custom_setting = nullptr;
        }
        return;
    }

    if (skip_flags & SKIP_CHANGED)
    {
        skip_flags = static_cast<SkipFlags>(skip_flags | SKIP_CUSTOM);
        if (skip_flags & SKIP_UNCHANGED)
            skip_flags = static_cast<SkipFlags>(skip_flags | SKIP_BUILTIN);
    }

    if (skip_flags & SKIP_BUILTIN)
        index = accessor_.size();
    else
        index = 0;

    if constexpr (Traits::allow_custom_settings)
    {
        if (skip_flags & SKIP_CUSTOM)
            custom_settings_iterator = settings_.custom_settings_map.end();
        else
            custom_settings_iterator = settings_.custom_settings_map.begin();
    }

    doSkip();
    setPointerToCustomSetting();
}

template <typename TTraits>
typename BaseSettings<TTraits>::Iterator & BaseSettings<TTraits>::Iterator::operator++()
{
    const auto & accessor = *field_ref.accessor;
    auto & index = field_ref.index;
    if (index != accessor.size())
        ++index;
    else
    {
        if constexpr (Traits::allow_custom_settings)
            ++custom_settings_iterator;
    }
    doSkip();
    setPointerToCustomSetting();
    return *this;
}

template <typename TTraits>
typename BaseSettings<TTraits>::Iterator BaseSettings<TTraits>::Iterator::operator++(int) /// NOLINT
{
    auto res = *this;
    ++*this;
    return res;
}

template <typename TTraits>
void BaseSettings<TTraits>::Iterator::doSkip()
{
    const auto & accessor = *field_ref.accessor;
    const auto & settings = *field_ref.settings;
    auto & index = field_ref.index;
    if (skip_flags & SKIP_CHANGED)
    {
        while ((index != accessor.size()) && accessor.isValueChanged(settings, index))
            ++index;
    }
    else if (skip_flags & SKIP_UNCHANGED)
    {
        while ((index != accessor.size()) && !accessor.isValueChanged(settings, index))
            ++index;
    }
}

template <typename TTraits>
void BaseSettings<TTraits>::Iterator::setPointerToCustomSetting()
{
    if constexpr (Traits::allow_custom_settings)
    {
        const auto & accessor = *field_ref.accessor;
        const auto & settings = *field_ref.settings;
        const auto & index = field_ref.index;
        if ((index == accessor.size()) && (custom_settings_iterator != settings.custom_settings_map.end()))
            field_ref.custom_setting = &custom_settings_iterator;
        else
            field_ref.custom_setting = nullptr;
    }
}

template <typename TTraits>
bool BaseSettings<TTraits>::Iterator::operator ==(const typename BaseSettings<TTraits>::Iterator & other) const
{
    if constexpr (TTraits::allow_custom_settings)
    {
        if (custom_settings_iterator != other.custom_settings_iterator)
            return false;
    }
    return ((field_ref.index == other.field_ref.index) && (field_ref.settings == other.field_ref.settings));
}

template <typename TTraits>
const String & BaseSettings<TTraits>::SettingFieldRef::getName() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return (*custom_setting)->first;
    }
    return accessor->getName(index);
}

template <typename TTraits>
std::string_view BaseSettings<TTraits>::SettingFieldRef::getPath() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return "";
    }
    return accessor->getPath(index);
}

template <typename TTraits>
Field BaseSettings<TTraits>::SettingFieldRef::getValue() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return static_cast<Field>((*custom_setting)->second);
    }
    return accessor->getValue(*settings, index);
}

template <typename TTraits>
void BaseSettings<TTraits>::SettingFieldRef::setValue(const Field & value)
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            (*custom_setting)->second = value;
    }
    else
        accessor->setValue(*settings, index, value);
}

template <typename TTraits>
String BaseSettings<TTraits>::SettingFieldRef::getValueString() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return (*custom_setting)->second.toString();
    }
    return accessor->getValueString(*settings, index);
}

template <typename TTraits>
String BaseSettings<TTraits>::SettingFieldRef::getDefaultValueString() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return (*custom_setting)->second.toString();
    }
    return accessor->getDefaultValueString(index);
}

template <typename TTraits>
bool BaseSettings<TTraits>::SettingFieldRef::isValueChanged() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return true;
    }
    return accessor->isValueChanged(*settings, index);
}

template <typename TTraits>
std::string_view BaseSettings<TTraits>::SettingFieldRef::getTypeName() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return "Custom";
    }
    return accessor->getTypeName(index);
}

template <typename TTraits>
std::string_view BaseSettings<TTraits>::SettingFieldRef::getDescription() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return "Custom";
    }
    return accessor->getDescription(index);
}

template <typename TTraits>
bool BaseSettings<TTraits>::SettingFieldRef::isCustom() const
{
    if constexpr (Traits::allow_custom_settings)
        return custom_setting != nullptr;
    else
        return false;
}

template <typename TTraits>
SettingsTierType BaseSettings<TTraits>::SettingFieldRef::getTier() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return SettingsTierType::PRODUCTION;
    }
    return accessor->getTier(index);
}

template <typename TTraits>
bool BaseSettings<TTraits>::SettingFieldRef::isHotReload() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return false;
    }
    return accessor->isHotReload(index);
}

// ============================================================================
// MACRO SYSTEM FOR DEFINING SETTINGS TRAITS
// ============================================================================

/** MACRO OVERVIEW:
  *
  * The macros below generate the "Traits" struct that describes a collection of settings.
  * The Traits struct contains:
  * - Data: A struct with actual SettingField members for each setting
  * - Accessor: A singleton class providing runtime reflection/metadata
  *
  * MACRO HIERARCHY:
  * - DECLARE_SETTINGS_TRAITS*: Public entry points, forward to DECLARE_SETTINGS_TRAITS_COMMON
  * - DECLARE_SETTINGS_TRAITS_COMMON: Generates the Traits struct declaration
  * - IMPLEMENT_SETTINGS_TRAITS*: Public entry points, forward to IMPLEMENT_SETTINGS_TRAITS_COMMON
  * - IMPLEMENT_SETTINGS_TRAITS_COMMON: Implements the Accessor singleton
  * - Helper macros: DECLARE_SETTINGS_TRAITS_, IMPLEMENT_SETTINGS_TRAITS_, etc.
  *
  * The complexity here is necessary to support:
  * - Compile-time type safety
  * - Runtime reflection (name -> value lookups)
  * - Efficient direct member access (no hash lookups for known settings)
  * - Custom user-defined settings (optional)
  * - Setting aliases
  */

using AliasMap = UnorderedMapWithMemoryTracking<std::string_view, std::string_view>;

// ---------------------------------------------------------------------------
// Helper macros for constexpr typed-array layout generation.
// Used by DECLARE_SETTINGS_TRAITS_COMMON and IMPLEMENT_SETTINGS_TRAITS_COMMON.
//
// These use unprefixed names (SettingTypeTag, SettingID, settings_layout, etc.)
// at namespace scope. This is safe because each .cpp file has at most one
// DECLARE_SETTINGS_TRAITS call, so there are no collisions within a TU.
// ---------------------------------------------------------------------------

#define SETTING_DECLARE_TYPE_TAG_(CLASS_NAME, TYPE) TYPE,
#define SETTING_DECLARE_ID_(TYPE, NAME, ...) NAME,
#define SETTING_TAG_ARRAY_ENTRY_(TYPE, NAME, ...) SettingTypeTag::TYPE,
#define SETTING_DECLARE_DATA_ARRAY_(CLASS_NAME, TYPE) \
    SettingField##TYPE TYPE##_[CLASS_NAME##Detail::settings_layout.type_counts[static_cast<size_t>(CLASS_NAME##Detail::SettingTypeTag::TYPE)]];

/// These reference Traits_ / Owner_ which are `using` aliases injected by
/// IMPLEMENT_SETTINGS_TRAITS_COMMON / IMPLEMENT_SETTINGS_EXTERN_ into the enclosing scope
/// before LIST_OF_SETTINGS expansion. This indirection is needed because LIST_OF_SETTINGS
/// callbacks are global #defines and can't reference the traits name directly (it's a
/// parameter of the outer macro, not visible to inner #defines).
#define SETTING_INIT_DEFAULT_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    TYPE##_[Traits_::settings_layout_.local_index[static_cast<size_t>(Traits_::SettingID_::NAME)]] = SettingField##TYPE(DEFAULT);
/* Offset is relative to `Traits_::Data` (not the Impl); `BaseSettings::operator[]` does the */
/* derived-to-base cast on a real `this` before adding it, so we never need a forged-pointer */
/* probe to compute the Impl->Data base distance. */
#define SETTING_EXTERN_ENTRY_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    SettingIndex<Owner_, SettingField##TYPE> NAME{  \
        offsetof(Traits_::Data, TYPE##_) \
        + Traits_::settings_layout_.local_index[static_cast<size_t>(Traits_::SettingID_::NAME)] * sizeof(SettingField##TYPE) \
        }; /* NOLINT(misc-use-internal-linkage) */

/// Generate traits for a basic settings collection (no custom settings, no paths)
/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, SUPPORTED_TYPES_MACRO) \
    DECLARE_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, SETTING_SKIP_TRAIT, 0, SUPPORTED_TYPES_MACRO)

/// Generate traits with support for custom (user-defined) settings
/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS_ALLOW_CUSTOM_SETTINGS(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, SUPPORTED_TYPES_MACRO) \
    DECLARE_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, SETTING_SKIP_TRAIT, 1, SUPPORTED_TYPES_MACRO)

/// Generate traits with support for settings that have config file paths
/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS_WITH_PATH(SETTINGS_TRAITS_NAME, LIST_NO_PATH, LIST_WITH_PATH, SUPPORTED_TYPES_MACRO) \
    DECLARE_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_NO_PATH, LIST_WITH_PATH, 0, SUPPORTED_TYPES_MACRO)

/// Generate traits with both custom settings and path support
/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS_WITH_PATH_ALLOW_CUSTOM_SETTINGS(SETTINGS_TRAITS_NAME, LIST_NO_PATH, LIST_WITH_PATH, SUPPORTED_TYPES_MACRO) \
    DECLARE_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_NO_PATH, LIST_WITH_PATH, 1, SUPPORTED_TYPES_MACRO)


// ----------------------------------------------------------------------------
// DECLARE_SETTINGS_TRAITS_COMMON - The actual implementation
// ----------------------------------------------------------------------------

/** This macro generates a complete Traits struct with:
  *
  * 1. Constexpr layout computation: groups settings by type into contiguous arrays
  *    for minimal padding and efficient bulk copy.
  *
  * 2. Data struct: Contains typed arrays (e.g., Bool_[], UInt64_[]) sized by the
  *    constexpr layout, with a constructor that initializes default values.
  *
  * 3. Accessor class: Provides runtime reflection capabilities
  *    - Maps setting names to indices
  *    - Provides generic get/set via Field interface
  *    - Stores metadata (description, type, default value, flags)
  *    - Singleton pattern for efficiency
  *
  * PARAMETERS:
  * - SETTINGS_TRAITS_NAME: Name of the generated traits struct
  * - LIST_OF_SETTINGS_WITHOUT_PATH_MACRO: Macro that expands to DECLARE() calls
  * - LIST_OF_SETTINGS_WITH_PATH_MACRO: Macro for settings with config paths
  * - ALLOW_CUSTOM_SETTINGS: 0 or 1, enables user-defined settings
  * - SUPPORTED_TYPES_MACRO: X-macro listing all SettingField types used, e.g. M(Bool) M(UInt64)
  */
/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS_COMMON( \
    SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_WITHOUT_PATH_MACRO, LIST_OF_SETTINGS_WITH_PATH_MACRO, \
    ALLOW_CUSTOM_SETTINGS, SUPPORTED_TYPES_MACRO) \
    /* Constexpr typed-array layout: groups settings by SettingField type into contiguous */ \
    /* arrays (e.g. Bool_[], UInt64_[]) so that all fields of the same type are adjacent. */ \
    /* This avoids padding between differently-sized types and enables bulk memcpy for */ \
    /* trivially-copyable types in the copy constructor. The layout (array sizes and */ \
    /* per-setting indices) is computed at compile time from the settings list, replacing */ \
    /* what would otherwise require an external code-generation step. */ \
    /* Helpers live in a per-traits namespace so each settings .cpp gets its own qualified */ \
    /* names (e.g. `SettingsTraits_detail::SettingTypeTag` vs `MergeTreeSettingsTraits_detail::`). */ \
    /* Putting them at namespace scope under fixed names would be an ODR violation: every .cpp */ \
    /* redefines them with different contents under the same `DB::SettingTypeTag`. Nesting */ \
    /* inside Traits directly hits constexpr-evaluation issues because in-class member function */ \
    /* bodies and default-member-initializers of nested types are deferred. */ \
    namespace SETTINGS_TRAITS_NAME##Detail \
    { \
        enum class SettingTypeTag : uint8_t \
        { \
            SUPPORTED_TYPES_MACRO(SETTINGS_TRAITS_NAME, SETTING_DECLARE_TYPE_TAG_) \
            NUM_TYPES \
        }; \
        enum class SettingID : size_t \
        { \
            LIST_OF_SETTINGS_WITHOUT_PATH_MACRO(SETTING_DECLARE_ID_, SETTING_DECLARE_ID_) \
            LIST_OF_SETTINGS_WITH_PATH_MACRO(SETTING_DECLARE_ID_, SETTING_DECLARE_ID_) \
            NUM_SETTINGS \
        }; \
        struct SettingsLayout \
        { \
            size_t type_counts[static_cast<size_t>(SettingTypeTag::NUM_TYPES)] = {}; \
            size_t local_index[static_cast<size_t>(SettingID::NUM_SETTINGS)] = {}; \
        }; \
        constexpr SettingTypeTag setting_type_tags[] = { \
            LIST_OF_SETTINGS_WITHOUT_PATH_MACRO(SETTING_TAG_ARRAY_ENTRY_, SETTING_TAG_ARRAY_ENTRY_) \
            LIST_OF_SETTINGS_WITH_PATH_MACRO(SETTING_TAG_ARRAY_ENTRY_, SETTING_TAG_ARRAY_ENTRY_) \
        }; \
        constexpr SettingsLayout computeSettingsLayout() \
        { \
            SettingsLayout layout; \
            for (size_t i = 0; i < static_cast<size_t>(SettingID::NUM_SETTINGS); ++i) \
                layout.local_index[i] = layout.type_counts[static_cast<size_t>(setting_type_tags[i])]++; \
            return layout; \
        } \
        constexpr SettingsLayout settings_layout = computeSettingsLayout(); \
    } \
    \
    struct SETTINGS_TRAITS_NAME \
    { \
        using SettingTypeTag_ = SETTINGS_TRAITS_NAME##Detail::SettingTypeTag; \
        using SettingID_ = SETTINGS_TRAITS_NAME##Detail::SettingID; \
        static constexpr auto & settings_layout_ = SETTINGS_TRAITS_NAME##Detail::settings_layout; \
        \
        /* Data struct: typed arrays sized by the constexpr layout */ \
        struct Data \
        { \
            SUPPORTED_TYPES_MACRO(SETTINGS_TRAITS_NAME, SETTING_DECLARE_DATA_ARRAY_) \
            /* Default ctor: typed copy from `Accessor::instance().default_data`. The per-setting */ \
            /* init lives in the private tag ctor below and runs only when the singleton's */ \
            /* `default_data` member is initialized for the first time. */ \
            Data(); \
            Data(const Data &) = default; \
            Data(Data &&) = default; \
            Data & operator=(const Data &) = default; \
            Data & operator=(Data &&) = default; \
            \
            /* Tag for the per-setting init constructor. Used only by `Accessor::default_data` */ \
            /* during singleton construction; every other `Data()` call is a typed copy from */ \
            /* that singleton. The tag has an explicit default ctor so callers can't synthesize */ \
            /* it accidentally with `{}`. */ \
            struct DefaultInitTag { explicit DefaultInitTag() = default; }; \
            explicit Data(DefaultInitTag); \
        }; \
        \
        DECLARE_SETTINGS_TRAITS_BODY_(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_WITHOUT_PATH_MACRO, LIST_OF_SETTINGS_WITH_PATH_MACRO, ALLOW_CUSTOM_SETTINGS) \
    };

/// Shared body for settings traits — everything after the Data definition.
/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS_BODY_( \
    SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_WITHOUT_PATH_MACRO, LIST_OF_SETTINGS_WITH_PATH_MACRO, ALLOW_CUSTOM_SETTINGS) \
        /** Accessor: Provides runtime reflection and metadata access */ \
        class Accessor \
        { \
        public: \
            /** Get the singleton instance (initialized once at first access) */ \
            static const Accessor & instance(); \
            \
            /** Get total number of settings */ \
            size_t size() const { return field_infos.size(); } \
            \
            /** Find setting index by name. Returns -1 if not found. */ \
            size_t find(std::string_view name) const; \
            \
            /* Metadata accessors (by index) */ \
            const String & getName(size_t index) const { return field_infos[index].name; } \
            std::string_view getPath(size_t index) const { return field_infos[index].path; } \
            std::string_view getTypeName(size_t index) const { return field_infos[index].type; } \
            std::string_view getDescription(size_t index) const { return field_infos[index].description; } \
            bool isImportant(size_t index) const { return field_infos[index].flags & BaseSettingsHelpers::Flags::IMPORTANT; } \
            bool isHotReload(size_t index) const { return field_infos[index].flags & BaseSettingsHelpers::Flags::HOT_RELOAD; } \
            SettingsTierType getTier(size_t index) const { return BaseSettingsHelpers::getTier(field_infos[index].flags); } \
            \
            /* Value conversion utilities — use type-level ops (no Data instance needed) */ \
            Field castValueUtil(size_t index, const Field & value) const \
            { \
                return field_infos[index].ops->cast_value(value); \
            } \
            String valueToStringUtil(size_t index, const Field & value) const \
            { \
                return field_infos[index].ops->value_to_string(value); \
            } \
            Field stringToValueUtil(size_t index, const String & str) const \
            { \
                return field_infos[index].ops->string_to_value(str); \
            } \
            \
            /* Direct data access (by index) */ \
            static void * settingPtr(Data & data, size_t offset) \
            { \
                return reinterpret_cast<char *>(&data) + offset; \
            } \
            static const void * settingPtr(const Data & data, size_t offset) \
            { \
                return reinterpret_cast<const char *>(&data) + offset; \
            } \
            void setValue(Data & data, size_t index, const Field & value) const \
            { \
                const auto & fi = field_infos[index]; \
                fi.ops->assign_from_field(settingPtr(data, fi.data_offset), value); \
            } \
            Field getValue(const Data & data, size_t index) const \
            { \
                const auto & fi = field_infos[index]; \
                return fi.ops->to_field(settingPtr(data, fi.data_offset)); \
            } \
            void setValueString(Data & data, size_t index, const String & str) const \
            { \
                const auto & fi = field_infos[index]; \
                fi.ops->parse_from_string(settingPtr(data, fi.data_offset), str); \
            } \
            String getValueString(const Data & data, size_t index) const \
            { \
                const auto & fi = field_infos[index]; \
                return fi.ops->to_string(settingPtr(data, fi.data_offset)); \
            } \
            bool isValueChanged(const Data & data, size_t index) const \
            { \
                const auto & fi = field_infos[index]; \
                return fi.ops->is_changed(settingPtr(data, fi.data_offset)); \
            } \
            void resetValueToDefault(Data & data, size_t index) const \
            { \
                /* Typed copy from the canonical default-constructed Data, dispatched per type via */ \
                /* SettingFieldOps::typed_copy. Avoids a Field round-trip for types whose */ \
                /* `operator Field` is non-invertible (e.g. `SettingFieldMaxThreads`, issue #103120). */ \
                const auto & fi = field_infos[index]; \
                fi.ops->typed_copy(settingPtr(data, fi.data_offset), settingPtr(default_data, fi.data_offset)); \
            } \
            \
            /* Binary serialization (by index) */ \
            void writeBinary(const Data & data, size_t index, WriteBuffer & out) const \
            { \
                const auto & fi = field_infos[index]; \
                fi.ops->write_binary(settingPtr(data, fi.data_offset), out); \
            } \
            void readBinary(Data & data, size_t index, ReadBuffer & in) const \
            { \
                const auto & fi = field_infos[index]; \
                fi.ops->read_binary(settingPtr(data, fi.data_offset), in); \
            } \
            \
            /* Default value as string, read from the canonical default-constructed Data. */ \
            String getDefaultValueString(size_t index) const \
            { \
                const auto & fi = field_infos[index]; \
                return fi.ops->to_string(settingPtr(default_data, fi.data_offset)); \
            } \
            \
            /* Canonical default-constructed Data instance. The public `Data()` constructor */ \
            /* copies from this so each fresh Settings construction is a typed copy rather than */ \
            /* a full per-setting init. */ \
            const Data & getDefaultData() const { return default_data; } \
            \
        private: \
            Accessor(); \
            \
            /** Metadata for one setting */ \
            struct FieldInfo \
            { \
                String name; \
                const std::string_view path; \
                const std::string_view type; \
                const std::string_view description; \
                const UInt64 flags; \
                const SettingFieldOps * ops;                    /* Type-erased ops, shared per type */ \
                size_t data_offset;                             /* Byte offset within Data struct */ \
            }; \
            \
            VectorWithMemoryTracking<FieldInfo> field_infos;                        /* Metadata for all settings */ \
            UnorderedMapWithMemoryTracking<std::string_view, size_t> name_to_index_map; /* Fast name -> index lookup */ \
            /* Canonical default-constructed instance. Used to reset individual settings to their */ \
            /* declared defaults via a typed copy (see resetValueToDefault) and to read the default */ \
            /* string representation (see getDefaultValueString). Initialized once via the tag */ \
            /* ctor; the public `Data()` then becomes a typed copy from this member. */ \
            Data default_data{typename Data::DefaultInitTag{}}; \
        }; \
        \
        /** Whether this traits allows custom settings */ \
        static constexpr bool allow_custom_settings = ALLOW_CUSTOM_SETTINGS; \
        \
        /** Map of aliases to actual setting names */ \
        static inline const AliasMap aliases_to_settings \
            = {LIST_OF_SETTINGS_WITHOUT_PATH_MACRO(SETTING_SKIP_TRAIT, DECLARE_SETTINGS_WITH_ALIAS_TRAITS_) \
                   LIST_OF_SETTINGS_WITH_PATH_MACRO(SETTING_SKIP_TRAIT, DECLARE_SETTINGS_WITH_ALIAS_TRAITS_)}; \
        \
        /** Reverse map: setting name -> list of aliases */ \
        using SettingsToAliasesMap = UnorderedMapWithMemoryTracking<std::string_view, VectorWithMemoryTracking<std::string_view>>; \
        static inline const SettingsToAliasesMap & settingsToAliases() \
        { \
            static SettingsToAliasesMap setting_to_aliases_mapping = [] \
            { \
                UnorderedMapWithMemoryTracking<std::string_view, VectorWithMemoryTracking<std::string_view>> map; \
                for (const auto & [alias, destination] : aliases_to_settings) \
                    map[destination].push_back(alias); \
                return map; \
            }(); \
            return setting_to_aliases_mapping; \
        } \
        \
        /** Resolve alias to actual setting name */ \
        static std::string_view resolveName(std::string_view name) \
        { \
            if (auto it = aliases_to_settings.find(name); it != aliases_to_settings.end()) \
                return it->second; \
            return name; \
        }


/// Skip this setting in the macro expansion (used for selective application)
/// NOLINTNEXTLINE
#define SETTING_SKIP_TRAIT(...)


/// Generates an alias mapping entry
/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_WITH_ALIAS_TRAITS_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ALIAS) \
    { #ALIAS, #NAME },

/// Implement the full settings infrastructure for a settings class.
/// Generates: Impl struct, Data constructor, Accessor singleton, and
/// namespace SETTING_NAMESPACE { NAME ... } extern variables.
///
/// Usage: IMPLEMENT_SETTINGS_TRAITS(MemorySettingsTraits, MEMORY_SETTINGS, MemorySettings, MemorySetting)
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, CLASS_NAME, SETTING_NAMESPACE) \
    struct CLASS_NAME##Impl : public BaseSettings<SETTINGS_TRAITS_NAME> {}; \
    IMPLEMENT_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, SETTING_SKIP_TRAIT) \
    IMPLEMENT_SETTINGS_EXTERN_(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, CLASS_NAME, SETTING_NAMESPACE)

/// For Impl types that have extra methods (sanityCheck, loadFromConfig, etc.)
/// and must be defined manually before this macro.
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS_CUSTOM_IMPL(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, CLASS_NAME, SETTING_NAMESPACE) \
    IMPLEMENT_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, SETTING_SKIP_TRAIT) \
    IMPLEMENT_SETTINGS_EXTERN_(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, CLASS_NAME, SETTING_NAMESPACE)

/// For settings with config file paths (ServerSettings) that need a custom Impl.
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS_WITH_PATH_CUSTOM_IMPL(SETTINGS_TRAITS_NAME, LIST_NO_PATH, LIST_WITH_PATH, CLASS_NAME, SETTING_NAMESPACE) \
    IMPLEMENT_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_NO_PATH, LIST_WITH_PATH) \
    IMPLEMENT_SETTINGS_EXTERN_WITH_PATH_(SETTINGS_TRAITS_NAME, LIST_NO_PATH, LIST_WITH_PATH, CLASS_NAME, SETTING_NAMESPACE)

/// Generates the extern SettingIndex variables inside namespace SETTING_NAMESPACE.
/// Owner_ and Traits_ are `using` aliases so that SETTING_EXTERN_ENTRY_ (a global #define)
/// can reference the per-traits types without knowing the traits name directly.
/// SettingIndex offsets are relative to `Traits::Data` so `BaseSettings::operator[]` can do
/// the derived-to-base conversion on a real `this` and avoid any forged-pointer arithmetic.
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_EXTERN_(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, CLASS_NAME, SETTING_NAMESPACE) \
    _Pragma("clang diagnostic push") \
    _Pragma("clang diagnostic ignored \"-Winvalid-offsetof\"") \
    struct CLASS_NAME; /* forward declaration for SettingIndex owner tag */ \
    /* Bind the traits to its owning class so `BaseSettings<Traits>::operator[]` rejects */ \
    /* SettingIndex values from a different settings class at compile time. */ \
    template <> struct SettingsOwner<SETTINGS_TRAITS_NAME> { using type = CLASS_NAME; }; \
    namespace SETTING_NAMESPACE \
    { \
        using Owner_ = CLASS_NAME; \
        using Traits_ = SETTINGS_TRAITS_NAME; \
        LIST_OF_SETTINGS_MACRO(SETTING_EXTERN_ENTRY_, SETTING_EXTERN_ENTRY_) \
    } \
    _Pragma("clang diagnostic pop")

/// Same as IMPLEMENT_SETTINGS_EXTERN_ but iterates both path and non-path lists.
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_EXTERN_WITH_PATH_(SETTINGS_TRAITS_NAME, LIST_NO_PATH, LIST_WITH_PATH, CLASS_NAME, SETTING_NAMESPACE) \
    _Pragma("clang diagnostic push") \
    _Pragma("clang diagnostic ignored \"-Winvalid-offsetof\"") \
    struct CLASS_NAME; /* forward declaration for SettingIndex owner tag */ \
    /* Bind the traits to its owning class so `BaseSettings<Traits>::operator[]` rejects */ \
    /* SettingIndex values from a different settings class at compile time. */ \
    template <> struct SettingsOwner<SETTINGS_TRAITS_NAME> { using type = CLASS_NAME; }; \
    namespace SETTING_NAMESPACE \
    { \
        using Owner_ = CLASS_NAME; \
        using Traits_ = SETTINGS_TRAITS_NAME; \
        LIST_NO_PATH(SETTING_EXTERN_ENTRY_, SETTING_EXTERN_ENTRY_) \
        LIST_WITH_PATH(SETTING_EXTERN_ENTRY_, SETTING_EXTERN_ENTRY_) \
    } \
    _Pragma("clang diagnostic pop")


/** This macro implements:
  * 1. Data::Data() constructor — initializes all settings to their defaults
  * 2. Accessor::instance() — Singleton accessor, initialized lazily on first access
  * 3. Accessor::Accessor() — Private constructor
  * 4. Accessor::find() — Name lookup function
  * 5. Explicit template instantiation of BaseSettings<SETTINGS_TRAITS_NAME>
  */
    /// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_WITHOUT_PATH_MACRO, LIST_OF_SETTINGS_WITH_PATH_MACRO) \
    /* Per-setting init: invoked once via the singleton's `default_data` member. */ \
    /* The Traits_ alias lets helper macros reference the enclosing traits. */ \
    SETTINGS_TRAITS_NAME::Data::Data(DefaultInitTag) \
    { \
        using Traits_ = SETTINGS_TRAITS_NAME; \
        LIST_OF_SETTINGS_WITHOUT_PATH_MACRO(SETTING_INIT_DEFAULT_, SETTING_INIT_DEFAULT_) \
        LIST_OF_SETTINGS_WITH_PATH_MACRO(SETTING_INIT_DEFAULT_, SETTING_INIT_DEFAULT_) \
    } \
    \
    /* Public default ctor: typed copy from the singleton's canonical default Data. */ \
    SETTINGS_TRAITS_NAME::Data::Data() : Data(SETTINGS_TRAITS_NAME::Accessor::instance().getDefaultData()) {} \
    \
    const SETTINGS_TRAITS_NAME::Accessor & SETTINGS_TRAITS_NAME::Accessor::instance() \
    { \
        using Traits_ = SETTINGS_TRAITS_NAME; \
        using Data = Traits_::Data; \
        /* Singleton pattern: initialize once, return same instance */ \
        static const Accessor the_instance = [] \
        { \
            [[maybe_unused]] constexpr int IMPORTANT = 0x01; \
            [[maybe_unused]] constexpr int HOT_RELOAD = 0x80; \
            Accessor res; \
            /* offsetof on non-standard-layout types is well-defined in Clang */ \
            _Pragma("clang diagnostic push") \
            _Pragma("clang diagnostic ignored \"-Winvalid-offsetof\"") \
            /* Populate field_infos with one entry per setting */ \
            LIST_OF_SETTINGS_WITHOUT_PATH_MACRO(IMPLEMENT_SETTINGS_TRAITS_, IMPLEMENT_SETTINGS_TRAITS_) \
            LIST_OF_SETTINGS_WITH_PATH_MACRO(IMPLEMENT_SETTINGS_TRAITS_WITH_PATH_, IMPLEMENT_SETTINGS_TRAITS_WITH_PATH_) \
            _Pragma("clang diagnostic pop") \
            /* Build name -> index map for fast lookups */ \
            for (size_t i = 0, size = res.field_infos.size(); i < size; ++i) \
            { \
                const auto & info = res.field_infos[i]; \
                res.name_to_index_map.emplace(info.name, i); \
            } \
            return res; \
        }(); \
        return the_instance; \
    } \
    \
    SETTINGS_TRAITS_NAME::Accessor::Accessor() {} \
    \
    size_t SETTINGS_TRAITS_NAME::Accessor::find(std::string_view name) const \
    { \
        auto it = name_to_index_map.find(name); \
        if (it != name_to_index_map.end()) \
            return it->second; \
        return static_cast<size_t>(-1); \
    } \


/// Generate a FieldInfo entry for a setting without a config path.
/// Uses typed-array offsets from the constexpr layout.
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    res.field_infos.emplace_back( \
        FieldInfo\
        { \
            #NAME, \
            "", \
            #TYPE, \
            DESCRIPTION, \
            static_cast<UInt64>(FLAGS), \
            &settingFieldOps<SettingField##TYPE>(), \
            offsetof(Data, TYPE##_) + Traits_::settings_layout_.local_index[static_cast<size_t>(Traits_::SettingID_::NAME)] * sizeof(SettingField##TYPE), \
        });

/// Generate a FieldInfo entry for a setting with a config file path
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS_WITH_PATH_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, PATH, ...) \
    res.field_infos.emplace_back( \
        FieldInfo\
        { \
            #NAME, \
            PATH, \
            #TYPE, \
            DESCRIPTION, \
            static_cast<UInt64>(FLAGS), \
            &settingFieldOps<SettingField##TYPE>(), \
            offsetof(Data, TYPE##_) + Traits_::settings_layout_.local_index[static_cast<size_t>(Traits_::SettingID_::NAME)] * sizeof(SettingField##TYPE), \
        });
}
