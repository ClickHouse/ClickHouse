#pragma once

#include <Core/SettingsFields.h>
#include <Common/SettingsChanges.h>
#include <base/range.h>
#include <boost/blank.hpp>
#include <unordered_map>
#include <boost/program_options/options_description.hpp>


namespace boost::program_options
{
    class options_description;
}

namespace DB
{

class ReadBuffer;
class WriteBuffer;

enum class SettingsWriteFormat
{
    BINARY,             /// Part of the settings are serialized as strings, and other part as variants. This is the old behaviour.
    STRINGS_WITH_FLAGS, /// All settings are serialized as strings. Before each value the flag `is_important` is serialized.
    DEFAULT = STRINGS_WITH_FLAGS,
};

/** Template class to define collections of settings.
  * Example of usage:
  *
  * mysettings.h:
  * #define APPLY_FOR_MYSETTINGS(M) \
  *     M(UInt64, a, 100, "Description of a", 0) \
  *     M(Float, f, 3.11, "Description of f", IMPORTANT) // IMPORTANT - means the setting can't be ignored by older versions) \
  *     M(String, s, "default", "Description of s", 0)
  *
  * DECLARE_SETTINGS_TRAITS(MySettingsTraits, APPLY_FOR_MYSETTINGS)

  * struct MySettings : public BaseSettings<MySettingsTraits>
  * {
  * };
  *
  * mysettings.cpp:
  * IMPLEMENT_SETTINGS_TRAITS(MySettingsTraits, APPLY_FOR_MYSETTINGS)
  */
template <class TTraits>
class BaseSettings : public TTraits::Data
{
    using CustomSettingMap = std::unordered_map<std::string_view, std::pair<std::shared_ptr<const String>, SettingFieldCustom>>;
public:
    BaseSettings() = default;
    BaseSettings(const BaseSettings &) = default;
    BaseSettings(BaseSettings &&) noexcept = default;
    BaseSettings & operator=(const BaseSettings &) = default;
    BaseSettings & operator=(BaseSettings &&) noexcept = default;
    virtual ~BaseSettings() = default;

    using Traits = TTraits;

    virtual void set(std::string_view name, const Field & value);
    Field get(std::string_view name) const;

    void setString(std::string_view name, const String & value);
    String getString(std::string_view name) const;

    bool tryGet(std::string_view name, Field & value) const;
    bool tryGetString(std::string_view name, String & value) const;

    bool isChanged(std::string_view name) const;
    SettingsChanges changes() const;
    void applyChange(const SettingChange & change);
    void applyChanges(const SettingsChanges & changes);
    void applyChanges(const BaseSettings & changes); /// NOLINT

    /// Resets all the settings to their default values.
    void resetToDefault();
    /// Resets specified setting to its default value.
    void resetToDefault(std::string_view name);

    bool has(std::string_view name) const { return hasBuiltin(name) || hasCustom(name); }
    static bool hasBuiltin(std::string_view name);
    bool hasCustom(std::string_view name) const;

    const char * getTypeName(std::string_view name) const;
    const char * getDescription(std::string_view name) const;

    /// Checks if it's possible to assign a field to a specified value and throws an exception if not.
    /// This function doesn't change the fields, it performs check only.
    static void checkCanSet(std::string_view name, const Field & value);
    static void checkCanSetString(std::string_view name, const String & str);

    /// Conversions without changing the fields.
    static Field castValueUtil(std::string_view name, const Field & value);
    static String valueToStringUtil(std::string_view name, const Field & value);
    static Field stringToValueUtil(std::string_view name, const String & str);

    void write(WriteBuffer & out, SettingsWriteFormat format = SettingsWriteFormat::DEFAULT) const;
    void read(ReadBuffer & in, SettingsWriteFormat format = SettingsWriteFormat::DEFAULT);

    // A debugging aid.
    std::string toString() const;

    /// Represents a reference to a setting field.
    class SettingFieldRef
    {
    public:
        const String & getName() const;
        Field getValue() const;
        String getValueString() const;
        bool isValueChanged() const;
        const char * getTypeName() const;
        const char * getDescription() const;
        bool isCustom() const;
        bool isObsolete() const;

        bool operator==(const SettingFieldRef & other) const { return (getName() == other.getName()) && (getValue() == other.getValue()); }
        bool operator!=(const SettingFieldRef & other) const { return !(*this == other); }

    private:
        friend class BaseSettings;
        const BaseSettings * settings;
        const typename Traits::Accessor * accessor;
        size_t index;
        std::conditional_t<Traits::allow_custom_settings, const CustomSettingMap::mapped_type*, boost::blank> custom_setting;
    };

    /// Adds program options to set the settings from a command line.
    /// (Don't forget to call notify() on the `variables_map` after parsing it!)
    void addProgramOptions(boost::program_options::options_description & options);

    /// Adds program options as to set the settings from a command line.
    /// Allows to set one setting multiple times, the last value will be used.
    /// (Don't forget to call notify() on the `variables_map` after parsing it!)
    void addProgramOptionsAsMultitokens(boost::program_options::options_description & options);

    void addProgramOption(boost::program_options::options_description & options, const SettingFieldRef & field);
    void addProgramOptionAsMultitoken(boost::program_options::options_description & options, const SettingFieldRef & field);

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

        bool operator ==(const Iterator & other) const;
        bool operator !=(const Iterator & other) const { return !(*this == other); }

    private:
        friend class BaseSettings;
        Iterator(const BaseSettings & settings_, const typename Traits::Accessor & accessor_, SkipFlags skip_flags_);
        void doSkip();
        void setPointerToCustomSetting();

        SettingFieldRef field_ref;
        std::conditional_t<Traits::allow_custom_settings, CustomSettingMap::const_iterator, boost::blank> custom_settings_iterator;
        SkipFlags skip_flags;
    };

    class Range
    {
    public:
        Range(const BaseSettings & settings_, SkipFlags skip_flags_) : settings(settings_), accessor(Traits::Accessor::instance()), skip_flags(skip_flags_) {}
        Iterator begin() const { return Iterator(settings, accessor, skip_flags); }
        Iterator end() const { return Iterator(settings, accessor, SKIP_ALL); }

    private:
        const BaseSettings & settings;
        const typename Traits::Accessor & accessor;
        SkipFlags skip_flags;
    };

    Range all(SkipFlags skip_flags = SKIP_NONE) const { return Range{*this, skip_flags}; }
    Range allChanged() const { return all(SKIP_UNCHANGED); }
    Range allUnchanged() const { return all(SKIP_CHANGED); }
    Range allBuiltin() const { return all(SKIP_CUSTOM); }
    Range allCustom() const { return all(SKIP_BUILTIN); }

    Iterator begin() const { return allChanged().begin(); }
    Iterator end() const { return allChanged().end(); }

private:
    SettingFieldCustom & getCustomSetting(std::string_view name);
    const SettingFieldCustom & getCustomSetting(std::string_view name) const;
    const SettingFieldCustom * tryGetCustomSetting(std::string_view name) const;

    std::conditional_t<Traits::allow_custom_settings, CustomSettingMap, boost::blank> custom_settings_map;
};

struct BaseSettingsHelpers
{
    [[noreturn]] static void throwSettingNotFound(std::string_view name);
    static void warningSettingNotFound(std::string_view name);

    static void writeString(std::string_view str, WriteBuffer & out);
    static String readString(ReadBuffer & in);

    enum Flags : UInt64
    {
        IMPORTANT = 0x01,
        CUSTOM = 0x02,
        OBSOLETE = 0x04,
    };
    static void writeFlags(Flags flags, WriteBuffer & out);
    static Flags readFlags(ReadBuffer & in);
};

template <typename TTraits>
void BaseSettings<TTraits>::set(std::string_view name, const Field & value)
{
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        accessor.setValue(*this, index, value);
    else
        getCustomSetting(name) = value;
}

template <typename TTraits>
Field BaseSettings<TTraits>::get(std::string_view name) const
{
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.getValue(*this, index);
    else
        return static_cast<Field>(getCustomSetting(name));
}

template <typename TTraits>
void BaseSettings<TTraits>::setString(std::string_view name, const String & value)
{
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        accessor.setValueString(*this, index, value);
    else
        getCustomSetting(name).parseFromString(value);
}

template <typename TTraits>
String BaseSettings<TTraits>::getString(std::string_view name) const
{
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.getValueString(*this, index);
    else
        return getCustomSetting(name).toString();
}

template <typename TTraits>
bool BaseSettings<TTraits>::tryGet(std::string_view name, Field & value) const
{
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
bool BaseSettings<TTraits>::tryGetString(std::string_view name, String & value) const
{
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
    {
        value = accessor.getValueString(*this, index);
        return true;
    }
    if (const auto * custom_setting = tryGetCustomSetting(name))
    {
        value = custom_setting->toString();
        return true;
    }
    return false;
}

template <typename TTraits>
bool BaseSettings<TTraits>::isChanged(std::string_view name) const
{
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
void BaseSettings<TTraits>::applyChanges(const BaseSettings & other_settings)
{
    for (const auto & field : other_settings)
        set(field.getName(), field.getValue());
}

template <typename TTraits>
void BaseSettings<TTraits>::resetToDefault()
{
    const auto & accessor = Traits::Accessor::instance();
    for (size_t i : collections::range(accessor.size()))
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
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        accessor.resetValueToDefault(*this, index);
}

template <typename TTraits>
bool BaseSettings<TTraits>::hasBuiltin(std::string_view name)
{
    const auto & accessor = Traits::Accessor::instance();
    return (accessor.find(name) != static_cast<size_t>(-1));
}

template <typename TTraits>
bool BaseSettings<TTraits>::hasCustom(std::string_view name) const
{
    return tryGetCustomSetting(name);
}

template <typename TTraits>
const char * BaseSettings<TTraits>::getTypeName(std::string_view name) const
{
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.getTypeName(index);
    else if (tryGetCustomSetting(name))
        return "Custom";
    else
        BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
const char * BaseSettings<TTraits>::getDescription(std::string_view name) const
{
    const auto & accessor = Traits::Accessor::instance();
    if (size_t index = accessor.find(name); index != static_cast<size_t>(-1))
        return accessor.getDescription(index);
    else if (tryGetCustomSetting(name))
        return "Custom";
    else
        BaseSettingsHelpers::throwSettingNotFound(name);
}

template <typename TTraits>
void BaseSettings<TTraits>::checkCanSet(std::string_view name, const Field & value)
{
    castValueUtil(name, value);
}

template <typename TTraits>
void BaseSettings<TTraits>::checkCanSetString(std::string_view name, const String & str)
{
    stringToValueUtil(name, str);
}

template <typename TTraits>
Field BaseSettings<TTraits>::castValueUtil(std::string_view name, const Field & value)
{
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

    for (auto field : *this)
    {
        bool is_custom = field.isCustom();
        bool is_important = !is_custom && accessor.isImportant(field.index);

        BaseSettingsHelpers::writeString(field.getName(), out);

        if ((format >= SettingsWriteFormat::STRINGS_WITH_FLAGS) || is_custom)
        {
            using Flags = BaseSettingsHelpers::Flags;
            Flags flags{0};
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
void BaseSettings<TTraits>::read(ReadBuffer & in, SettingsWriteFormat format)
{
    resetToDefault();
    const auto & accessor = Traits::Accessor::instance();
    while (true)
    {
        String name = BaseSettingsHelpers::readString(in);
        if (name.empty() /* empty string is a marker of the end of settings */)
            break;
        size_t index = accessor.find(name);

        using Flags = BaseSettingsHelpers::Flags;
        Flags flags{0};
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
}

template <typename TTraits>
String BaseSettings<TTraits>::toString() const
{
    String res;
    for (const auto & field : *this)
    {
        if (!res.empty())
            res += ", ";
        res += field.getName() + " = " + field.getValueString();
    }
    return res;
}

template <typename TTraits>
void BaseSettings<TTraits>::addProgramOptions(boost::program_options::options_description & options)
{
    for (const auto & field : all())
        addProgramOption(options, field);
}

template <typename TTraits>
void BaseSettings<TTraits>::addProgramOptionsAsMultitokens(boost::program_options::options_description & options)
{
    for (const auto & field : all())
        addProgramOptionAsMultitoken(options, field);
}

template <typename TTraits>
void BaseSettings<TTraits>::addProgramOption(boost::program_options::options_description & options, const SettingFieldRef & field)
{
    const std::string_view name = field.getName();
    auto on_program_option = boost::function1<void, const std::string &>([this, name](const std::string & value) { set(name, value); });
    options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
        name.data(), boost::program_options::value<std::string>()->composing()->notifier(on_program_option), field.getDescription())));
}

template <typename TTraits>
void BaseSettings<TTraits>::addProgramOptionAsMultitoken(boost::program_options::options_description & options, const SettingFieldRef & field)
{
    const std::string_view name = field.getName();
    auto on_program_option = boost::function1<void, const Strings &>([this, name](const Strings & values) { set(name, values.back()); });
    options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
        name.data(), boost::program_options::value<Strings>()->multitoken()->composing()->notifier(on_program_option), field.getDescription())));
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
        {
            auto new_name = std::make_shared<String>(name);
            it = custom_settings_map.emplace(*new_name, std::make_pair(new_name, SettingFieldCustom{})).first;
        }
        return it->second.second;
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
            return it->second.second;
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
            return &it->second.second;
    }
    return nullptr;
}

template <typename TTraits>
BaseSettings<TTraits>::Iterator::Iterator(const BaseSettings & settings_, const typename Traits::Accessor & accessor_, SkipFlags skip_flags_)
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
            field_ref.custom_setting = &custom_settings_iterator->second;
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
            return *custom_setting->first;
    }
    return accessor->getName(index);
}

template <typename TTraits>
Field BaseSettings<TTraits>::SettingFieldRef::getValue() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return static_cast<Field>(custom_setting->second);
    }
    return accessor->getValue(*settings, index);
}

template <typename TTraits>
String BaseSettings<TTraits>::SettingFieldRef::getValueString() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return custom_setting->second.toString();
    }
    return accessor->getValueString(*settings, index);
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
const char * BaseSettings<TTraits>::SettingFieldRef::getTypeName() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return "Custom";
    }
    return accessor->getTypeName(index);
}

template <typename TTraits>
const char * BaseSettings<TTraits>::SettingFieldRef::getDescription() const
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
bool BaseSettings<TTraits>::SettingFieldRef::isObsolete() const
{
    if constexpr (Traits::allow_custom_settings)
    {
        if (custom_setting)
            return false;
    }
    return accessor->isObsolete(index);
}

/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO) \
    DECLARE_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, 0)

/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS_ALLOW_CUSTOM_SETTINGS(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO) \
    DECLARE_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, 1)

/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS_COMMON(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, ALLOW_CUSTOM_SETTINGS) \
    struct SETTINGS_TRAITS_NAME \
    { \
        struct Data \
        { \
            LIST_OF_SETTINGS_MACRO(DECLARE_SETTINGS_TRAITS_) \
        }; \
        \
        class Accessor \
        { \
        public: \
            static const Accessor & instance(); \
            size_t size() const { return field_infos.size(); } \
            size_t find(std::string_view name) const; \
            const String & getName(size_t index) const { return field_infos[index].name; } \
            const char * getTypeName(size_t index) const { return field_infos[index].type; } \
            const char * getDescription(size_t index) const { return field_infos[index].description; } \
            bool isImportant(size_t index) const { return field_infos[index].is_important; } \
            bool isObsolete(size_t index) const { return field_infos[index].is_obsolete; } \
            Field castValueUtil(size_t index, const Field & value) const { return field_infos[index].cast_value_util_function(value); } \
            String valueToStringUtil(size_t index, const Field & value) const { return field_infos[index].value_to_string_util_function(value); } \
            Field stringToValueUtil(size_t index, const String & str) const { return field_infos[index].string_to_value_util_function(str); } \
            void setValue(Data & data, size_t index, const Field & value) const { return field_infos[index].set_value_function(data, value); } \
            Field getValue(const Data & data, size_t index) const { return field_infos[index].get_value_function(data); } \
            void setValueString(Data & data, size_t index, const String & str) const { return field_infos[index].set_value_string_function(data, str); } \
            String getValueString(const Data & data, size_t index) const { return field_infos[index].get_value_string_function(data); } \
            bool isValueChanged(const Data & data, size_t index) const { return field_infos[index].is_value_changed_function(data); } \
            void resetValueToDefault(Data & data, size_t index) const { return field_infos[index].reset_value_to_default_function(data); } \
            void writeBinary(const Data & data, size_t index, WriteBuffer & out) const { return field_infos[index].write_binary_function(data, out); } \
            void readBinary(Data & data, size_t index, ReadBuffer & in) const { return field_infos[index].read_binary_function(data, in); } \
        \
        private: \
            Accessor(); \
            struct FieldInfo \
            { \
                String name; \
                const char * type; \
                const char * description; \
                bool is_important; \
                bool is_obsolete; \
                Field (*cast_value_util_function)(const Field &); \
                String (*value_to_string_util_function)(const Field &); \
                Field (*string_to_value_util_function)(const String &); \
                void (*set_value_function)(Data &, const Field &) ; \
                Field (*get_value_function)(const Data &) ; \
                void (*set_value_string_function)(Data &, const String &) ; \
                String (*get_value_string_function)(const Data &) ; \
                bool (*is_value_changed_function)(const Data &); \
                void (*reset_value_to_default_function)(Data &) ; \
                void (*write_binary_function)(const Data &, WriteBuffer &) ; \
                void (*read_binary_function)(Data &, ReadBuffer &) ; \
            }; \
            std::vector<FieldInfo> field_infos; \
            std::unordered_map<std::string_view, size_t> name_to_index_map; \
        }; \
        static constexpr bool allow_custom_settings = ALLOW_CUSTOM_SETTINGS; \
    };

/// NOLINTNEXTLINE
#define DECLARE_SETTINGS_TRAITS_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    SettingField##TYPE NAME {DEFAULT};

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO) \
    const SETTINGS_TRAITS_NAME::Accessor & SETTINGS_TRAITS_NAME::Accessor::instance() \
    { \
        static const Accessor the_instance = [] \
        { \
            Accessor res; \
            constexpr int IMPORTANT = 0x01; \
            UNUSED(IMPORTANT); \
            LIST_OF_SETTINGS_MACRO(IMPLEMENT_SETTINGS_TRAITS_) \
            for (size_t i : collections::range(res.field_infos.size())) \
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
    \
    template class BaseSettings<SETTINGS_TRAITS_NAME>;

//-V:IMPLEMENT_SETTINGS:501
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    res.field_infos.emplace_back( \
        FieldInfo{#NAME, #TYPE, DESCRIPTION, (FLAGS) & IMPORTANT, \
            static_cast<bool>((FLAGS) & BaseSettingsHelpers::Flags::OBSOLETE), \
            [](const Field & value) -> Field { return static_cast<Field>(SettingField##TYPE{value}); }, \
            [](const Field & value) -> String { return SettingField##TYPE{value}.toString(); }, \
            [](const String & str) -> Field { SettingField##TYPE temp; temp.parseFromString(str); return static_cast<Field>(temp); }, \
            [](Data & data, const Field & value) { data.NAME = value; }, \
            [](const Data & data) -> Field { return static_cast<Field>(data.NAME); }, \
            [](Data & data, const String & str) { data.NAME.parseFromString(str); }, \
            [](const Data & data) -> String { return data.NAME.toString(); }, \
            [](const Data & data) -> bool { return data.NAME.changed; }, \
            [](Data & data) { data.NAME = SettingField##TYPE{DEFAULT}; }, \
            [](const Data & data, WriteBuffer & out) { data.NAME.writeBinary(out); }, \
            [](Data & data, ReadBuffer & in) { data.NAME.readBinary(in); } \
        });
}
