#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Storages/MaterializedView/RefreshSettings.h>

namespace DB
{

#define LIST_OF_REFRESH_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Int64, refresh_retries, 2, "How many times to retry refresh query if it fails. If all attempts fail, wait for the next refresh time according to schedule. 0 to disable retries. -1 for infinite retries.", 0) \
    DECLARE(UInt64, refresh_retry_initial_backoff_ms, 100, "Delay before the first retry if refresh query fails (if refresh_retries setting is not zero). Each subsequent retry doubles the delay, up to refresh_retry_max_backoff_ms.", 0) \
    DECLARE(UInt64, refresh_retry_max_backoff_ms, 60'000, "Limit on the exponential growth of delay between refresh attempts, if they keep failing and refresh_retries is positive.", 0) \
    DECLARE(Bool, all_replicas, /* do not change or existing tables will break */ false, "If the materialized view is in a Replicated database, and APPEND is enabled, this flag controls whether all replicas or one replica will refresh.", 0) \

struct RefreshSettingsTraits
{
    struct Data
    {
        SettingFieldInt64 refresh_retries{ 2 };
        SettingFieldUInt64 refresh_retry_initial_backoff_ms{ 100 };
        SettingFieldUInt64 refresh_retry_max_backoff_ms{ 60'000 };
        SettingFieldBool all_replicas{ false };
    };
    class Accessor
    {
    public:
        static const Accessor& instance();
        size_t size() const
        {
            return field_infos.size();
        }
        size_t find(std::string_view name) const;
        const String& getName(size_t index) const
        {
            return field_infos[index].name;
        }
        const char* getTypeName(size_t index) const
        {
            return field_infos[index].type;
        }
        const char* getDescription(size_t index) const
        {
            return field_infos[index].description;
        }
        bool isImportant(size_t index) const
        {
            return field_infos[index].is_important;
        }
        bool isObsolete(size_t index) const
        {
            return field_infos[index].is_obsolete;
        }
        Field castValueUtil(size_t index, const Field& value) const
        {
            return field_infos[index].cast_value_util_function(value);
        }
        String valueToStringUtil(size_t index, const Field& value) const
        {
            return field_infos[index].value_to_string_util_function(value);
        }
        Field stringToValueUtil(size_t index, const String& str) const
        {
            return field_infos[index].string_to_value_util_function(str);
        }
        void setValue(Data& data, size_t index, const Field& value) const
        {
            return field_infos[index].set_value_function(data, value);
        }
        Field getValue(const Data& data, size_t index) const
        {
            return field_infos[index].get_value_function(data);
        }
        void setValueString(Data& data, size_t index, const String& str) const
        {
            return field_infos[index].set_value_string_function(data, str);
        }
        String getValueString(const Data& data, size_t index) const
        {
            return field_infos[index].get_value_string_function(data);
        }
        bool isValueChanged(const Data& data, size_t index) const
        {
            return field_infos[index].is_value_changed_function(data);
        }
        void resetValueToDefault(Data& data, size_t index) const
        {
            return field_infos[index].reset_value_to_default_function(data);
        }
        void writeBinary(const Data& data, size_t index, WriteBuffer& out) const
        {
            return field_infos[index].write_binary_function(data, out);
        }
        void readBinary(Data& data, size_t index, ReadBuffer& in) const
        {
            return field_infos[index].read_binary_function(data, in);
        }
        Field getDefaultValue(size_t index) const
        {
            return field_infos[index].get_default_value_function();
        }
        String getDefaultValueString(size_t index) const
        {
            return field_infos[index].get_default_value_string_function();
        }
    private:
        Accessor();
        struct FieldInfo
        {
            String name;
            const char* type;
            const char* description;
            bool is_important;
            bool is_obsolete;
            Field (* cast_value_util_function)(const Field&);
            String (* value_to_string_util_function)(const Field&);
            Field (* string_to_value_util_function)(const String&);
            void (* set_value_function)(Data&, const Field&);
            Field (* get_value_function)(const Data&);
            void (* set_value_string_function)(Data&, const String&);
            String (* get_value_string_function)(const Data&);
            bool (* is_value_changed_function)(const Data&);
            void (* reset_value_to_default_function)(Data&);
            void (* write_binary_function)(const Data&, WriteBuffer&);
            void (* read_binary_function)(Data&, ReadBuffer&);
            Field (* get_default_value_function)();
            String (* get_default_value_string_function)();
        };
        std::vector<FieldInfo> field_infos;
        std::unordered_map<std::string_view, size_t> name_to_index_map;
    };
    static constexpr bool allow_custom_settings = 0;
    static inline const AliasMap aliases_to_settings = DefineAliases().setName("refresh_retries").setName(
            "refresh_retry_initial_backoff_ms").setName("refresh_retry_max_backoff_ms").setName("all_replicas");
    using SettingsToAliasesMap = std::unordered_map<std::string_view, std::vector<std::string_view>>;
    static inline const SettingsToAliasesMap& settingsToAliases()
    {
        static SettingsToAliasesMap setting_to_aliases_mapping = []
        {
            std::unordered_map<std::string_view, std::vector<std::string_view>> map;
            for (const auto& [alias, destination] : aliases_to_settings)map[destination].push_back(alias);
            return map;
        }();
        return setting_to_aliases_mapping;
    }
    static std::string_view resolveName(std::string_view name)
    {
        if (auto it = aliases_to_settings.find(name);it != aliases_to_settings.end())return it->second;
        return name;
    }
};

const RefreshSettingsTraits::Accessor& RefreshSettingsTraits::Accessor::instance()
{
    static const Accessor the_instance = []
    {
        Accessor res;
        constexpr int IMPORTANT = 0x01;
        UNUSED(IMPORTANT);
        res.field_infos.emplace_back(FieldInfo{ "refresh_retries", "Int64",
                                                "How many times to retry refresh query if it fails. If all attempts fail, wait for the next refresh time according to schedule. 0 to disable retries. -1 for infinite retries.",
                                                (0) & IMPORTANT,
                                                static_cast<bool>((0) & BaseSettingsHelpers::Flags::OBSOLETE),
                                                [](const Field& value) -> Field
                                                { return static_cast<Field>(SettingFieldInt64{ value }); },
                                                [](const Field& value) -> String
                                                { return SettingFieldInt64{ value }.toString(); },
                                                [](const String& str) -> Field
                                                {
                                                    SettingFieldInt64 temp;
                                                    temp.parseFromString(str);
                                                    return static_cast<Field>(temp);
                                                }, [](Data& data, const Field& value)
                                                { data.refresh_retries = value; }, [](const Data& data) -> Field
                                                { return static_cast<Field>(data.refresh_retries); },
                                                [](Data& data, const String& str)
                                                { data.refresh_retries.parseFromString(str); },
                                                [](const Data& data) -> String
                                                { return data.refresh_retries.toString(); },
                                                [](const Data& data) -> bool
                                                { return data.refresh_retries.changed; }, [](Data& data)
                                                { data.refresh_retries = SettingFieldInt64{ 2 }; },
                                                [](const Data& data, WriteBuffer& out)
                                                { data.refresh_retries.writeBinary(out); },
                                                [](Data& data, ReadBuffer& in)
                                                { data.refresh_retries.readBinary(in); }, []() -> Field
                                                { return static_cast<Field>(SettingFieldInt64{ 2 }); }, []() -> String
                                                { return SettingFieldInt64{ 2 }.toString(); }});
        res.field_infos.emplace_back(FieldInfo{ "refresh_retry_initial_backoff_ms", "UInt64",
                                                "Delay before the first retry if refresh query fails (if refresh_retries setting is not zero). Each subsequent retry doubles the delay, up to refresh_retry_max_backoff_ms.",
                                                (0) & IMPORTANT,
                                                static_cast<bool>((0) & BaseSettingsHelpers::Flags::OBSOLETE),
                                                [](const Field& value) -> Field
                                                { return static_cast<Field>(SettingFieldUInt64{ value }); },
                                                [](const Field& value) -> String
                                                { return SettingFieldUInt64{ value }.toString(); },
                                                [](const String& str) -> Field
                                                {
                                                    SettingFieldUInt64 temp;
                                                    temp.parseFromString(str);
                                                    return static_cast<Field>(temp);
                                                }, [](Data& data, const Field& value)
                                                { data.refresh_retry_initial_backoff_ms = value; },
                                                [](const Data& data) -> Field
                                                { return static_cast<Field>(data.refresh_retry_initial_backoff_ms); },
                                                [](Data& data, const String& str)
                                                { data.refresh_retry_initial_backoff_ms.parseFromString(str); },
                                                [](const Data& data) -> String
                                                { return data.refresh_retry_initial_backoff_ms.toString(); },
                                                [](const Data& data) -> bool
                                                { return data.refresh_retry_initial_backoff_ms.changed; },
                                                [](Data& data)
                                                { data.refresh_retry_initial_backoff_ms = SettingFieldUInt64{ 100 }; },
                                                [](const Data& data, WriteBuffer& out)
                                                { data.refresh_retry_initial_backoff_ms.writeBinary(out); },
                                                [](Data& data, ReadBuffer& in)
                                                { data.refresh_retry_initial_backoff_ms.readBinary(in); }, []() -> Field
                                                { return static_cast<Field>(SettingFieldUInt64{ 100 }); },
                                                []() -> String
                                                { return SettingFieldUInt64{ 100 }.toString(); }});
        res.field_infos.emplace_back(FieldInfo{ "refresh_retry_max_backoff_ms", "UInt64",
                                                "Limit on the exponential growth of delay between refresh attempts, if they keep failing and refresh_retries is positive.",
                                                (0) & IMPORTANT,
                                                static_cast<bool>((0) & BaseSettingsHelpers::Flags::OBSOLETE),
                                                [](const Field& value) -> Field
                                                { return static_cast<Field>(SettingFieldUInt64{ value }); },
                                                [](const Field& value) -> String
                                                { return SettingFieldUInt64{ value }.toString(); },
                                                [](const String& str) -> Field
                                                {
                                                    SettingFieldUInt64 temp;
                                                    temp.parseFromString(str);
                                                    return static_cast<Field>(temp);
                                                }, [](Data& data, const Field& value)
                                                { data.refresh_retry_max_backoff_ms = value; },
                                                [](const Data& data) -> Field
                                                { return static_cast<Field>(data.refresh_retry_max_backoff_ms); },
                                                [](Data& data, const String& str)
                                                { data.refresh_retry_max_backoff_ms.parseFromString(str); },
                                                [](const Data& data) -> String
                                                { return data.refresh_retry_max_backoff_ms.toString(); },
                                                [](const Data& data) -> bool
                                                { return data.refresh_retry_max_backoff_ms.changed; }, [](Data& data)
                                                { data.refresh_retry_max_backoff_ms = SettingFieldUInt64{ 60'000 }; },
                                                [](const Data& data, WriteBuffer& out)
                                                { data.refresh_retry_max_backoff_ms.writeBinary(out); },
                                                [](Data& data, ReadBuffer& in)
                                                { data.refresh_retry_max_backoff_ms.readBinary(in); }, []() -> Field
                                                { return static_cast<Field>(SettingFieldUInt64{ 60'000 }); },
                                                []() -> String
                                                { return SettingFieldUInt64{ 60'000 }.toString(); }});
        res.field_infos.emplace_back(FieldInfo{ "all_replicas", "Bool",
                                                "If the materialized view is in a Replicated database, and APPEND is enabled, this flag controls whether all replicas or one replica will refresh.",
                                                (0) & IMPORTANT,
                                                static_cast<bool>((0) & BaseSettingsHelpers::Flags::OBSOLETE),
                                                [](const Field& value) -> Field
                                                { return static_cast<Field>(SettingFieldBool{ value }); },
                                                [](const Field& value) -> String
                                                { return SettingFieldBool{ value }.toString(); },
                                                [](const String& str) -> Field
                                                {
                                                    SettingFieldBool temp;
                                                    temp.parseFromString(str);
                                                    return static_cast<Field>(temp);
                                                }, [](Data& data, const Field& value)
                                                { data.all_replicas = value; }, [](const Data& data) -> Field
                                                { return static_cast<Field>(data.all_replicas); },
                                                [](Data& data, const String& str)
                                                { data.all_replicas.parseFromString(str); },
                                                [](const Data& data) -> String
                                                { return data.all_replicas.toString(); }, [](const Data& data) -> bool
                                                { return data.all_replicas.changed; }, [](Data& data)
                                                { data.all_replicas = SettingFieldBool{ false }; },
                                                [](const Data& data, WriteBuffer& out)
                                                { data.all_replicas.writeBinary(out); }, [](Data& data, ReadBuffer& in)
                                                { data.all_replicas.readBinary(in); }, []() -> Field
                                                { return static_cast<Field>(SettingFieldBool{ false }); },
                                                []() -> String
                                                { return SettingFieldBool{ false }.toString(); }});
        for (size_t i : collections::range(res.field_infos.size()))
        {
            const auto& info = res.field_infos[i];
            res.name_to_index_map.emplace(info.name, i);
        }
        return res;
    }();
    return the_instance;
}
RefreshSettingsTraits::Accessor::Accessor()
{
}
size_t RefreshSettingsTraits::Accessor::find(std::string_view name) const
{
    auto it = name_to_index_map.find(name);
    if (it != name_to_index_map.end())return it->second;
    return static_cast<size_t>(-1);
}
template
class BaseSettings<RefreshSettingsTraits>;

struct RefreshSettingsImpl : public BaseSettings<RefreshSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) RefreshSettings##TYPE NAME = &RefreshSettingsImpl ::NAME;

namespace RefreshSetting
{
LIST_OF_REFRESH_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

RefreshSettings::RefreshSettings() : impl(std::make_unique<RefreshSettingsImpl>())
{
}

RefreshSettings::RefreshSettings(const RefreshSettings & settings) : impl(std::make_unique<RefreshSettingsImpl>(*settings.impl))
{
}

RefreshSettings::RefreshSettings(RefreshSettings && settings) noexcept
    : impl(std::make_unique<RefreshSettingsImpl>(std::move(*settings.impl)))
{
}

RefreshSettings::~RefreshSettings() = default;

RefreshSettings & RefreshSettings::operator=(const RefreshSettings & other)
{
    *impl = *other.impl;
    return *this;
}

REFRESH_SETTINGS_SUPPORTED_TYPES(RefreshSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void RefreshSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}
}
