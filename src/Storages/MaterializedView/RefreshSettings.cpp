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
    DECLARE(Bool, prefer_dependency_replica, false, "When enabled and the view has dependencies (DEPENDS ON), the replica that ran the parent refresh gets priority for running the dependent refresh. Other replicas delay their attempt by prefer_dependency_replica_delay_ms. Useful for SharedMergeTree to avoid replication lag causing missing data in dependent refresh chains.", 0) \
    DECLARE(UInt64, prefer_dependency_replica_delay_ms, 2000, "How long non-preferred replicas wait before attempting to run a dependent refresh when prefer_dependency_replica is enabled. The preferred replica (the one that ran the parent) attempts immediately.", 0) \

DECLARE_SETTINGS_TRAITS(RefreshSettingsTraits, LIST_OF_REFRESH_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(RefreshSettingsTraits, LIST_OF_REFRESH_SETTINGS)

struct RefreshSettingsImpl : public BaseSettings<RefreshSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) RefreshSettings##TYPE NAME = &RefreshSettingsImpl ::NAME;

namespace RefreshSetting
{
LIST_OF_REFRESH_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
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
    if (&other == this)
        return *this;
    *impl = *other.impl;
    return *this;
}

REFRESH_SETTINGS_SUPPORTED_TYPES(RefreshSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void RefreshSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}
}
