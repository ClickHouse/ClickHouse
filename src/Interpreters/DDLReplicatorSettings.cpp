#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Interpreters/DDLReplicatorSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}

#define LIST_OF_DDL_REPLICATOR_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, max_replication_lag_to_enqueue, 50, "Replica will throw exception on attempt to execute query if its replication lag greater", 0) \
    DECLARE(UInt64, wait_entry_commited_timeout_sec, 3600, "Replicas will try to cancel query if timeout exceed, but initiator host has not executed it yet", 0) \
    DECLARE(Bool, check_consistency, true, "Check consistency of local metadata and metadata in Keeper, do replica recovery on inconsistency", 0) \
    DECLARE(UInt64, max_retries_before_automatic_recovery, 10, "Max number of attempts to execute a queue entry before marking replica as lost recovering it from snapshot (0 means infinite)", 0) \
    DECLARE(NonZeroUInt64, logs_to_keep, 1000, "Default number of logs to keep in ZooKeeper for DDL replication.", 0) \

DECLARE_SETTINGS_TRAITS(DDLReplicatorSettingsTraits, LIST_OF_DDL_REPLICATOR_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DDLReplicatorSettingsTraits, LIST_OF_DDL_REPLICATOR_SETTINGS)

struct DDLReplicatorSettingsImpl : public BaseSettings<DDLReplicatorSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    DDLReplicatorSettings##TYPE NAME = &DDLReplicatorSettingsImpl ::NAME;

namespace DDLReplicatorSetting
{
LIST_OF_DDL_REPLICATOR_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

DDLReplicatorSettings::DDLReplicatorSettings() : impl(std::make_unique<DDLReplicatorSettingsImpl>())
{
}

DDLReplicatorSettings::DDLReplicatorSettings(const DDLReplicatorSettings & settings)
    : impl(std::make_unique<DDLReplicatorSettingsImpl>(*settings.impl))
{
}

DDLReplicatorSettings::DDLReplicatorSettings(DDLReplicatorSettings && settings) noexcept
    : impl(std::make_unique<DDLReplicatorSettingsImpl>(std::move(*settings.impl)))
{
}

DDLReplicatorSettings::~DDLReplicatorSettings() = default;

DDL_REPLICATOR_SETTINGS_SUPPORTED_TYPES(DDLReplicatorSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

bool DDLReplicatorSettings::hasBuiltin(std::string_view name)
{
    return DDLReplicatorSettingsImpl::hasBuiltin(name);
}

void DDLReplicatorSettings::applyChange(const SettingChange & change)
{
    impl->applyChange(change);
}

void DDLReplicatorSettings::set(const String & name, const String & value)
{
    impl->set(name, value);
}

String DDLReplicatorSettings::toString() const
{
    return impl->toString();
}
}
