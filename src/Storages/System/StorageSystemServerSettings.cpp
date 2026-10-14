#include <Core/ServerSettings.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/ServerSettingColumnsParams.h>
#include <Storages/System/StorageSystemServerSettings.h>

#include <fmt/ranges.h>

namespace DB
{

ColumnsDescription StorageSystemServerSettings::getColumnsDescription()
{
    auto changeable_without_restart_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"No",              static_cast<Int8>(ServerSettings::ChangeableWithoutRestart::No)},
            {"IncreaseOnly",    static_cast<Int8>(ServerSettings::ChangeableWithoutRestart::IncreaseOnly)},
            {"DecreaseOnly",    static_cast<Int8>(ServerSettings::ChangeableWithoutRestart::DecreaseOnly)},
            {"Yes",             static_cast<Int8>(ServerSettings::ChangeableWithoutRestart::Yes)},
        });

    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Server setting name."},
        {"value", std::make_shared<DataTypeString>(), "Server setting value."},
        {"default", std::make_shared<DataTypeString>(), "Server setting default value."},
        {"changed", std::make_shared<DataTypeUInt8>(), "Shows whether a setting was specified in config.xml"},
        {"description", std::make_shared<DataTypeString>(), "Short server setting description."},
        {"type", std::make_shared<DataTypeString>(), "Server setting value type."},
        {"changeable_without_restart", std::move(changeable_without_restart_type), "Shows whether a setting can be changed at runtime."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."}
    };
}

void StorageSystemServerSettings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & config = context->getConfigRef();
    ServerSettings settings;
    settings.loadSettingsFromConfig(config);

    /// Fill in the setting value dynamically.
    if (zkutil::hasZooKeeperConfig(config))
    {
        zkutil::ZooKeeperArgs args(config, zkutil::getZooKeeperConfigName(config));
        settings.set("keeper_hosts", fmt::format("{}", fmt::join(args.hosts, ",")));
    }

    ServerSettingColumnsParams params{res_columns, context};
    settings.dumpToSystemServerSettingsColumns(params);
}

}
