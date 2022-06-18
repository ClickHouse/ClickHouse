#include "DiskLocal.h"
#include "DiskSelector.h"

#include <IO/WriteHelpers.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

#include <set>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_DISK;
}

DiskSelector::DiskSelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    constexpr auto default_disk_name = "default";
    bool has_default_disk = false;
    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception("Disk name can contain only alphanumeric and '_' (" + disk_name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        if (disk_name == default_disk_name)
            has_default_disk = true;

        auto disk_config_prefix = config_prefix + "." + disk_name;

        disks.emplace(disk_name, factory.create(disk_name, config, disk_config_prefix, context, disks));
    }
    if (!has_default_disk)
    {
        disks.emplace(
            default_disk_name,
            std::make_shared<DiskLocal>(
                default_disk_name, context->getPath(), 0, context, config.getUInt("local_disk_check_period_ms", 0)));
    }
}


DiskSelectorPtr DiskSelector::updateFromConfig(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    std::shared_ptr<DiskSelector> result = std::make_shared<DiskSelector>(*this);

    constexpr auto default_disk_name = "default";
    DisksMap old_disks_minus_new_disks (result->getDisksMap());

    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception("Disk name can contain only alphanumeric and '_' (" + disk_name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        auto disk_config_prefix = config_prefix + "." + disk_name;
        if (!result->getDisksMap().contains(disk_name))
        {
            result->addToDiskMap(disk_name, factory.create(disk_name, config, disk_config_prefix, context, result->getDisksMap()));
        }
        else
        {
            auto disk = old_disks_minus_new_disks[disk_name];

            disk->applyNewSettings(config, context, disk_config_prefix, result->getDisksMap());

            old_disks_minus_new_disks.erase(disk_name);
        }
    }

    old_disks_minus_new_disks.erase(default_disk_name);

    if (!old_disks_minus_new_disks.empty())
    {
        WriteBufferFromOwnString warning;
        if (old_disks_minus_new_disks.size() == 1)
            writeString("Disk ", warning);
        else
            writeString("Disks ", warning);

        int index = 0;
        for (const auto & [name, _] : old_disks_minus_new_disks)
        {
            if (index++ > 0)
                writeString(", ", warning);
            writeBackQuotedString(name, warning);
        }

        writeString(" disappeared from configuration, this change will be applied after restart of ClickHouse", warning);
        LOG_WARNING(&Poco::Logger::get("DiskSelector"), fmt::runtime(warning.str()));
    }

    return result;
}


DiskPtr DiskSelector::get(const String & name) const
{
    auto it = disks.find(name);
    if (it == disks.end())
        throw Exception("Unknown disk " + name, ErrorCodes::UNKNOWN_DISK);
    return it->second;
}

}
