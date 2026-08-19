#include <Disks/loadLocalDiskConfig.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>
#include <Disks/DiskLocal.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

void loadDiskLocalConfig(const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      String & path,
                      UInt64 & keep_free_space_bytes)
{
    path = config.getString(config_prefix + ".path", "");
    if (name == "default")
    {
        if (!path.empty())
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                "\"default\" disk path should be provided in <path> not it <storage_configuration>");
        path = context->getPath();
    }
    else
    {
        if (path.empty())
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Disk path can not be empty. Disk {}", name);
        if (path.back() != '/')
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Disk path must end with /. Disk {}", name);
        if (path == context->getPath())
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Disk path ('{}') cannot be equal to <path>. Use <default> disk instead.", path);
    }

    bool has_space_ratio = config.has(config_prefix + ".keep_free_space_ratio");

    if (config.has(config_prefix + ".keep_free_space_bytes") && has_space_ratio)
        throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                        "Only one of 'keep_free_space_bytes' and 'keep_free_space_ratio' can be specified");

    keep_free_space_bytes = config.getUInt64(config_prefix + ".keep_free_space_bytes", 0);

    if (has_space_ratio)
    {
        auto ratio = config.getDouble(config_prefix + ".keep_free_space_ratio");
        if (ratio < 0 || ratio > 1)
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "'keep_free_space_ratio' have to be between 0 and 1");
        String tmp_path = path;
        if (tmp_path.empty())
            tmp_path = context->getPath();

        // Create tmp disk for getting total disk space.
        keep_free_space_bytes = static_cast<UInt64>(*DiskLocal("tmp", tmp_path, 0, config, config_prefix).getTotalSpace() * ratio);
    }
}

}
