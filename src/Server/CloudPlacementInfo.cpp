#include <Server/CloudPlacementInfo.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Credentials.h>
#include <Poco/String.h>
#include <fmt/core.h>
#include <filesystem>


namespace DB
{

namespace PlacementInfo
{

namespace
{
    std::string getConfigPath(std::string_view path)
    {
        return fmt::format("{}.{}", PLACEMENT_CONFIG_PREFIX, path);
    }

    String loadAvailabilityZoneFromFile(const Poco::Util::AbstractConfiguration & config)
    {
        auto az_file = config.getString(getConfigPath("availability_zone_from_file"), DEFAULT_AZ_FILE_PATH);

        if (!std::filesystem::exists(az_file))
            return "";

        String availability_zone_from_file;

        ReadBufferFromFile in(az_file);
        readStringUntilEOF(availability_zone_from_file, in);
        Poco::trimInPlace(availability_zone_from_file);

        return availability_zone_from_file;
    }
}


PlacementInfo & PlacementInfo::instance()
{
    static PlacementInfo instance;
    return instance;
}

void PlacementInfo::initialize(const Poco::Util::AbstractConfiguration & config)
{
    use_imds = config.getBool(getConfigPath("use_imds"), false);

    if (use_imds)
    {
        availability_zone = S3::getRunningAvailabilityZone();
    }
    else
    {
        availability_zone = config.getString(getConfigPath("availability_zone"), "");

        if (availability_zone.empty())
            availability_zone = loadAvailabilityZoneFromFile(config);

        if (availability_zone.empty())
            LOG_WARNING(log, "Availability zone info not found");
    }

    LOG_DEBUG(log, "Loaded info: availability_zone: {}", availability_zone);
    initialized = true;
}

std::string PlacementInfo::getAvailabilityZone() const
{
    if (!initialized)
    {
        LOG_WARNING(log, "Placement info has not been loaded");
        return "";
    }

    return availability_zone;
}

}
}
