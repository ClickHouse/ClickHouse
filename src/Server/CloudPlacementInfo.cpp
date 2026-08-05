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

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

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

    String loadRegionFromFile(const Poco::Util::AbstractConfiguration & config)
    {
        auto region_file = config.getString(getConfigPath("region_from_file"), DEFAULT_REGION_FILE_PATH);

        if (!std::filesystem::exists(region_file))
            return "";

        String region_from_file;

        ReadBufferFromFile in(region_file);
        readStringUntilEOF(region_from_file, in);
        Poco::trimInPlace(region_from_file);

        return region_from_file;
    }
}


PlacementInfo & PlacementInfo::instance()
{
    static PlacementInfo instance;
    return instance;
}

void PlacementInfo::initialize(const Poco::Util::AbstractConfiguration & config)
try
{
    if (initialized)
        return;

    if (!config.has(DB::PlacementInfo::PLACEMENT_CONFIG_PREFIX))
    {
        availability_zone = "";
        region = "";
        initialized = true;
        return;
    }

    use_imds = config.getBool(getConfigPath("use_imds"), false);

    if (use_imds)
    {
        availability_zone = S3::getRunningAvailabilityZone();
        region = "";
    }
    else
    {
        availability_zone = config.getString(getConfigPath("availability_zone"), "");
        region = config.getString(getConfigPath("region"), "");

        if (availability_zone.empty())
        {
            availability_zone = loadAvailabilityZoneFromFile(config);
            if (availability_zone.empty())
                LOG_WARNING(log, "Availability zone info not found");
        }

        if (region.empty())
            region = loadRegionFromFile(config);
    }

    LOG_DEBUG(log, "Loaded info: availability_zone: {}, region: {}", availability_zone, region);
    initialized = true;
}
catch (...)
{
    tryLogCurrentException("Failed to get availability zone");
    availability_zone = "";
    initialized = true;
}

std::string PlacementInfo::getAvailabilityZone() const
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Placement info has not been loaded");

    return availability_zone;
}

std::string PlacementInfo::getRegion() const
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Placement info has not been loaded");

    return region;
}

}
}
