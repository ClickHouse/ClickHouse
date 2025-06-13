#pragma once

#include <base/types.h>

namespace DB::S3
{
enum class AZFacilities
{
    ALL,
    MSK,
    GCP
};

/// getRunningAvailabilityZone returns the availability zone of the underlying compute resources where the current process runs.
std::string getRunningAvailabilityZone(bool is_zone_id = false, AZFacilities az_facility = AZFacilities::ALL);
std::string tryGetRunningAvailabilityZone(bool is_zone_id = false, AZFacilities az_facility = AZFacilities::ALL);
}
