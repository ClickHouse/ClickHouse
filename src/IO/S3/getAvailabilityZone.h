#pragma once

#include <string>

namespace DB::S3
{
enum class AZFacilities
{
    ALL,
    MSK,
    CONFLUENT,
    GCP,
    CLICKHOUSE
};

/// getRunningAvailabilityZone returns the availability zone of the underlying compute resources where the current process runs.
std::string getRunningAvailabilityZone(AZFacilities az_facility = AZFacilities::ALL);
std::string tryGetRunningAvailabilityZone(AZFacilities az_facility = AZFacilities::ALL);
}
