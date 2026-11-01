#pragma once

#include <cstdint>

namespace DB
{

enum class LoadBalancing : uint8_t
{
    /// among replicas with a minimum number of errors selected randomly
    RANDOM = 0,
    /// a replica is selected among the replicas with the minimum number of errors
    /// with the minimum number of distinguished characters in the replica name prefix and local hostname prefix
    NEAREST_HOSTNAME,
    /// just like NEAREST_HOSTNAME, but it count distinguished characters in a levenshtein distance manner
    HOSTNAME_LEVENSHTEIN_DISTANCE,
    /// just like NEAREST_HOSTNAME, but prefers the replica whose hostname shares the longest common prefix with the local hostname
    HOSTNAME_LONGEST_COMMON_PREFIX,
    /// just like NEAREST_HOSTNAME, but prefers the replica whose hostname shares the longest common suffix with the local hostname
    HOSTNAME_LONGEST_COMMON_SUFFIX,
    // replicas with the same number of errors are accessed in the same order
    // as they are specified in the configuration.
    IN_ORDER,
    /// if first replica one has higher number of errors,
    ///   pick a random one from replicas with minimum number of errors
    FIRST_OR_RANDOM,
    // round robin across replicas with the same number of errors.
    ROUND_ROBIN,
};

}
