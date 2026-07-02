#include <Common/Exception.h>
#include <Common/GetPriorityForLoadBalancing.h>
#include <Common/Priority.h>
#include <Common/thread_local_rng.h>

#include <cmath>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

GetPriorityForLoadBalancing::Func
GetPriorityForLoadBalancing::getPriorityFunc(
    LoadBalancing load_balance,
    size_t offset,
    size_t pool_size,
    size_t least_request_choice_count,
    Float64 least_request_active_request_bias) const
{
    std::function<Priority(size_t index)> get_priority;
    switch (load_balance)
    {
        case LoadBalancing::NEAREST_HOSTNAME:
            if (hostname_prefix_distance.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug: hostname_prefix_distance is not initialized");
            get_priority = [this](size_t i) { return Priority{static_cast<Int64>(hostname_prefix_distance[i])}; };
            break;
        case LoadBalancing::HOSTNAME_LEVENSHTEIN_DISTANCE:
            if (hostname_levenshtein_distance.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug: hostname_levenshtein_distance is not initialized");
            get_priority = [this](size_t i) { return Priority{static_cast<Int64>(hostname_levenshtein_distance[i])}; };
            break;
        case LoadBalancing::HOSTNAME_LONGEST_COMMON_PREFIX:
            if (hostname_longest_common_prefix.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug: hostname_longest_common_prefix is not initialized");
            /// `Priority` is "lower value means higher priority", and the other hostname strategies store a
            /// *distance* (smaller == more similar == preferred) that is returned verbatim. Here we instead
            /// store a *similarity* (the common prefix length, where larger == more similar == preferred), so
            /// we negate it to turn it into a priority. We deliberately do not store a synthetic distance such
            /// as `BIG - length`: negating here keeps the stored vector semantically honest (it really is the
            /// common prefix length) and avoids an arbitrary magic constant.
            get_priority = [this](size_t i) { return Priority{-static_cast<Int64>(hostname_longest_common_prefix[i])}; };
            break;
        case LoadBalancing::HOSTNAME_LONGEST_COMMON_SUFFIX:
            if (hostname_longest_common_suffix.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug: hostname_longest_common_suffix is not initialized");
            /// See the comment for HOSTNAME_LONGEST_COMMON_PREFIX: the stored value is a similarity (common
            /// suffix length, larger == preferred), so we negate it to obtain a priority (lower == preferred).
            get_priority = [this](size_t i) { return Priority{-static_cast<Int64>(hostname_longest_common_suffix[i])}; };
            break;
        case LoadBalancing::IN_ORDER:
            get_priority = [](size_t i) { return Priority{static_cast<Int64>(i)}; };
            break;
        case LoadBalancing::RANDOM:
            break;
        case LoadBalancing::FIRST_OR_RANDOM:
            get_priority = [offset](size_t i) { return i != offset ? Priority{1} : Priority{0}; };
            break;
        case LoadBalancing::LEAST_REQUEST:
        {
            /// Without a source of in-flight request counts (e.g. for ZooKeeper connections)
            /// all pools are considered equally loaded and the choice degenerates to `random`.
            if (!get_active_count)
                break;

            /// "Power of two choices": sample `least_request_choice_count` distinct pools and prefer
            /// the one maximizing 1 / (in_flight_requests + 1)^bias, i.e. the least loaded one
            /// (see the LEAST_REQUEST load balancing policy of Envoy). If `least_request_choice_count`
            /// is greater than or equal to the number of pools, all of them are examined and the
            /// globally least loaded one is preferred.
            /// The winner only gets a better `PoolState::priority`: error counts and config priorities
            /// still take precedence in `PoolState::compare`, so failover semantics are unaffected,
            /// and the remaining pools are still ordered by the random tiebreak.
            const size_t choice_count = std::min(std::max<size_t>(2, least_request_choice_count), pool_size);

            std::vector<size_t> candidates(pool_size);
            std::iota(candidates.begin(), candidates.end(), 0);
            /// Partial Fisher-Yates shuffle: the first `choice_count` elements form a uniform random sample.
            for (size_t i = 0; i + 1 < pool_size && i < choice_count; ++i)
                std::swap(candidates[i], candidates[i + thread_local_rng() % (pool_size - i)]);

            size_t best_index = candidates[0];
            Float64 best_score = 0;
            for (size_t i = 0; i < choice_count; ++i)
            {
                const size_t active_count = get_active_count(candidates[i]);
                const Float64 score = 1.0 / std::pow(static_cast<Float64>(active_count) + 1.0, least_request_active_request_bias);
                /// Strict comparison: ties are broken in favor of the earlier candidate,
                /// which is uniformly random because the sample itself is shuffled.
                if (score > best_score)
                {
                    best_score = score;
                    best_index = candidates[i];
                }
            }

            get_priority = [best_index](size_t i) { return i != best_index ? Priority{1} : Priority{0}; };
            break;
        }
        case LoadBalancing::ROUND_ROBIN:
            /// `last_used` is `std::atomic<size_t>`. Use relaxed ordering: this counter
            /// is only used to derive a rotation index, and there is no happens-before
            /// relationship that needs to be observed by other threads. We compute
            /// `local_last_used` from the pre-increment value so that two concurrent
            /// callers receive distinct rotations of the round-robin sequence.
            auto local_last_used = last_used.fetch_add(1, std::memory_order_relaxed) % pool_size;

            // Example: pool_size = 5
            // | local_last_used | i=0 | i=1 | i=2 | i=3 | i=4 |
            // | 0               | 4   | 0   | 1   | 2   | 3   |
            // | 1               | 3   | 4   | 0   | 1   | 2   |
            // | 2               | 2   | 3   | 4   | 0   | 1   |
            // | 3               | 1   | 2   | 3   | 4   | 0   |
            // | 4               | 0   | 1   | 2   | 3   | 4   |

            get_priority = [pool_size, local_last_used](size_t i)
            {
                size_t priority = pool_size - 1;
                if (i < local_last_used)
                    priority = pool_size - 1 - (local_last_used - i);
                if (i > local_last_used)
                    priority = i - local_last_used - 1;

                return Priority{static_cast<Int64>(priority)};
            };
            break;
    }
    return get_priority;
}

/// Some load balancing strategies (such as "nearest hostname") have preferred nodes to connect to.
/// Usually it's a node in the same data center/availability zone.
/// For other strategies there's no difference between nodes.
bool GetPriorityForLoadBalancing::hasOptimalNode() const
{
    switch (load_balancing)
    {
        case LoadBalancing::NEAREST_HOSTNAME:
            return true;
        case LoadBalancing::HOSTNAME_LEVENSHTEIN_DISTANCE:
            return true;
        case LoadBalancing::HOSTNAME_LONGEST_COMMON_PREFIX:
            return true;
        case LoadBalancing::HOSTNAME_LONGEST_COMMON_SUFFIX:
            return true;
        case LoadBalancing::IN_ORDER:
            return false;
        case LoadBalancing::RANDOM:
            return false;
        case LoadBalancing::FIRST_OR_RANDOM:
            return true;
        case LoadBalancing::ROUND_ROBIN:
            return false;
        case LoadBalancing::LEAST_REQUEST:
            return false;
    }
}

}
