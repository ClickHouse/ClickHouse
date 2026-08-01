#include <Common/Exception.h>
#include <Common/GetPriorityForLoadBalancing.h>
#include <Common/Priority.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

GetPriorityForLoadBalancing::Func
GetPriorityForLoadBalancing::getPriorityFunc(LoadBalancing load_balance, size_t offset, size_t pool_size) const
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
        case LoadBalancing::IN_ORDER:
            get_priority = [](size_t i) { return Priority{static_cast<Int64>(i)}; };
            break;
        case LoadBalancing::RANDOM:
            break;
        case LoadBalancing::FIRST_OR_RANDOM:
            get_priority = [offset](size_t i) { return i != offset ? Priority{1} : Priority{0}; };
            break;
        case LoadBalancing::ROUND_ROBIN:
            auto local_last_used = last_used % pool_size;
            ++last_used;

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
        case LoadBalancing::IN_ORDER:
            return false;
        case LoadBalancing::RANDOM:
            return false;
        case LoadBalancing::FIRST_OR_RANDOM:
            return true;
        case LoadBalancing::ROUND_ROBIN:
            return false;
    }
}

}
