#include <Common/GetPriorityForLoadBalancing.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::function<size_t(size_t index)> GetPriorityForLoadBalancing::getPriorityFunc(LoadBalancing load_balance, size_t offset, size_t pool_size) const
{
    std::function<size_t(size_t index)> get_priority;
    switch (load_balance)
    {
        case LoadBalancing::NEAREST_HOSTNAME:
            if (hostname_differences.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug: hostname_differences is not initialized");
            get_priority = [&](size_t i) { return hostname_differences[i]; };
            break;
        case LoadBalancing::IN_ORDER:
            get_priority = [](size_t i) { return i; };
            break;
        case LoadBalancing::RANDOM:
            break;
        case LoadBalancing::FIRST_OR_RANDOM:
            get_priority = [offset](size_t i) -> size_t { return i != offset; };
            break;
        case LoadBalancing::ROUND_ROBIN:
            if (last_used >= pool_size)
                last_used = 0;
            ++last_used;
            /* Consider pool_size equals to 5
             * last_used = 1 -> get_priority: 0 1 2 3 4
             * last_used = 2 -> get_priority: 4 0 1 2 3
             * last_used = 3 -> get_priority: 4 3 0 1 2
             * ...
             * */
            get_priority = [&](size_t i)
            {
                ++i;
                return i < last_used ? pool_size - i : i - last_used;
            };
            break;
    }
    return get_priority;
}

}
