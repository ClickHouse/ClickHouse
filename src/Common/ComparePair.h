#pragma once

namespace DB
{
    struct ComparePair final
    {
        template <typename T1, typename T2>
        bool operator()(const std::pair<T1, T2> & lhs, const std::pair<T1, T2> & rhs) const
        {
            return lhs.first == rhs.first ? lhs.second < rhs.second : lhs.first < rhs.first;
        }
    };
}
