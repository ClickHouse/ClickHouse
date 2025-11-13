#pragma once
#include <base/StringRef.h>

namespace DB
{

/// See https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0919r3.html
struct StringHashForHeterogeneousLookup
{
    using hash_type = std::hash<std::string_view>;
    using transparent_key_equal = std::equal_to<>;
    using is_transparent = void; // required to make find() work with different type than key_type

    auto operator()(const std::string_view view) const
    {
        return hash_type()(view);
    }

    auto operator()(const std::string & str) const
    {
        return hash_type()(str);
    }

    auto operator()(const char * data) const
    {
        return hash_type()(data);
    }
};

}
