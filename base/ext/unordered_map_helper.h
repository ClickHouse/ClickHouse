#pragma once

#include <type_traits>
#include <string_view>
#include <version>

/** Helpers that allow using std::string_view on STL associative containers keyed by std::string.
 *
 *  based on https://stackoverflow.com/a/64101153
 *
 *  For the reference see:
 *  http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2013/n3657.htm
 *  http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n4296.pdf (23.2.4 [associative.reqmts] para 13)
 */

namespace ext
{
    struct string_equal
    {
        using is_transparent = std::true_type;

        bool operator()(std::string_view l, std::string_view r) const noexcept
        {
            return l == r;
        }
    };

    struct string_hash
    {
        using is_transparent = std::true_type;

        auto operator()(std::string_view str) const noexcept
        {
            return std::hash<std::string_view>()(str);
        }
    };

    inline auto heterogeneousKey(std::string_view key)
    {
        // `__cpp_lib_transparent_operators` is a portable way to check for transparent operators feature in STL,
        // however it doesn't always works in practice.
        // So to fix 'unbundled' builds with some old versions of STL, we explicitly rely on `_LIBCPP_STD_VER` macro.
        // Which enables specific `find` overload in bundled version of STL, e.g. @ contrib/libcxx/include/unordered_map:1356
#if _LIBCPP_STD_VER > 17
        return key;
#else
        return std::string(key);
#endif
    }

}
