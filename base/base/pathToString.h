#pragma once

#include <filesystem>
#include <string>
#include <string_view>

/// Conversions between `std::filesystem::path` and the UTF-8 byte strings ClickHouse uses for
/// paths everywhere else.
///
/// On POSIX these are the identity: `path::value_type` is `char`, `path::string_type` is
/// `std::string`, and a `path` converts to and from `std::string` implicitly - which is why most
/// of the codebase relies on that implicit conversion and never needed a helper.
///
/// On Windows `path::value_type` is `wchar_t` and the implicit conversion does not exist, so
/// every such site has to say which encoding it means. The answer is not `path::string()`: that
/// converts through the process's active code page and so mangles any path containing a
/// character outside it. `path::u8string()` is the one that round-trips.
///
/// Prefer these over `path::string()` in portable code, in both directions - constructing a
/// `path` from a `std::string` has the same code-page problem as reading one out of it.

namespace detail
{
inline std::string u8StringToString(const std::u8string & str)
{
    /// `char8_t` and `char` have the same size, representation and alignment; this is the
    /// conversion `std::u8string` exists to make explicit.
    return std::string(reinterpret_cast<const char *>(str.data()), str.size());
}
}

inline std::string pathToString(const std::filesystem::path & path)
{
#if defined(OS_WINDOWS)
    return detail::u8StringToString(path.u8string());
#else
    return path.string();
#endif
}

/// The same, but always separated by `/` rather than by the host's preferred separator.
///
/// For paths that are not the local filesystem's: keys in an object store, entries inside an
/// archive, the virtual paths of `IDisk`. Those are `/`-separated by definition, so a `path`
/// assembled from them with `operator/` - which appends `\` on Windows - has to be read back
/// generically or it stops being the path it names. On POSIX this is identical to `pathToString`.
inline std::string pathToGenericString(const std::filesystem::path & path)
{
#if defined(OS_WINDOWS)
    return detail::u8StringToString(path.generic_u8string());
#else
    return path.generic_string();
#endif
}

inline std::filesystem::path pathFromString(std::string_view path)
{
#if defined(OS_WINDOWS)
    return std::filesystem::path(std::u8string_view(reinterpret_cast<const char8_t *>(path.data()), path.size()));
#else
    return std::filesystem::path(path);
#endif
}
