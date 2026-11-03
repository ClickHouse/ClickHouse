#include <IO/Archives/ArchiveUtils.h>

#include <IO/Archives/IArchiveReader.h>
#include <IO/Archives/createArchiveReader.h>
#include <Common/StringUtils.h>

#include <string_view>
#include <array>

namespace DB
{

namespace
{

using namespace std::literals;
constexpr std::array tar_extensions{".tar"sv, ".tar.gz"sv, ".tgz"sv, ".tar.zst"sv, ".tzst"sv, ".tar.xz"sv, ".tar.bz2"sv, ".tar.lzma"sv};
constexpr std::array zip_extensions{".zip"sv, ".zipx"sv};
constexpr std::array sevenz_extensiosns{".7z"sv};

bool hasSupportedExtension(std::string_view path, const auto & supported_extensions)
{
    for (auto supported_extension : supported_extensions)
    {
        if (path.ends_with(supported_extension))
            return true;
    }

    return false;
}

/// The `::` that separates an archive from a path inside it, ignoring the `::` of an IPv6 literal in the
/// authority: `http://[::1]:8123/a.zip::member.csv` and `http://user:pass@[::1]/a.zip::member.csv` keep
/// their bracketed host intact, so the separator is the `::` after it. A host template can carry several
/// bracketed hosts, as in `http://{[::1]:8123|[::2]:8123}/a.zip::member.csv`, so every bracketed segment is
/// skipped rather than only a leading one. A source without a scheme has no authority and is searched from
/// the beginning, which keeps plain paths unchanged.
size_t findArchiveSeparator(const std::string & source)
{
    size_t search_from = 0;
    if (const size_t scheme_pos = source.find("://"); scheme_pos != std::string::npos)
    {
        const size_t authority_begin = scheme_pos + 3;
        const size_t authority_end = source.find_first_of("/?#", authority_begin);
        const std::string_view authority(
            source.data() + authority_begin,
            (authority_end == std::string::npos ? source.size() : authority_end) - authority_begin);

        /// A `::` in the userinfo is not the separator either: `user:p::q@host` keeps it in the password.
        const size_t userinfo_end = authority.rfind('@');
        const size_t host_begin = userinfo_end == std::string_view::npos ? 0 : userinfo_end + 1;
        search_from = authority_begin + host_begin;
        /// Skip every bracketed host, not only one that starts the authority: a host template such as
        /// `{[::1]:8123|[::2]:8123}` starts with `{`, and its literals keep their `::` all the same.
        size_t opening_bracket = authority.find('[', host_begin);
        while (opening_bracket != std::string_view::npos)
        {
            const size_t closing_bracket = authority.find(']', opening_bracket + 1);
            if (closing_bracket == std::string_view::npos)
                break;
            search_from = authority_begin + closing_bracket + 1;
            opening_bracket = authority.find('[', closing_bracket + 1);
        }
    }

    return source.find("::", search_from);
}

/// Split `source` at `separator`, trimming the spaces around `::`. The caller has already decided that
/// the part before the separator is an archive, so this only rejects an empty side.
std::pair<std::string, std::optional<std::string>> splitAtArchiveSeparator(const std::string & source, size_t separator)
{
    std::string_view path_to_archive_view = std::string_view{source}.substr(0, separator);
    while (path_to_archive_view.ends_with(' '))
        path_to_archive_view.remove_suffix(1);

    std::string_view archive_pattern_view = std::string_view{source}.substr(separator + 2);
    while (archive_pattern_view.starts_with(' '))
        archive_pattern_view.remove_prefix(1);

    if (archive_pattern_view.empty() || path_to_archive_view.empty())
        return {source, std::nullopt};

    return std::pair{std::string{path_to_archive_view}, std::string{archive_pattern_view}};
}

}

bool hasSupportedTarExtension(std::string_view path)
{
    return hasSupportedExtension(path, tar_extensions);
}

bool hasSupportedZipExtension(std::string_view path)
{
    return hasSupportedExtension(path, zip_extensions);
}

bool hasSupported7zExtension(std::string_view path)
{
    return hasSupportedExtension(path, sevenz_extensiosns);
}

bool hasSupportedArchiveExtension(std::string_view path)
{
    return hasSupportedTarExtension(path) || hasSupportedZipExtension(path) || hasSupported7zExtension(path);
}

std::pair<std::string, std::string> splitToArchivePathAndPathInArchive(const std::string & source)
{
    size_t pos = findArchiveSeparator(source);
    if (pos == std::string::npos)
        return {{}, source};

    std::string_view path_to_archive_view = std::string_view{source}.substr(0, pos);
    while (path_to_archive_view.ends_with(' '))
        path_to_archive_view.remove_suffix(1);

    std::string_view filename_view = std::string_view{source}.substr(pos + 2);
    while (filename_view.starts_with(' '))
        filename_view.remove_prefix(1);

    if (filename_view.empty() || path_to_archive_view.empty())
        return {{}, source};

    /// possible situations when the first part can be archive is only if one of the following is true:
    /// - it contains supported archive extension
    /// - it contains characters that could mean glob expression
    if (!hasSupportedArchiveExtension(path_to_archive_view) && path_to_archive_view.find_first_of("*?{") == std::string_view::npos)
        return {{}, source};

    return {std::string{path_to_archive_view}, std::string{filename_view}};
}

bool archiveContainsFile(const std::string & path_to_archive, const std::string & path_in_archive)
{
    if (containsGlobs(path_in_archive))
        return true;

    return createArchiveReader(path_to_archive)->fileExists(path_in_archive);
}

std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(const std::string & source)
{
    size_t pos = findArchiveSeparator(source);
    if (pos == std::string::npos)
        return {source, std::nullopt};

    std::string_view path_to_archive_view = std::string_view{source}.substr(0, pos);
    bool contains_spaces_around_operator = false;
    while (path_to_archive_view.ends_with(' '))
    {
        contains_spaces_around_operator = true;
        path_to_archive_view.remove_suffix(1);
    }

    std::string_view archive_pattern_view = std::string_view{source}.substr(pos + 2);
    while (archive_pattern_view.starts_with(' '))
    {
        contains_spaces_around_operator = true;
        archive_pattern_view.remove_prefix(1);
    }

    /// possible situations when the first part can be archive is only if one of the following is true:
    /// - it contains supported extension
    /// - it contains spaces after or before :: (URI cannot contain spaces)
    /// - it contains characters that could mean glob expression
    if (archive_pattern_view.empty() || path_to_archive_view.empty()
        || (!contains_spaces_around_operator && !hasSupportedArchiveExtension(path_to_archive_view)
            && path_to_archive_view.find_first_of("*?{") == std::string_view::npos))
        return {source, std::nullopt};

    return std::pair{std::string{path_to_archive_view}, std::string{archive_pattern_view}};
}

std::pair<std::string, std::optional<std::string>> getURLAndArchivePattern(const std::string & source)
{
    const size_t archive_separator = findArchiveSeparator(source);
    if (archive_separator == std::string::npos)
        return {source, std::nullopt};

    const size_t query_or_fragment = source.find_first_of("?#");
    if (query_or_fragment != std::string::npos && query_or_fragment < archive_separator)
    {
        const size_t scheme_pos = source.find("://");
        const size_t authority_start = scheme_pos == std::string::npos ? 0 : scheme_pos + 3;
        const size_t path_start = source.find('/', authority_start);
        std::string_view path;
        if (path_start != std::string::npos && path_start < query_or_fragment)
            path = std::string_view(source.data() + path_start, query_or_fragment - path_start);
        else if (scheme_pos == std::string::npos)
            /// A relative reference has no authority, so its whole leading part is the path: a task path
            /// listed by an HTTP index page looks like `a.zip?token=x::member`.
            path = std::string_view(source.data(), query_or_fragment);
        else
            /// An absolute URL without a path has no archive to name; its host is not the path.
            return {source, std::nullopt};
        /// For `url` the `{...}` and `*` of the path are shard and wildcard syntax, not evidence of an
        /// archive: `data{1,2}.csv?x=::1` is a literal URL whose query happens to contain `::`. Only a
        /// supported archive suffix makes a `::` after the query a separator, and an archive without
        /// such a suffix could not be opened anyway (`createArchiveReader` refuses to guess the type).
        if (!hasSupportedArchiveExtension(path))
            return {source, std::nullopt};

        /// The path alone already proves the archive type, so split here rather than delegating to
        /// `getURIAndArchivePattern`: that function re-tests the whole left part, which carries the query or
        /// the fragment. A query passes that test only because `?` is one of the glob characters it accepts,
        /// while a fragment has none of them, so a source with a fragment would be reported as literal and
        /// its member part silently dropped. Splitting here makes `archive.zip#frag::member.tsv` behave like
        /// `archive.zip?token=x::member.tsv`.
        return splitAtArchiveSeparator(source, archive_separator);
    }

    return getURIAndArchivePattern(source);
}
}
