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
size_t archiveSeparatorSearchStart(std::string_view source)
{
    size_t search_from = 0;
    /// Only a `://` that comes before the first `::` and before any other delimiter can be the scheme of the
    /// source: a member path may itself look like a URL (`a.zip::http://host/x`), and a query value may too
    /// (`a.zip?u=http://host::m`). Treating either as the scheme would push the search past the real
    /// separator and turn the whole source into a literal.
    const size_t first_separator = source.find("::");
    if (const size_t scheme_pos = source.find("://");
        scheme_pos != std::string_view::npos && (first_separator == std::string_view::npos || scheme_pos < first_separator)
        && source.find_first_of("/?#") >= scheme_pos)
    {
        const size_t authority_begin = scheme_pos + 3;
        const size_t authority_end = source.find_first_of("/?#", authority_begin);
        const std::string_view authority(
            source.data() + authority_begin,
            (authority_end == std::string_view::npos ? source.size() : authority_end) - authority_begin);

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
            /// An unclosed bracket still ends the authority: the rest of it is not a host either, so a `::`
            /// there is not the separator.
            if (closing_bracket == std::string_view::npos)
            {
                search_from = authority.size() + authority_begin;
                break;
            }
            search_from = authority_begin + closing_bracket + 1;
            opening_bracket = authority.find('[', closing_bracket + 1);
        }
    }

    return search_from;
}

size_t findArchiveSeparator(std::string_view source)
{
    return source.find("::", archiveSeparatorSearchStart(source));
}

/// The first `::` with whitespace on either side. A URL cannot contain whitespace - it has to be
/// percent-encoded - so such a separator is one the user wrote explicitly, while a `::` without it may be
/// part of the URL itself, as in the signature of `archive.zip?sig=abc::def`. The scan starts past the
/// scheme, the userinfo and any bracketed host, so a `::` of those is not taken for it; an unbracketed host
/// is not skipped, but a `::` there is not part of a valid URL either.
size_t findExplicitArchiveSeparator(std::string_view source)
{
    size_t pos = source.find("::", archiveSeparatorSearchStart(source));
    while (pos != std::string_view::npos)
    {
        const bool space_before = pos > 0 && isWhitespaceASCII(source[pos - 1]);
        const bool space_after = pos + 2 < source.size() && isWhitespaceASCII(source[pos + 2]);
        if (space_before || space_after)
            return pos;
        pos = source.find("::", pos + 2);
    }
    return std::string_view::npos;
}

/// Split `source` at `separator`, trimming the whitespace around `::`. The caller has already decided that
/// the part before the separator is an archive, so this only rejects an empty side.
std::pair<std::string, std::optional<std::string>> splitAtArchiveSeparator(std::string_view source, size_t separator)
{
    /// Trim exactly what `findExplicitArchiveSeparator` accepts as the separator's whitespace, so a
    /// recognized separator never leaves a tab or a newline inside either half.
    std::string_view path_to_archive_view = source.substr(0, separator);
    while (!path_to_archive_view.empty() && isWhitespaceASCII(path_to_archive_view.back()))
        path_to_archive_view.remove_suffix(1);

    std::string_view archive_pattern_view = source.substr(separator + 2);
    while (!archive_pattern_view.empty() && isWhitespaceASCII(archive_pattern_view.front()))
        archive_pattern_view.remove_prefix(1);

    if (archive_pattern_view.empty() || path_to_archive_view.empty())
        return {std::string{source}, std::nullopt};

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

std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(std::string_view source)
{
    size_t pos = findArchiveSeparator(source);
    if (pos == std::string_view::npos)
        return {std::string{source}, std::nullopt};

    std::string_view path_to_archive_view = source.substr(0, pos);
    bool contains_spaces_around_operator = false;
    while (path_to_archive_view.ends_with(' '))
    {
        contains_spaces_around_operator = true;
        path_to_archive_view.remove_suffix(1);
    }

    std::string_view archive_pattern_view = source.substr(pos + 2);
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
        return {std::string{source}, std::nullopt};

    return std::pair{std::string{path_to_archive_view}, std::string{archive_pattern_view}};
}

namespace
{

/// The path of `source` between its authority and `end`. Only a `://` before any other delimiter is a
/// scheme: a relative path may carry one of its own - `data/http://a.zip::m` is a task path listed by an
/// index page - and reading that as the scheme would leave the path empty and drop the member.
std::string_view urlPathBefore(std::string_view source, size_t end)
{
    const size_t scheme_pos = source.find("://");
    const bool has_scheme = scheme_pos != std::string_view::npos && scheme_pos < end
        && source.find_first_of("/?#") >= scheme_pos;
    const size_t authority_start = has_scheme ? scheme_pos + 3 : 0;
    const size_t path_start = source.find('/', authority_start);
    if (path_start != std::string_view::npos && path_start < end)
        return source.substr(path_start, end - path_start);
    if (!has_scheme)
        /// A relative reference has no authority, so its whole leading part is the path: a task path listed
        /// by an HTTP index page looks like `a.zip?token=x::member`.
        return source.substr(0, end);
    /// An absolute URL without a path has no archive to name; its host is not the path.
    return {};
}

/// Whether the path of `source` up to `end` names an archive. For `url` the `{...}` of the path is shard
/// syntax, not evidence of an archive: `data{1,2}.csv?x=::1` is a literal URL whose query happens to
/// contain `::`. Only a supported archive suffix makes a `::` after the query a separator, and a fixed
/// archive without such a suffix could not be opened anyway (`createArchiveReader` refuses to guess the
/// type).
///
/// A `*` is different: it is always a wildcard for `url`, never a literal path character, and the type of a
/// wildcard archive comes from the object it expands to, not from the pattern. Treating
/// `part*?token=x::m.tsv` as a literal URL would read the archive bytes as data.
bool urlPathLooksLikeArchive(std::string_view source, size_t end)
{
    const auto path = urlPathBefore(source, end);
    return hasSupportedArchiveExtension(path) || path.contains('*');
}

}

std::pair<std::string, std::optional<std::string>> getURLAndArchivePattern(std::string_view source)
{
    const size_t query_or_fragment = source.find_first_of("?#");

    /// An explicit separator outranks a `::` that belongs to the URL: without this, the signature of
    /// `archive.zip?sig=abc::def :: member.csv` is split at its own `::`, and the member becomes
    /// `def :: member.csv`. It only outranks a candidate that is inside the URL - after a `?` or a `#` -
    /// because "a URL cannot contain whitespace" holds only for the URL part: a member name may contain
    /// whitespace, so a `::` with whitespace inside the member must not move the separator there.
    /// `file()` and `s3()` keep the first-candidate grammar they share with `getURIAndArchivePattern`.
    ///
    /// The whitespace decides which `::` is the separator, not whether the source is an archive: after a
    /// query that is still decided by the path, exactly as for an unspaced separator, so `a.csv?x=1 :: y=2`
    /// stays literal like `a.csv?x=::1`.
    if (const size_t explicit_separator = findExplicitArchiveSeparator(source); explicit_separator != std::string::npos)
    {
        /// The query branch trusts the explicit separator only when the first candidate is inside the URL
        /// too: a `?` that belongs to the member name must not make the member look like the URL part.
        const size_t first_separator = findArchiveSeparator(source);
        if (query_or_fragment != std::string::npos && query_or_fragment < explicit_separator
            && (first_separator == std::string_view::npos || first_separator > query_or_fragment))
        {
            if (!urlPathLooksLikeArchive(source, query_or_fragment))
                return {std::string{source}, std::nullopt};
            return splitAtArchiveSeparator(source, explicit_separator);
        }
        /// Without a query before it, the explicit separator is the separator only when it is also the first
        /// candidate: a spaced `::` inside the member name must not move the separator there.
        if (explicit_separator == first_separator)
            return splitAtArchiveSeparator(source, explicit_separator);
    }

    const size_t archive_separator = findArchiveSeparator(source);
    if (archive_separator == std::string::npos)
        return {std::string{source}, std::nullopt};

    if (query_or_fragment != std::string::npos && query_or_fragment < archive_separator)
    {
        if (!urlPathLooksLikeArchive(source, query_or_fragment))
            return {std::string{source}, std::nullopt};

        /// The path alone already proves the archive type, so split here rather than delegating to
        /// `getURIAndArchivePattern`: that function re-tests the whole left part, which carries the query or
        /// the fragment. A query passes that test only because `?` is one of the glob characters it accepts,
        /// while a fragment has none of them, so a source with a fragment would be reported as literal and
        /// its member part silently dropped. Splitting here makes `archive.zip#frag::member.tsv` behave like
        /// `archive.zip?token=x::member.tsv`.
        return splitAtArchiveSeparator(source, archive_separator);
    }

    /// The same holds without a query: a host that happens to end with an archive suffix is not a path, so
    /// `http://example.zip::m` is a URL whose host is `example.zip`, not an archive with an empty path.
    const size_t scheme_pos = source.find("://");
    if (scheme_pos != std::string_view::npos && scheme_pos < archive_separator
        && source.find_first_of("/?#") >= scheme_pos
        && source.find('/', scheme_pos + 3) >= archive_separator)
        return {std::string{source}, std::nullopt};

    /// Without a query the path-level evidence rule still applies. A `{...}` of the path is shard or
    /// failover syntax for `url`, not evidence of an archive, so delegating unconditionally to
    /// `getURIAndArchivePattern` - which accepts `{` - would reinterpret a written or stored
    /// `url('http://{h1,h2}/data/file::1.csv')` as an archive read. An explicit spaced separator never
    /// reaches this point: it is found by `findExplicitArchiveSeparator` and handled above.
    if (!urlPathLooksLikeArchive(source, archive_separator))
        return {std::string{source}, std::nullopt};

    return getURIAndArchivePattern(source);
}
}
