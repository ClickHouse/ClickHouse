#pragma once

#include "config.h"

#if USE_LIBARCHIVE

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"

#include <archive.h>
#include <archive_entry.h>
#pragma clang diagnostic pop
#endif

#include <optional>
#include <string_view>
#include <string>

namespace DB
{

bool hasSupportedTarExtension(std::string_view path);
bool hasSupportedZipExtension(std::string_view path);
bool hasSupported7zExtension(std::string_view path);

bool hasSupportedArchiveExtension(std::string_view path);

std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(std::string_view source);

/// Splits the archive path syntax (e.g. `archive.zip::file*.parquet`) into the path to the archive
/// (`archive.zip`) and the path inside it (`file*.parquet`).
/// If the source string doesn't follow the archive syntax, the function just returns it in the second part.
std::pair<std::string, std::string> splitToArchivePathAndPathInArchive(const std::string & source);

/// Whether `path_in_archive` names a file stored in the archive at `path_to_archive`, which has to
/// exist. A member glob (e.g. `*.csv`) matches a dynamic set of files, possibly empty, so such a
/// name is reported as present without reading the archive, the way a glob over paths is.
bool archiveContainsFile(const std::string & path_to_archive, const std::string & path_in_archive);
/// A `::` after `?` or `#` is an archive separator only when the URL path itself looks like
/// a supported archive path, or contains `*` - a wildcard archive proves nothing about its type, the object
/// it expands to does. This keeps `/api?x=::1` literal while allowing
/// `/archive.zip?token=x::member.csv` without requiring spaces around `::`.
///
/// A `::` with whitespace on either side is the separator the user wrote, because a URL cannot contain
/// it, so it outranks an unspaced `::` that belongs to the URL: `/archive.zip?sig=abc::def :: m.csv`
/// keeps its signature. `getURIAndArchivePattern`, which serves `s3`, recognizes and trims only a space;
/// this URL-specific splitter recognizes and trims any ASCII whitespace, and the same characters are
/// removed from both halves. `file()` is served by `splitToArchivePathAndPathInArchive`, which trims only a
/// space around the separator it picks and does not let whitespace choose it. The whitespace only decides which `::` is the separator: when a `?` or
/// `#` precedes it, whether the source is an archive at all is still decided by the path, so
/// `/a.csv?x=1 :: y=2` stays literal. Without a query the left side is taken as the archive, as
/// `getURIAndArchivePattern` already does for a spaced separator, and the reader reports a path that is
/// not an archive.
std::pair<std::string, std::optional<std::string>> getURLAndArchivePattern(std::string_view source);

}
