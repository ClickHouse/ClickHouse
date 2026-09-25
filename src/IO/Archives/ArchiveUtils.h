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

std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(const std::string & source);

/// Splits the archive path syntax (e.g. `archive.zip::file*.parquet`) into the path to the archive
/// (`archive.zip`) and the path inside it (`file*.parquet`).
/// If the source string doesn't follow the archive syntax, the function just returns it in the second part.
std::pair<std::string, std::string> splitToArchivePathAndPathInArchive(const std::string & source);

/// Whether `path_in_archive` names a file stored in the archive at `path_to_archive`, which has to
/// exist. A member glob (e.g. `*.csv`) matches a dynamic set of files, possibly empty, so such a
/// name is reported as present without reading the archive, the way a glob over paths is.
bool archiveContainsFile(const std::string & path_to_archive, const std::string & path_in_archive);

}
