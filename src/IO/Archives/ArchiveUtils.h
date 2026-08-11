#pragma once

#include "config.h"

#if USE_LIBARCHIVE

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"

#include <archive.h>
#include <archive_entry.h>
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

/// Splits `source` at `::` into a path to an archive and a pattern inside the archive,
/// when the left side can plausibly denote an archive. `use_glob_ast` selects the glob
/// parser used for the "left side contains a glob" capability check: the legacy parser
/// treats any of `*?{` as a glob, while the AST parser treats e.g. `data_{x}` as literal
/// text, so under it such a path stays a plain (non-archive) path.
std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(const std::string & source, bool use_glob_ast);

}
