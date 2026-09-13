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

/// A `::` after `?` or `#` is an archive separator only when the URL path itself looks like
/// a supported archive path. This keeps `/api?x=::1` literal while allowing
/// `/archive.zip?token=x::member.csv` without requiring spaces around `::`.
std::pair<std::string, std::optional<std::string>> getURLAndArchivePattern(const std::string & source);

}
