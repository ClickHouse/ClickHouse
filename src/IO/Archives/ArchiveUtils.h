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

std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(const std::string & source);

}
