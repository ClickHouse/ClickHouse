#pragma once

#include <unordered_set>
#include <Core/Types.h>

namespace DataLake
{

static constexpr auto DATABASE_ENGINE_NAME = "DataLake";
static inline std::unordered_set<String> SETTINGS_TO_HIDE = {"catalog_credential", "auth_header"};
static constexpr std::string_view FILE_PATH_PREFIX = "file:/";
static constexpr auto FAKE_TABLE_ENGINE_NAME_FOR_UNREADABLE_TABLES = "Other";

}
