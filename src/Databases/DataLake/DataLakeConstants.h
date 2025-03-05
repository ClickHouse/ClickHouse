#pragma once

#include <unordered_set>
#include <Core/Types.h>

namespace DataLake
{

static constexpr auto DATABASE_ENGINE_NAME = "DataLakeCatalog";
static inline std::unordered_set<String> SETTINGS_TO_HIDE = {"catalog_credential", "auth_header", "aws_access_key_id", "aws_secret_access_key"};
static constexpr std::string_view FILE_PATH_PREFIX = "file:/";
static constexpr auto FAKE_TABLE_ENGINE_NAME_FOR_UNREADABLE_TABLES = "Other";

}
