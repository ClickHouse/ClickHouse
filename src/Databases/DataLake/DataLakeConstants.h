#pragma once

#include <unordered_set>
#include <Core/Types.h>

namespace DataLake
{

static constexpr auto DATABASE_ENGINE_NAME = "DataLakeCatalog";
static inline std::unordered_set<String> SETTINGS_TO_HIDE = {"catalog_credential", "auth_header", "aws_access_key_id", "aws_secret_access_key"};
static constexpr std::string_view FILE_PATH_PREFIX = "file:/";

/// Some catalogs (Unity or Glue) may store not only Iceberg/DeltaLake tables but other kinds of "tables"
/// as simple files or some in-memory tables, or even DataLake tables but in some private storages.
/// ClickHouse can see these tables via catalog, but obviously cannot read them.
/// We use this placeholder when user ask for SHOW CREATE TABLE unreadable_table.
static constexpr auto FAKE_TABLE_ENGINE_NAME_FOR_UNREADABLE_TABLES = "Other";

}
