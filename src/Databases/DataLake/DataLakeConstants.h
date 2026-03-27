#pragma once

#include <unordered_set>
#include <Core/Types.h>
#include <Core/Field.h>

namespace DataLake
{

static constexpr auto DATABASE_ENGINE_NAME = "DataLakeCatalog";
static constexpr std::string_view FILE_PATH_PREFIX = "file:/";

/// Some catalogs (Unity or Glue) may store not only Iceberg/DeltaLake tables but other kinds of "tables"
/// as simple files or some in-memory tables, or even DataLake tables but in some private storages.
/// ClickHouse can see these tables via catalog, but obviously cannot read them.
/// We use this placeholder when user ask for SHOW CREATE TABLE unreadable_table.
static constexpr auto FAKE_TABLE_ENGINE_NAME_FOR_UNREADABLE_TABLES = "Other";

static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::string(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"catalog_credential", DEFAULT_MASKING_RULE},
    {"auth_header", DEFAULT_MASKING_RULE},
    {"aws_access_key_id", DEFAULT_MASKING_RULE},
    {"aws_secret_access_key", DEFAULT_MASKING_RULE},
};
}
