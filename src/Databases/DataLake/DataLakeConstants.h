#pragma once

#include <unordered_set>
#include <Core/Types.h>
#include <Interpreters/SecretArgumentsSpec.h>
#include <Core/Field.h>
#include <Common/HiddenSecret.h>
#include <optional>

namespace DataLake
{

static constexpr auto DATABASE_ENGINE_NAME = "DataLakeCatalog";
static constexpr std::string_view FILE_PATH_PREFIX = "file:/";

/// Some catalogs (Unity or Glue) may store not only Iceberg/DeltaLake tables but other kinds of "tables"
/// as simple files or some in-memory tables, or even DataLake tables but in some private storages.
/// ClickHouse can see these tables via catalog, but obviously cannot read them.
/// We use this placeholder when user ask for SHOW CREATE TABLE unreadable_table.
static constexpr auto FAKE_TABLE_ENGINE_NAME_FOR_UNREADABLE_TABLES = "Other";


using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    /// Catalog credentials
    {"catalog_credential", DB::hideSecretValue},
    {"auth_header", DB::hideSecretValue},
    /// AWS credentials
    {"aws_access_key_id", DB::hideSecretValue},
    {"aws_secret_access_key", DB::hideSecretValue},
    {"aws_external_id", DB::hideSecretValue},
    /// A trust policy can require a specific session name (`sts:RoleSessionName`), so it is a secret too.
    {"aws_role_session_name", DB::hideSecretValue},
    /// Legacy storage_* aliases (declared in DataLakeStorageSettings.h, originally for the Glue catalog)
    {"storage_catalog_credential", DB::hideSecretValue},
    {"storage_auth_header", DB::hideSecretValue},
    {"storage_aws_access_key_id", DB::hideSecretValue},
    {"storage_aws_secret_access_key", DB::hideSecretValue},
    {"storage_aws_role_session_name", DB::hideSecretValue},
    /// OneLake credentials
    {"onelake_client_secret", DB::hideSecretValue},
    {"onelake_bearer_token", DB::hideSecretValue},
    {"onelake_refresh_token", DB::hideSecretValue},
    /// Google credentials
    {"google_adc_client_secret", DB::hideSecretValue},
    {"google_adc_refresh_token", DB::hideSecretValue},
    /// DLF credentials
    {"dlf_access_key_id", DB::hideSecretValue},
    {"dlf_access_key_secret", DB::hideSecretValue},
};

/// The data lake table engines and table functions read `DataLakeStorageSettings`, and hide their credentials
/// in their own `SETTINGS`.
inline DB::SecretArgumentsSpec withSecretSettings(DB::SecretArgumentsSpec spec)
{
    spec.secret_settings = SETTINGS_TO_HIDE;
    return spec;
}

}
