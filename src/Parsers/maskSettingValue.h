#pragma once

#include <base/types.h>

#include <optional>
#include <string_view>

namespace DB
{

/// The masked rendering of a setting's value, or nullopt when the setting is not secret.
///
/// Secrecy is a property of the pair (engine, setting name): `password` is a credential for
/// `RabbitMQ` and means nothing to `MergeTree`, and the registries are per engine for that reason.
/// Data lake settings are the exception - `DataLakeStorageSettings` is shared by the
/// `DataLakeCatalog` database engine and the `Iceberg*`, `Paimon*` and `DeltaLake*` table engines,
/// so its credentials are secret whatever names the engine goes by.
///
/// Most rules replace the value outright; a few mask only part of it, keeping the host of a URI or
/// an Azure connection string and hiding the password inside it. The result is plain text: a caller
/// writing SQL adds its own quoting.
///
/// This decides only *what a secret looks like*. Whether the caller is allowed to see the real value
/// is a separate question, answered by `Context::displaySecretsInShowAndSelect`, the
/// `format_display_secrets_in_show_and_select` setting and the `displaySecretsInShowAndSelect`
/// grant - see `formatWithPossiblyHidingSecrets.cpp`.
std::optional<String> maskSettingValue(std::string_view engine_name, const String & setting_name, std::string_view value);

/// Whether the name is a secret of *some* engine. Deliberately ignores the engine, unlike
/// `maskSettingValue`: a caller uses this to decide whether a statement needs masking at all, where
/// answering yes too often is safe and answering no wrongly is not.
bool isSecretSettingName(const String & setting_name);

}
