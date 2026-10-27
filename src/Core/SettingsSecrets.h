#pragma once

#include <base/types.h>

#include <optional>

namespace DB
{
class Field;
}

namespace DB::CoreSettings
{

/// Masks the credential in the value of a query-level setting that can carry one, such as
/// `format_avro_schema_registry_url`, `url_base` or `s3_base`. `system.query_log.query` shows
/// `format_avro_schema_registry_url = 'http://[HIDDEN]@registry:8080'`, so every other place that
/// prints the same value hides the same secret through this. Returns whether anything was masked.
bool maskSettingValue(const String & setting_name, String & value);

/// Same as above, for a sink that also has the raw `Field`. A setting value can be an AST rather
/// than a literal, e.g. `custom_x = disk(type = 's3', secret_access_key = '...')`, and no plain
/// `Field` formatter hides that.
///
/// This is the form for a sink that prints the value as it is, such as a system table column or a
/// log line. A sink that prints SQL needs `renderSecretSettingValue` instead.
bool maskSettingValue(const String & setting_name, const Field & field, String & value);

/// Renders a setting value that holds a secret as the SQL text that hides it, and returns `nullopt`
/// for a value that holds none, which the caller then renders with its own visitor.
///
/// The masking runs on the raw string and the result is quoted afterwards, because the two cannot be
/// done in the other order: the value of a presigned URL parameter ends at the end of the text, so
/// masking an already-quoted literal takes the closing quote with it and leaves
/// `s3_base = 'https://bucket/f.csv?X-Amz-Signature=[HIDDEN]`, which no longer parses.
///
/// Whether a value holds a secret and how that secret is hidden are the same question, so a
/// `formatImpl` and the matching `hasSecretParts` both ask it here and cannot disagree.
std::optional<String> renderSecretSettingValue(const String & setting_name, const Field & value);

}
