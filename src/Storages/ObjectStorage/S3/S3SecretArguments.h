#pragma once

#include <Interpreters/SecretArgumentsSpec.h>

#include <string_view>

namespace DB
{

/// `s3`, `gcs`, `cosn`, `oss` and the S3 data lake table functions; `s3Cluster` and the S3 data lake cluster ones.
SecretArgumentsSpec s3TableFunctionSecretArguments(bool is_cluster_function);
/// `S3`, `COSN`, `OSS`, `GCS`, `S3Queue` and the S3 data lake table engines.
SecretArgumentsSpec s3TableEngineSecretArguments();
/// The `S3` and `DataLakeCatalog` database engines.
SecretArgumentsSpec s3DatabaseSecretArguments();
/// `BACKUP ... TO S3(...)`.
SecretArgumentsSpec s3BackupSecretArguments();

/// Named arguments carrying S3 secrets, shared by every S3 form.
bool isS3SecretKey(std::string_view key);

/// Whether the value of this key of an `extra_credentials(..)` map stays visible when the map is masked.
bool isNonSecretExtraCredentialsKey(std::string_view key);

/// Records the nested `headers(..)` and `extra_credentials(..)` maps, which carry secret auth material at any position.
void maskHeadersAndExtraCredentials(FunctionSecretArgumentsFinder & finder);

/// Masks credential material embedded in an S3 URL itself: the userinfo part and the values of
/// presigned-URL query parameters. Returns true if anything was masked.
bool maskS3URICredentials(String & url);

}
