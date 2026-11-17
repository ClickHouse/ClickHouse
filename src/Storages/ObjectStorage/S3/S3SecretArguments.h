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
/// `is_s3_database_engine`: the `S3` database engine, whose keys are known; `false` for `DataLakeCatalog`, which reads others.
SecretArgumentsSpec s3DatabaseSecretArguments(bool is_s3_database_engine);
/// `BACKUP ... TO S3(...)`.
SecretArgumentsSpec s3BackupSecretArguments();

/// Named arguments carrying S3 secrets, shared by every S3 form.
bool isS3SecretKey(std::string_view key);

/// Records the nested `headers(..)` map, whose values are hidden, and hides every `extra_credentials(..)` whole: both carry
/// secret auth material at any position.
void maskHeadersAndExtraCredentials(FunctionSecretArgumentsFinder & finder);

}
