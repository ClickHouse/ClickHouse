#pragma once

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Settings.h>
#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#if CLICKHOUSE_CLOUD
#include <Storages/ObjectStorage/S3/Serde.h>
#endif
#include <Parsers/IAST_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

namespace DB
{

/// A credential the collection itself supplied may only be sent to an origin (scheme/host/port) the collection
/// itself declares. Throws `BAD_ARGUMENTS` when a query moved the origin with such a credential still attached,
/// which would send it - and, for SigV4, a signature over an attacker-chosen request - to a user-chosen host.
/// A collection that stores no `url` authorises no destination at all. One whose stored `url` is relative, or is
/// a value no URL parser accepts, declares no origin to compare against and stays unbound.
///
/// Call once per seam, after the seam has finished resolving credentials and the destination URL, with the
/// collection still in hand so that per-key override provenance is available. `effective_url` is the URL the
/// request will actually be sent to.
///
/// `is_metadata_replay` marks a definition read back from persisted metadata rather than supplied by a fresh
/// query. Such a definition is logged and allowed instead of refused, since whoever loads it did not choose it
/// and a refusal at startup stops the server; the server setting
/// `s3_load_table_anonymously_if_credentials_restricted` makes it a hard failure instead.
///
/// Returns true only in that logged-and-allowed case, which grandfathers the single destination the stored
/// definition names. A seam that goes on to pick a destination per query must then drop the credentials.
bool validateS3CollectionDestinationBinding(
    const NamedCollection & collection,
    const S3::S3AuthSettings & effective_auth,
    const String & effective_url,
    ContextPtr context,
    bool is_metadata_replay);

struct S3StorageParsedArguments : private StorageParsedArguments
{
    friend class StorageS3Configuration;
    static constexpr auto max_number_of_arguments_with_structure = 10;
    static constexpr auto signatures_with_structure
        = " - url\n"
          " - url, NOSIGN\n"
          " - url, format\n"
          " - url, NOSIGN, format\n"
          " - url, format, structure\n"
          " - url, NOSIGN, format, structure\n"
          " - url, format, structure, compression_method\n"
          " - url, NOSIGN, format, structure, compression_method\n"
          " - url, access_key_id, secret_access_key, format, structure\n"
          " - url, access_key_id, secret_access_key, session_token, format, structure\n"
          " - url, access_key_id, secret_access_key, format, structure, compression_method\n"
          " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method\n"
          " - url, access_key_id, secret_access_key, session_token, format, structure, partition_strategy\n"
          " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy\n"
          " - url, access_key_id, secret_access_key, session_token, format, structure, partition_strategy, "
          "partition_columnns_in_data_file\n"
          " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy, "
          "partition_columnns_in_data_file\n"
          " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy, "
          "partition_columnns_in_data_file, storage_class_name\n"
          "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    static constexpr auto max_number_of_arguments_without_structure = max_number_of_arguments_with_structure - 1;
    /// All possible signatures for S3 storage without structure argument (for example for S3 table engine).
    static constexpr auto signatures_without_structure
        = " - url\n"
          " - url, NOSIGN\n"
          " - url, format\n"
          " - url, NOSIGN, format\n"
          " - url, format, compression_method\n"
          " - url, NOSIGN, format, compression_method\n"
          " - url, access_key_id, secret_access_key\n"
          " - url, access_key_id, secret_access_key, session_token\n"
          " - url, access_key_id, secret_access_key, format\n"
          " - url, access_key_id, secret_access_key, session_token, format\n"
          " - url, access_key_id, secret_access_key, format, compression_method\n"
          " - url, access_key_id, secret_access_key, session_token, format, compression_method\n"
          " - url, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy\n"
          " - url, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy, "
          "partition_columnns_in_data_file\n"
          "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    static constexpr std::string getSignatures(bool with_structure = true)
    {
        return with_structure ? signatures_with_structure : signatures_without_structure;
    }

    static constexpr size_t getMaxNumberOfArguments(bool with_structure = true)
    {
        return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure;
    }

    static bool collectCredentials(ASTPtr maybe_credentials, S3::S3AuthSettings & auth_settings_, ContextPtr local_context);


    S3::URI url;
    std::unique_ptr<S3Settings> s3_settings;
    std::unique_ptr<S3Capabilities> s3_capabilities;
    HTTPHeaderEntries headers_from_ast;
    String path_suffix;

public:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context, bool is_metadata_replay = false);
    void fromDisk(const DiskPtr & disk, ASTs & args, ContextPtr context, bool with_structure);
    void fromAST(ASTs & args, ContextPtr context, bool with_structure);
    S3StorageParsedArguments() = default;
};


class StorageS3Configuration : public StorageObjectStorageConfiguration
{
public:
    static constexpr auto type = ObjectStorageType::S3;
    static constexpr auto type_name = "s3";
    static constexpr auto namespace_name = "bucket";
    /// All possible signatures for S3 storage with structure argument (for example for s3 table function).

    StorageS3Configuration() = default;

    void setInitializationAsBigLake(const String & client_id_, const String & client_secret_, const String & refresh_token_)
    {
        biglake_adc_client_id = client_id_;
        biglake_adc_client_secret = client_secret_;
        biglake_adc_refresh_token = refresh_token_;
    }

    ObjectStorageType getType() const override { return type; }

    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return url.storage_name; }
    std::string getNamespaceType() const override { return namespace_name; }

    bool supportsFullyQualifiedPaths() const override { return isDataLakeConfiguration(); }

    const S3::S3AuthSettings & getAuthSettings() const { return s3_settings->auth_settings; }

    Path getRawPath() const override { return url.key; }
    void setRawPath(const Path & path) override { url.key = path.path; }
    const String & getRawURI() const override { return url.uri_str; }

    const Paths & getPaths() const override { return keys; }
    void setPaths(const Paths & paths) override
    {
        keys = paths;
    }

    String getNamespace() const override { return url.bucket; }
    String getDataSourceDescription() const override;
    String getDataSourceDescriptionForNamespace(const String & object_namespace) const override;
    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr &) const override;

    bool isArchive() const override { return url.archive_pattern.has_value(); }
    std::string getPathInArchive() const override;

    void check(ContextPtr context) override;
    void validateNamespace(const String & name) const override;
    bool isStaticConfiguration() const override { return static_configuration; }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args,
        const String & structure,
        const String & format,
        ContextPtr context,
        bool with_structure) override;

    static bool collectCredentials(ASTPtr maybe_credentials, S3::S3AuthSettings & auth_settings_, ContextPtr local_context);

    S3::URI url;


    Paths keys;

    std::unique_ptr<S3Settings> s3_settings;
    std::unique_ptr<S3Capabilities> s3_capabilities;

    HTTPHeaderEntries headers_from_ast; /// Headers from ast is a part of static configuration.
    /// If s3 configuration was passed from ast, then it is static.
    /// If from config - it can be changed with config reload.
    bool static_configuration = true;

    String biglake_adc_client_id;
    String biglake_adc_client_secret;
    String biglake_adc_refresh_token;

protected:
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override;

private:
    void initializeFromParsedArguments(S3StorageParsedArguments && parsed_arguments);

    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;

    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
};
}

#endif
