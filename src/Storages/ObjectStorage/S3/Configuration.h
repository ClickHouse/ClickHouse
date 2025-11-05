#pragma once

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Settings.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Parsers/IAST_fwd.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{

struct S3StorageParsableArguments
{
    using Paths = StorageObjectStorageConfiguration::Paths;
    using Path = StorageObjectStorageConfiguration::Path;
    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";
    PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
    bool partition_columns_in_data_file = true;
    std::shared_ptr<IPartitionStrategy> partition_strategy;
    S3::URI url;
    std::unique_ptr<S3Settings> s3_settings;
    std::unique_ptr<S3Capabilities> s3_capabilities;
    HTTPHeaderEntries headers_from_ast;
    Paths keys;
    bool static_configuration = true;
    Path read_path;

public:
    S3StorageParsableArguments() = default;

    void fromNamedCollection(const NamedCollection & collection, ContextPtr context);
    void fromAST(ASTs & args, ContextPtr context, bool with_structure);
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure);
};


class StorageS3Configuration : public StorageObjectStorageConfiguration
{
public:
    static constexpr auto type = ObjectStorageType::S3;
    static constexpr auto type_name = "s3";
    static constexpr auto namespace_name = "bucket";
    /// All possible signatures for S3 storage with structure argument (for example for s3 table function).
    static constexpr auto max_number_of_arguments_with_structure = 10;
    static constexpr auto signatures_with_structure =
        " - url\n"
        " - url, NOSIGN\n"
        " - url, format\n"
        " - url, NOSIGN, format\n"
        " - url, format, structure\n"
        " - url, NOSIGN, format, structure\n"
        " - url, format, structure, compression_method\n"
        " - url, NOSIGN, format, structure, compression_method\n"
        " - url, access_key_id, secret_access_key\n"
        " - url, access_key_id, secret_access_key, session_token\n"
        " - url, access_key_id, secret_access_key, format\n"
        " - url, access_key_id, secret_access_key, session_token, format\n"
        " - url, access_key_id, secret_access_key, format, structure\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure\n"
        " - url, access_key_id, secret_access_key, format, structure, compression_method\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure, partition_strategy\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure, partition_strategy, partition_columnns_in_data_file\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy, partition_columnns_in_data_file\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy, partition_columnns_in_data_file, storage_class_name\n"
        "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    /// All possible signatures for S3 storage without structure argument (for example for S3 table engine).
    static constexpr auto max_number_of_arguments_without_structure = 9;
    static constexpr auto signatures_without_structure =
        " - url\n"
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
        " - url, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy, partition_columnns_in_data_file\n"
        "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    StorageS3Configuration() = default;

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return url.storage_name; }
    std::string getNamespaceType() const override { return namespace_name; }

    static constexpr std::string getSignatures(bool with_structure = true)
    {
        return with_structure ? signatures_with_structure : signatures_without_structure;
    }

    const S3::S3AuthSettings & getAuthSettings() const { return s3_settings->auth_settings; }

    Path getRawPath() const override { return url.key; }
    const String & getRawURI() const override { return url.uri_str; }

    const Paths & getPaths() const override { return keys; }
    void setPaths(const Paths & paths) override
    {
        keys = paths;
    }

    String getNamespace() const override { return url.bucket; }
    String getDataSourceDescription() const override;
    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr &) const override;

    bool isArchive() const override { return url.archive_pattern.has_value(); }
    std::string getPathInArchive() const override;

    void check(ContextPtr context) override;
    void validateNamespace(const String & name) const override;
    bool isStaticConfiguration() const override { return static_configuration; }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args,
        const String & structure,
        const String & format,
        ContextPtr context,
        bool with_structure) override;

    S3::URI url;


    Paths keys;

    std::unique_ptr<S3Settings> s3_settings;
    std::unique_ptr<S3Capabilities> s3_capabilities;

    HTTPHeaderEntries headers_from_ast; /// Headers from ast is a part of static configuration.
    /// If s3 configuration was passed from ast, then it is static.
    /// If from config - it can be changed with config reload.
    bool static_configuration = true;

protected:
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override;

private:
    void initializeFromParsableArguments(S3StorageParsableArguments && parsable_arguments)
    {
        format = std::move(parsable_arguments.format);
        compression_method = std::move(parsable_arguments.compression_method);
        structure = std::move(parsable_arguments.structure);
        partition_strategy_type = parsable_arguments.partition_strategy_type;
        partition_columns_in_data_file = parsable_arguments.partition_columns_in_data_file;
        partition_strategy = std::move(parsable_arguments.partition_strategy);
        url = std::move(parsable_arguments.url);
        keys = std::move(parsable_arguments.keys);
        s3_settings = std::move(parsable_arguments.s3_settings);
        s3_capabilities = std::move(parsable_arguments.s3_capabilities);
        headers_from_ast = std::move(parsable_arguments.headers_from_ast);
        static_configuration = parsable_arguments.static_configuration;
    }

    size_t getMaxNumberOfArguments(bool with_structure = true) const
    {
        return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure;
    }

    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;

    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
};

void fromNamedCollectionSimple(S3StorageParsableArguments & entity_to_initialize, const NamedCollection & collection, ContextPtr context);

void addStructureAndFormatToArgsIfNeededSimple(
    ASTs & args, const String & structure, const String & format, ContextPtr context, bool with_structure, size_t max_number_of_arguments);

void fromASTSimple(
    S3StorageParsableArguments & entity_to_initialize,
    ASTs & args,
    ContextPtr context,
    bool with_structure,
    size_t max_number_of_arguments);

void fromDiskSimple(
    S3StorageParsableArguments & entity_to_initialize, const String & disk_name, ASTs & args, ContextPtr context, bool with_structure);

bool collectCredentials(ASTPtr maybe_credentials, S3::S3AuthSettings & auth_settings_, ContextPtr local_context);
}

#endif
