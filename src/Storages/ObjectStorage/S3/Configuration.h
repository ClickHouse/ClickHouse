#pragma once

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Settings.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class StorageS3Configuration : public StorageObjectStorage::Configuration
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    static constexpr auto type = ObjectStorageType::S3;
    static constexpr auto type_name = "s3";
    static constexpr auto namespace_name = "bucket";
    /// All possible signatures for S3 storage with structure argument (for example for s3 table function).
    static constexpr auto max_number_of_arguments_with_structure = 7;
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
        "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    /// All possible signatures for S3 storage without structure argument (for example for S3 table engine).
    static constexpr auto max_number_of_arguments_without_structure = 6;
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
        "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    StorageS3Configuration() = default;
    StorageS3Configuration(const StorageS3Configuration & other);

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return url.storage_name; }
    std::string getNamespaceType() const override { return namespace_name; }

    std::string getSignatures(bool with_structure = true) const { return with_structure ? signatures_with_structure : signatures_without_structure; }
    size_t getMaxNumberOfArguments(bool with_structure = true) const { return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure; }

    Path getPath() const override { return url.key; }
    void setPath(const Path & path) override { url.key = path; }

    const Paths & getPaths() const override { return keys; }
    void setPaths(const Paths & paths) override { keys = paths; }

    String getNamespace() const override { return url.bucket; }
    String getDataSourceDescription() const override;
    StorageObjectStorage::QuerySettings getQuerySettings(const ContextPtr &) const override;

    bool isArchive() const override { return url.archive_pattern.has_value(); }
    std::string getPathInArchive() const override;

    void check(ContextPtr context) const override;
    void validateNamespace(const String & name) const override;
    ConfigurationPtr clone() override { return std::make_shared<StorageS3Configuration>(*this); }
    bool isStaticConfiguration() const override { return static_configuration; }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args,
        const String & structure,
        const String & format,
        ContextPtr context) override;

private:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;

    S3::URI url;
    std::vector<String> keys;

    S3::S3AuthSettings auth_settings;
    S3::S3RequestSettings request_settings;
    HTTPHeaderEntries headers_from_ast; /// Headers from ast is a part of static configuration.
    /// If s3 configuration was passed from ast, then it is static.
    /// If from config - it can be changed with config reload.
    bool static_configuration = true;
};

}

#endif
