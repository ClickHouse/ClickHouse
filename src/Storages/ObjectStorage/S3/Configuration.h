#pragma once

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/getObjectInfo.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

namespace DB
{

class StorageS3Configuration : public StorageObjectStorageConfiguration
{
public:
    StorageS3Configuration() = default;
    StorageS3Configuration(const StorageS3Configuration & other);

    Path getPath() const override { return url.key; }
    void setPath(const Path & path) override { url.key = path; }

    const Paths & getPaths() const override { return keys; }
    Paths & getPaths() override { return keys; }

    String getNamespace() const override { return url.bucket; }
    String getDataSourceDescription() override;

    void check(ContextPtr context) const override;
    StorageObjectStorageConfigurationPtr clone() override { return std::make_shared<StorageS3Configuration>(*this); }
    bool isStaticConfiguration() const override { return static_configuration; }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly = true) override; /// NOLINT
    static void addStructureToArgs(ASTs & args, const String & structure, ContextPtr context);

private:
    void fromNamedCollection(const NamedCollection & collection) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;

    S3::URI url;
    std::vector<String> keys;

    S3::AuthSettings auth_settings;
    S3Settings::RequestSettings request_settings;
    HTTPHeaderEntries headers_from_ast; /// Headers from ast is a part of static configuration.
    /// If s3 configuration was passed from ast, then it is static.
    /// If from config - it can be changed with config reload.
    bool static_configuration = true;
};

}

#endif
