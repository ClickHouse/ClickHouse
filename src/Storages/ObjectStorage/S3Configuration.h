#pragma once
#include <IO/S3/getObjectInfo.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/ObjectStorage/StorageObejctStorageConfiguration.h>

namespace DB
{

class StorageS3Configuration : public StorageObjectStorageConfiguration
{
public:
    Path getPath() const override { return url.key; }
    void setPath(const Path & path) override { url.key = path; }

    const Paths & getPaths() const override { return keys; }
    Paths & getPaths() override { return keys; }

    String getNamespace() const override { return url.bucket; }
    String getDataSourceDescription() override;

    void check(ContextPtr context) const override;
    StorageObjectStorageConfigurationPtr clone() override;

    ObjectStoragePtr createOrUpdateObjectStorage(ContextPtr context, bool is_readonly = true) override; /// NOLINT

    void fromNamedCollection(const NamedCollection & collection) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    static void addStructureToArgs(ASTs & args, const String & structure, ContextPtr context);

private:
    S3::URI url;
    S3::AuthSettings auth_settings;
    S3Settings::RequestSettings request_settings;
    /// If s3 configuration was passed from ast, then it is static.
    /// If from config - it can be changed with config reload.
    bool static_configuration = true;
    /// Headers from ast is a part of static configuration.
    HTTPHeaderEntries headers_from_ast;
    std::vector<String> keys;

    std::unique_ptr<S3::Client> createClient(ContextPtr context);

    bool initialized = false;
};

}
