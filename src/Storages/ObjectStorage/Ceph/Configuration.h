#pragma once

#include "Disks/ObjectStorages/Ceph/CephUtils.h"
#include "config.h"

#if USE_CEPH
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class StorageRadosConfiguration : public StorageObjectStorage::Configuration
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    static constexpr auto type_name = "rados";
    static constexpr auto engine_name = "Rados";
    static constexpr auto namespace_name = "pool";

    StorageRadosConfiguration() = default;
    StorageRadosConfiguration(const StorageRadosConfiguration & other);

    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }
    std::string getNamespaceType() const override { return namespace_name; }

    Path getPath() const override { return path; }
    void setPath(const Path & path_) override { path = path_; }

    const Paths & getPaths() const override { return paths; }
    void setPaths(const Paths & paths_) override { paths = paths_; }

    String getNamespace() const override { return endpoint.pool; }
    String getDataSourceDescription() const override;
    StorageObjectStorage::QuerySettings getQuerySettings(const ContextPtr &) const override;

    bool isArchive() const override { return false; }
    std::string getPathInArchive() const override { return {}; }

    void check(ContextPtr context) const override;
    void validateNamespace(const String & name) const override;
    ConfigurationPtr clone() override { return std::make_shared<StorageRadosConfiguration>(*this); }
    bool isStaticConfiguration() const override { return static_configuration; }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;

    void addStructureAndFormatToArgs(
        ASTs & args,
        const String & structure,
        const String & format,
        ContextPtr context) override;

private:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;

    RadosEndpoint endpoint;
    String path;
    std::vector<String> paths;

    HTTPHeaderEntries headers_from_ast; /// Headers from ast is a part of static configuration.
    /// If s3 configuration was passed from ast, then it is static.
    /// If from config - it can be changed with config reload.
    bool static_configuration = true;
};

}

#endif
