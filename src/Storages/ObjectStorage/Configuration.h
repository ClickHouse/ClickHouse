#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/NamedCollectionsHelpers.h>

namespace DB
{

class StorageObjectStorageConfiguration;
using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;

class StorageObjectStorageConfiguration
{
public:
    StorageObjectStorageConfiguration() = default;
    virtual ~StorageObjectStorageConfiguration() = default;

    using Path = std::string;
    using Paths = std::vector<Path>;

    virtual Path getPath() const = 0;
    virtual void setPath(const Path & path) = 0;

    virtual const Paths & getPaths() const = 0;
    virtual Paths & getPaths() = 0;

    virtual String getDataSourceDescription() = 0;
    virtual String getNamespace() const = 0;

    bool isPathWithGlobs() const { return getPath().find_first_of("*?{") != std::string::npos; }
    bool isNamespaceWithGlobs() const { return getNamespace().find_first_of("*?{") != std::string::npos; }

    std::string getPathWithoutGlob() const { return getPath().substr(0, getPath().find_first_of("*?{")); }

    virtual bool withWildcard() const
    {
        static const String PARTITION_ID_WILDCARD = "{_partition_id}";
        return getPath().find(PARTITION_ID_WILDCARD) != String::npos;
    }

    virtual void check(ContextPtr context) const = 0;
    virtual StorageObjectStorageConfigurationPtr clone() = 0;

    virtual ObjectStoragePtr createOrUpdateObjectStorage(ContextPtr context, bool is_readonly = true) = 0; /// NOLINT

    virtual void fromNamedCollection(const NamedCollection & collection) = 0;
    virtual void fromAST(ASTs & args, ContextPtr context, bool with_structure) = 0;

    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";
};

using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;

}
