#pragma once

#include <memory>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <filesystem>


namespace DB
{

struct LocalStorageParsedArguments : private StorageParsedArguments
{
    friend class StorageLocalConfiguration;
    static constexpr auto max_number_of_arguments_with_structure = 4;
    static constexpr auto signatures_with_structure =
        " - path\n"
        " - path, format\n"
        " - path, format, structure\n"
        " - path, format, structure, compression_method\n";

    /// All possible signatures for S3 engine without structure argument (for example for Local table engine).
    static constexpr auto max_number_of_arguments_without_structure = max_number_of_arguments_with_structure - 1;
    static constexpr auto signatures_without_structure =
        " - path\n"
        " - path, format\n"
        " - path, format, compression_method\n";

    static constexpr std::string getSignatures(bool with_structure = true)
    {
        return with_structure ? signatures_with_structure : signatures_without_structure;
    }
    static constexpr size_t getMaxNumberOfArguments(bool with_structure = true)
    {
        return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure;
    }

    using Paths = StorageObjectStorageConfiguration::Paths;
    using Path = StorageObjectStorageConfiguration::Path;
    String path;
    String path_suffix;
    void fromNamedCollection(const NamedCollection & collection, ContextPtr);
    void fromDisk(DiskPtr disk, ASTs & args, ContextPtr context, bool with_structure);
    void fromAST(ASTs & args, ContextPtr context, bool with_structure);
    StorageParsedArguments extractBaseArguments();
};

class StorageLocalConfiguration : public StorageObjectStorageConfiguration
{
public:
    static constexpr auto type = ObjectStorageType::Local;
    static constexpr auto type_name = "local";
    /// All possible signatures for Local engine with structure argument (for example for local table function).
    StorageLocalConfiguration() = default;
    StorageLocalConfiguration(const StorageLocalConfiguration & other) = default;
    explicit StorageLocalConfiguration(const String & path_, const String & disk_name_ = "");

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return "Local"; }

    Path getRawPath() const override { return path; }
    void setRawPath(const Path & p) override { path = p; }
    const String & getRawURI() const override { return path.path; }

    String getNamespace() const override { return ""; }
    String getDataSourceDescription() const override { return ""; }
    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr &) const override;

    ObjectStoragePtr
    createObjectStorageImpl(ContextPtr, bool readonly, CredentialsConfigurationCallback /*refresh_credentials_callback*/) override
    {
        return std::make_shared<LocalObjectStorage>(LocalObjectStorageSettings(disk_name, "/", readonly));
    }

    void addStructureAndFormatToArgsIfNeeded(ASTs &, const String &, const String &, ContextPtr, bool) override { }

private:
    String disk_name;
    Path path;
};

ConfigWithOptions fromLocalAST(ASTs & args, ContextPtr context, bool with_structure);
ConfigWithOptions fromLocalDisk(const String & disk_name_, ASTs & args, ContextPtr context, bool with_structure);
ConfigWithOptions fromLocalNamedCollection(const NamedCollection & collection, ContextPtr context);

}
