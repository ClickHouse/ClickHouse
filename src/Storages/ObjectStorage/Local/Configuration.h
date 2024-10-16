#pragma once

#include <memory>
#include "Disks/ObjectStorages/Local/LocalObjectStorage.h"

#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

class StorageLocalConfiguration : public StorageObjectStorage::Configuration
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    static constexpr auto type_name = "local";
    /// All possible signatures for Local engine with structure argument (for example for local table function).
    static constexpr auto max_number_of_arguments_with_structure = 4;
    static constexpr auto signatures_with_structure =
        " - path\n"
        " - path, format\n"
        " - path, format, structure\n"
        " - path, format, structure, compression_method\n";

    /// All possible signatures for S3 engine without structure argument (for example for Local table engine).
    static constexpr auto max_number_of_arguments_without_structure = 3;
    static constexpr auto signatures_without_structure =
        " - path\n"
        " - path, format\n"
        " - path, format, compression_method\n";

    StorageLocalConfiguration() = default;
    StorageLocalConfiguration(const StorageLocalConfiguration & other) = default;

    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return "Local"; }

    std::string getSignatures(bool with_structure = true) const { return with_structure ? signatures_with_structure : signatures_without_structure; }
    size_t getMaxNumberOfArguments(bool with_structure = true) const { return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure; }

    Path getPath() const override { return path; }
    void setPath(const Path & path_) override { path = path_; }

    const Paths & getPaths() const override { return paths; }
    void setPaths(const Paths & paths_) override { paths = paths_; }

    String getNamespace() const override { return ""; }
    String getDataSourceDescription() const override { return ""; }
    StorageObjectStorage::QuerySettings getQuerySettings(const ContextPtr &) const override;

    ConfigurationPtr clone() override { return std::make_shared<StorageLocalConfiguration>(*this); }

    ObjectStoragePtr createObjectStorage(ContextPtr, bool) override { return std::make_shared<LocalObjectStorage>("/"); }

    void addStructureAndFormatToArgsIfNeeded(ASTs &, const String &, const String &, ContextPtr) override { }

private:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    Path path;
    Paths paths;
};

}
