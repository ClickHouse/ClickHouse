#pragma once

#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>
#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

namespace DB
{

struct WebStorageParsedArguments : private StorageParsedArguments
{
    friend class StorageWebConfiguration;

    static constexpr auto max_number_of_arguments_with_structure = 4;
    static constexpr auto signatures_with_structure
        = " - url\n"
          " - url, format\n"
          " - url, format, structure\n"
          " - url, format, structure, compression_method\n"
          "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    static constexpr auto max_number_of_arguments_without_structure = max_number_of_arguments_with_structure - 1;
    static constexpr auto signatures_without_structure
        = " - url\n"
          " - url, format\n"
          " - url, format, compression_method\n"
          "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    static constexpr std::string getSignatures(bool with_structure = true)
    {
        return with_structure ? signatures_with_structure : signatures_without_structure;
    }

    static constexpr size_t getMaxNumberOfArguments(bool with_structure = true)
    {
        return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure;
    }

    String url;
    HTTPHeaderEntries headers_from_ast;

public:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context);
    void fromAST(ASTs & args, ContextPtr context, bool with_structure);
};

class StorageWebConfiguration : public StorageObjectStorageConfiguration
{
public:
    static constexpr auto type = ObjectStorageType::Web;
    static constexpr auto type_name = "web";
    static constexpr auto engine_name = "URL";

    StorageWebConfiguration() = default;

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }
    std::string getNamespaceType() const override { return "host"; }

    Path getRawPath() const override { return path; }
    const String & getRawURI() const override { return raw_url; }

    bool supportsPartialPathPrefix() const override { return false; }

    const Paths & getPaths() const override { return paths; }
    void setPaths(const Paths & paths_) override { paths = paths_; }

    String getNamespace() const override { return namespace_prefix; }
    String getDataSourceDescription() const override { return raw_url; }

    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr & context) const override;

    void check(ContextPtr context) override;

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args,
        const String & structure_,
        const String & format_,
        ContextPtr context,
        bool with_structure) override;

protected:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override;

private:
    void initializeFromParsedArguments(WebStorageParsedArguments && parsed_arguments);
    void setNamespaceFromURL();

    String raw_url;
    String base_url;
    String query_fragment;
    String namespace_prefix;
    Path path;
    Paths paths;
    HTTPHeaderEntries headers_from_ast;
};

}
