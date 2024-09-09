#pragma once
#include "config.h"

#if USE_HDFS
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class StorageHDFSConfiguration : public StorageObjectStorage::Configuration
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    static constexpr auto type_name = "hdfs";
    static constexpr auto engine_name = "HDFS";

    StorageHDFSConfiguration() = default;
    StorageHDFSConfiguration(const StorageHDFSConfiguration & other);

    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }

    Path getPath() const override { return path; }
    void setPath(const Path & path_) override { path = path_; }

    const Paths & getPaths() const override { return paths; }
    void setPaths(const Paths & paths_) override { paths = paths_; }
    std::string getPathWithoutGlobs() const override;

    String getNamespace() const override { return ""; }
    String getDataSourceDescription() const override { return url; }
    StorageObjectStorage::QuerySettings getQuerySettings(const ContextPtr &) const override;

    void check(ContextPtr context) const override;
    ConfigurationPtr clone() override { return std::make_shared<StorageHDFSConfiguration>(*this); }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;

    void addStructureAndFormatToArgs(
        ASTs & args,
        const String & structure_,
        const String & format_,
        ContextPtr context) override;

private:
    void fromNamedCollection(const NamedCollection &, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr, bool /* with_structure */) override;
    void setURL(const std::string & url_);

    String url;
    String path;
    std::vector<String> paths;
};

}

#endif
