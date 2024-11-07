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

    static constexpr auto type = ObjectStorageType::HDFS;
    static constexpr auto type_name = "hdfs";
    static constexpr auto engine_name = "HDFS";
    /// All possible signatures for HDFS engine with structure argument (for example for hdfs table function).
    static constexpr auto max_number_of_arguments_with_structure = 4;
    static constexpr auto signatures_with_structure =
        " - uri\n"
        " - uri, format\n"
        " - uri, format, structure\n"
        " - uri, format, structure, compression_method\n";

    /// All possible signatures for HDFS engine without structure argument (for example for HS table engine).
    static constexpr auto max_number_of_arguments_without_structure = 3;
    static constexpr auto signatures_without_structure =
        " - uri\n"
        " - uri, format\n"
        " - uri, format, compression_method\n";

    StorageHDFSConfiguration() = default;
    StorageHDFSConfiguration(const StorageHDFSConfiguration & other);

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }

    std::string getSignatures(bool with_structure = true) const { return with_structure ? signatures_with_structure : signatures_without_structure; }
    size_t getMaxNumberOfArguments(bool with_structure = true) const { return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure; }

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

    void addStructureAndFormatToArgsIfNeeded(
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
