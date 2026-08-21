#pragma once
#include "config.h"

#if USE_HDFS
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{
struct HDFSStorageParsedArguments : private StorageParsedArguments
{
    friend class StorageHDFSConfiguration;
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
    static constexpr std::string getSignatures(bool with_structure = true)
    {
        return with_structure ? signatures_with_structure : signatures_without_structure;
    }
    static constexpr size_t getMaxNumberOfArguments(bool with_structure = true)
    {
        return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure;
    }

    void fromNamedCollection(const NamedCollection & collection, ContextPtr context);
    void fromAST(ASTs & args, ContextPtr context, bool with_structure);

    std::string url_str;
};

class StorageHDFSConfiguration : public StorageObjectStorageConfiguration
{
public:
    static constexpr auto type = ObjectStorageType::HDFS;
    static constexpr auto type_name = "hdfs";
    static constexpr auto engine_name = "HDFS";
    /// All possible signatures for HDFS engine with structure argument (for example for hdfs table function).

    StorageHDFSConfiguration() = default;

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }

    bool supportsPartialPathPrefix() const override { return false; }

    /// Unlike s3 and azure, which are object storages,
    /// hdfs is a filesystem, so it cannot list files by partial prefix,
    /// only by directory.
    /// Therefore in the below methods we use supports_partial_prefix=false.
    Path getRawPath() const override { return path; }
    const String & getRawURI() const override { return url; }

    const Paths & getPaths() const override { return paths; }
    void setPaths(const Paths & paths_) override
    {
        paths = paths_;
    }

    String getNamespace() const override { return ""; }
    String getDataSourceDescription() const override { return url; }
    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr &) const override;

    void check(ContextPtr context) override;

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback /*refresh_credentials_callback*/) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure) override;

private:
    void initializeFromParsedArguments(const HDFSStorageParsedArguments & parsed_arguments);
    void setURL(const std::string & url_);
    void fromAST(ASTs & args, ContextPtr, bool /* with_structure */) override;

    void fromNamedCollection(const NamedCollection &, ContextPtr context) override;

    String url;
    Path path;
    Paths paths;
};
}

#endif
