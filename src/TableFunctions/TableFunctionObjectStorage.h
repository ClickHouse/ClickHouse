#pragma once

#include "config.h"
#include <TableFunctions/ITableFunction.h>
#include <Formats/FormatFactory.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class Context;
class StorageS3Configuration;
class StorageAzureConfiguration;
class StorageHDFSConfiguration;
struct S3StorageSettings;
struct AzureStorageSettings;
struct HDFSStorageSettings;

struct AzureDefinition
{
    static constexpr auto name = "azureBlobStorage";
    static constexpr auto storage_type_name = "Azure";
    static constexpr auto signature = " - connection_string, container_name, blobpath\n"
                                      " - connection_string, container_name, blobpath, structure \n"
                                      " - connection_string, container_name, blobpath, format \n"
                                      " - connection_string, container_name, blobpath, format, compression \n"
                                      " - connection_string, container_name, blobpath, format, compression, structure \n"
                                      " - storage_account_url, container_name, blobpath, account_name, account_key\n"
                                      " - storage_account_url, container_name, blobpath, account_name, account_key, structure\n"
                                      " - storage_account_url, container_name, blobpath, account_name, account_key, format\n"
                                      " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression\n"
                                      " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure\n";
    static constexpr auto max_number_of_arguments = 8;
};

struct S3Definition
{
    static constexpr auto name = "s3";
    static constexpr auto storage_type_name = "S3";
    static constexpr auto signature = " - url\n"
                                      " - url, format\n"
                                      " - url, format, structure\n"
                                      " - url, format, structure, compression_method\n"
                                      " - url, access_key_id, secret_access_key\n"
                                      " - url, access_key_id, secret_access_key, session_token\n"
                                      " - url, access_key_id, secret_access_key, format\n"
                                      " - url, access_key_id, secret_access_key, session_token, format\n"
                                      " - url, access_key_id, secret_access_key, format, structure\n"
                                      " - url, access_key_id, secret_access_key, session_token, format, structure\n"
                                      " - url, access_key_id, secret_access_key, format, structure, compression_method\n"
                                      " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method\n"
                                      "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";
    static constexpr auto max_number_of_arguments = 8;
};

struct GCSDefinition
{
    static constexpr auto name = "gcs";
    static constexpr auto storage_type_name = "GCS";
    static constexpr auto signature = S3Definition::signature;
    static constexpr auto max_number_of_arguments = S3Definition::max_number_of_arguments;
};

struct COSNDefinition
{
    static constexpr auto name = "cosn";
    static constexpr auto storage_type_name = "COSN";
    static constexpr auto signature = S3Definition::signature;
    static constexpr auto max_number_of_arguments = S3Definition::max_number_of_arguments;
};

struct OSSDefinition
{
    static constexpr auto name = "oss";
    static constexpr auto storage_type_name = "OSS";
    static constexpr auto signature = S3Definition::signature;
    static constexpr auto max_number_of_arguments = S3Definition::max_number_of_arguments;
};

struct HDFSDefinition
{
    static constexpr auto name = "hdfs";
    static constexpr auto storage_type_name = "HDFS";
    static constexpr auto signature = " - uri\n"
                                      " - uri, format\n"
                                      " - uri, format, structure\n"
                                      " - uri, format, structure, compression_method\n";
    static constexpr auto max_number_of_arguments = 4;
};

template <typename Definition, typename Configuration>
class TableFunctionObjectStorage : public ITableFunction
{
public:
    static constexpr auto name = Definition::name;
    static constexpr auto signature = Definition::signature;

    static size_t getMaxNumberOfArguments() { return Definition::max_number_of_arguments; }

    String getName() const override { return name; }

    virtual String getSignature() const { return signature; }

    bool hasStaticStructure() const override { return configuration->structure != "auto"; }

    bool needStructureHint() const override { return configuration->structure == "auto"; }

    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

    bool supportsReadingSubsetOfColumns(const ContextPtr & context) override
    {
        return configuration->format != "auto" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context);
    }

    std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const override
    {
        return VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
    }

    virtual void parseArgumentsImpl(ASTs & args, const ContextPtr & context)
    {
        StorageObjectStorage::Configuration::initialize(*getConfiguration(), args, context, true);
    }

    static void updateStructureAndFormatArgumentsIfNeeded(
      ASTs & args,
      const String & structure,
      const String & format,
      const ContextPtr & context)
    {
        Configuration().addStructureAndFormatToArgs(args, structure, format, context);
    }

protected:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return Definition::storage_type_name; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ObjectStoragePtr getObjectStorage(const ContextPtr & context, bool create_readonly) const;
    ConfigurationPtr getConfiguration() const;

    mutable ConfigurationPtr configuration;
    mutable ObjectStoragePtr object_storage;
    ColumnsDescription structure_hint;

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;
};

#if USE_AWS_S3
using TableFunctionS3 = TableFunctionObjectStorage<S3Definition, StorageS3Configuration>;
#endif

#if USE_AZURE_BLOB_STORAGE
using TableFunctionAzureBlob = TableFunctionObjectStorage<AzureDefinition, StorageAzureConfiguration>;
#endif

#if USE_HDFS
using TableFunctionHDFS = TableFunctionObjectStorage<HDFSDefinition, StorageHDFSConfiguration>;
#endif
}
