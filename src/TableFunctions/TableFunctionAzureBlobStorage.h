#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageAzureBlob.h>


namespace DB
{

class Context;

/* AzureBlob(source, [access_key_id, secret_access_key,] [format, compression, structure]) - creates a temporary storage for a file in AzureBlob.
 */
class TableFunctionAzureBlobStorage : public ITableFunction
{
public:
    static constexpr auto name = "azureBlobStorage";

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

    static size_t getMaxNumberOfArguments() { return 8; }

    String getName() const override
    {
        return name;
    }

    virtual String getSignature() const
    {
        return signature;
    }

    bool hasStaticStructure() const override { return configuration.structure != "auto"; }

    bool needStructureHint() const override { return configuration.structure == "auto"; }

    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

    bool supportsReadingSubsetOfColumns(const ContextPtr & context) override;

    std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const override;

    virtual void parseArgumentsImpl(ASTs & args, const ContextPtr & context);

    static void updateStructureAndFormatArgumentsIfNeeded(ASTs & args, const String & structure, const String & format, const ContextPtr & context);

protected:

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "Azure"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    mutable StorageAzureBlob::Configuration configuration;
    ColumnsDescription structure_hint;
};

}

#endif
