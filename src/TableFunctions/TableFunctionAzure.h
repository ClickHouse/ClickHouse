#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageAzure.h>


namespace DB
{

class Context;

/* AzureBlob(source, [access_key_id, secret_access_key,] [format, structure, compression]) - creates a temporary storage for a file in AzureBlob.
 */
class TableFunctionAzure : public ITableFunction
{
public:
    static constexpr auto name = "azure_blob";
    static constexpr auto signature = "- connection_url, container, blob, format, structure\n";

    static size_t getMaxNumberOfArguments() { return 5; }

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

    bool supportsReadingSubsetOfColumns() override;

    std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const override
    {
        return {"_path", "_file"};
    }

    virtual void parseArgumentsImpl(ASTs & args, const ContextPtr & context);

    static void addColumnsStructureToArguments(ASTs & args, const String & structure, const ContextPtr & context);

protected:

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "Azure"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    mutable StorageAzure::Configuration configuration;
    ColumnsDescription structure_hint;
};

}

#endif
