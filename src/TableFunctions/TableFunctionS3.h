#pragma once

#include "config.h"

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageS3.h>


namespace DB
{

class Context;

/* s3(source, [access_key_id, secret_access_key,] [format, structure, compression]) - creates a temporary storage for a file in S3.
 */
class TableFunctionS3 : public ITableFunction
{
public:
    static constexpr auto name = "s3";
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

    static size_t getMaxNumberOfArguments() { return 6; }

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

    const char * getStorageTypeName() const override { return "S3"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    mutable StorageS3::Configuration configuration;
    ColumnsDescription structure_hint;

private:

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;
};

}

#endif
