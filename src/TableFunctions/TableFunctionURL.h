#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Storages/StorageURL.h>
#include <IO/ReadWriteBufferFromHTTP.h>


namespace DB
{

class Context;

/* url(source, [format, structure, compression]) - creates a temporary storage from url.
 */
class TableFunctionURL : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "url";
    static constexpr auto signature = " - uri\n"
                                      " - uri, format\n"
                                      " - uri, format, structure\n"
                                      " - uri, format, structure, compression_method\n"
                                      "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    String getName() const override
    {
        return name;
    }

    String getSignature() const override
    {
        return signature;
    }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    static void updateStructureAndFormatArgumentsIfNeeded(ASTs & args, const String & structure_, const String & format_, const ContextPtr & context);

protected:
    void parseArguments(const ASTPtr & ast, ContextPtr context) override;
    void parseArgumentsImpl(ASTs & args, const ContextPtr & context) override;

    StorageURL::Configuration configuration;

private:
    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_) const override;

    const char * getStorageTypeName() const override { return "URL"; }

    std::optional<String> tryGetFormatFromFirstArgument() override;
};

}
