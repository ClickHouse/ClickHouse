#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Storages/StorageURL.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>


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

    static void updateStructureAndFormatArgumentsIfNeeded(ASTs & args, const String & structure_, const String & format_, const ContextPtr & context, bool with_structure)
    {
        if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
        {
            /// In case of named collection, just add key-value pairs "format='...', structure='...'"
            /// at the end of arguments to override existed format and structure with "auto" values.
            if (collection->getOrDefault<String>("format", "auto") == "auto")
            {
                ASTs format_equal_func_args = {std::make_shared<ASTIdentifier>("format"), std::make_shared<ASTLiteral>(format_)};
                auto format_equal_func = makeASTFunction("equals", std::move(format_equal_func_args));
                args.push_back(format_equal_func);
            }
            if (with_structure && collection->getOrDefault<String>("structure", "auto") == "auto")
            {
                ASTs structure_equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
                auto structure_equal_func = makeASTFunction("equals", std::move(structure_equal_func_args));
                args.push_back(structure_equal_func);
            }
        }
        else
        {
            /// If arguments contain headers, just remove it and add to the end of arguments later.
            HTTPHeaderEntries tmp_headers;
            size_t count = StorageURL::evalArgsAndCollectHeaders(args, tmp_headers, context);
            ASTPtr headers_ast;
            if (count != args.size())
            {
                chassert(count + 1 == args.size());
                headers_ast = args.back();
                args.pop_back();
            }

            ITableFunctionFileLike::updateStructureAndFormatArgumentsIfNeeded(args, structure_, format_, context, with_structure);

            if (headers_ast)
                args.push_back(headers_ast);
        }
    }

protected:
    void parseArguments(const ASTPtr & ast, ContextPtr context) override;
    void parseArgumentsImpl(ASTs & args, const ContextPtr & context) override;

    StorageURL::Configuration configuration;

private:
    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "URL"; }

    std::optional<String> tryGetFormatFromFirstArgument() override;
};

}
