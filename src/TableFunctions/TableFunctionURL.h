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

    /// When the URL scheme dispatches to another engine (file://, s3://, ...), these are forwarded
    /// to the delegate table function so that schema hints and capabilities are reported correctly.
    bool needStructureHint() const override;
    void setStructureHint(const ColumnsDescription & structure_hint_) override;
    bool supportsReadingSubsetOfColumns(const ContextPtr & context) override;
    NameSet getVirtualsToCheckBeforeUsingStructureHint() const override;
    bool hasStaticStructure() const override;
    void setPartitionBy(const ASTPtr & partition_by_) override;

    static void updateStructureAndFormatArgumentsIfNeeded(ASTs & args, const String & structure_, const String & format_, const ContextPtr & context, bool with_structure)
    {
        if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
        {
            /// In case of named collection, just add key-value pairs "format='...', structure='...'"
            /// at the end of arguments to override existed format and structure with "auto" values.
            if (collection->getOrDefault<String>("format", "auto") == "auto")
            {
                ASTs format_equal_func_args = {make_intrusive<ASTIdentifier>("format"), make_intrusive<ASTLiteral>(format_)};
                auto format_equal_func = makeASTOperator("equals", std::move(format_equal_func_args));
                args.push_back(format_equal_func);
            }
            if (with_structure && collection->getOrDefault<String>("structure", "auto") == "auto")
            {
                ASTs structure_equal_func_args = {make_intrusive<ASTIdentifier>("structure"), make_intrusive<ASTLiteral>(structure_)};
                auto structure_equal_func = makeASTOperator("equals", std::move(structure_equal_func_args));
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

    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    StorageURL::Configuration configuration;

    /// When the URL scheme maps to another backend (file://, s3://, az://, hdfs://, ...), the `url`
    /// table function acts as a thin wrapper that delegates to the corresponding table function.
    /// It is null when the scheme is handled by StorageURL itself (http, https, ...).
    TableFunctionPtr delegate;
    const char * delegate_engine_name = "URL";

private:
    /// Build `delegate` for a non-URL scheme target by constructing and parsing the delegate
    /// table function (`file`, `s3`, `azureBlobStorage`, `hdfs`) from the already-parsed arguments.
    void buildDelegate(URLSchemeTarget target, const ContextPtr & context);

    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return delegate_engine_name; }
    const String & getFunctionURI() const override { return filename; }

    std::optional<String> tryGetFormatFromFirstArgument() override;
};

}
