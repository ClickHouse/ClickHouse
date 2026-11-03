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
class StorageWebConfiguration;

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
    bool hasUnreplicatedTableDataOnDisk() const override;
    void checkTableCreationSettings(const ContextPtr & context, bool is_fresh_definition) const override;
    void setPartitionBy(const ASTPtr & partition_by_) override;

    static void updateStructureAndFormatArgumentsIfNeeded(
        ASTs & args,
        const String & structure_,
        const String & format_,
        const ContextPtr & context,
        bool with_structure,
        bool is_replayed_definition = false)
    {
        if (auto collection = tryGetNamedCollectionWithOverrides(args, context, true, nullptr, nullptr, nullptr, is_replayed_definition))
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

    void checkExperimentalURLWildcardFromIndexPages() const;

    std::shared_ptr<StorageWebConfiguration> createWebObjectStorageConfiguration(
        const String & source,
        const String & format_,
        const String & structure_,
        const String & compression_method_,
        ContextPtr context) const;

    StorageURL::Configuration configuration;

    /// When the URL scheme maps to another backend (file://, s3://, az://, hdfs://, ...), the `url`
    /// table function acts as a thin wrapper that delegates to the corresponding table function.
    /// It is null when the scheme is handled by StorageURL itself (http, https, ...).
    TableFunctionPtr delegate;
    const char * delegate_engine_name = "URL";
    /// Source URI used for the access check of the delegated function (e.g. the file path or the
    /// resolved object-storage URL). Empty for `file://`, matching `TableFunctionFile` behavior.
    String delegate_function_uri;
    /// Whether the URL argument was a relative reference resolved against the `url_base` setting.
    bool is_relative_url = false;
    /// `allow_url_wildcard_from_index_pages` of the session that parsed the arguments. It is recorded
    /// here because `getStorage` is handed the global context for a table definition, whose settings are
    /// the server defaults, while this experimental gate is a session setting: it has to be read from the
    /// query that asked for the reader, not from the context the storage happens to be built in.
    bool allow_url_wildcard_from_index_pages = false;
    /// `allow_archive_path_syntax` of the same session, for the same reason: it decides whether the source
    /// is an archive, and that decision has to be the one the arguments were parsed with.
    bool allow_archive_path_syntax = false;
    /// Whether the storage is being built for a table definition rather than for an ad-hoc query. Such a
    /// definition was validated when it was created (`checkTableCreationSettings`), and refusing it again
    /// would block attaching or loading the table, so the experimental gate does not run for it - the rule
    /// `registerStorageURL` applies with `args.mode <= LoadingStrictnessLevel::CREATE`.
    mutable bool is_stored_definition = false;

private:
    /// Build `delegate` for a non-URL scheme target by constructing and parsing the delegate
    /// table function (`file`, `s3`, `azureBlobStorage`, `hdfs`) from the already-parsed arguments.
    void buildDelegate(URLSchemeTarget target, const ContextPtr & context);

    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return delegate_engine_name; }
    const String & getFunctionURI() const override { return delegate ? delegate_function_uri : filename; }

    std::optional<String> tryGetFormatFromFirstArgument() override;
};

}
