#pragma once
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageMergeTreeParts.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/**
 * Reads a set of MergeTree data parts described explicitly by the query, from a disk
 * that is also described by the query.
 */
class TableFunctionMergeTreeParts : public ITableFunction
{
public:
    static constexpr auto name = "mergeTreeParts";

    std::string getName() const override { return name; }

    /// Every argument is a description written as a function call (`structure(...)`, `parts(...)`,
    /// `disk(...)`, `table_settings(...)`) rather than an expression, so none of them is resolvable.
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    /// The data is read from the disk described by the query, so the source to check access for is the
    /// kind of that disk, the same one that `file`, `s3`, ... check.
    std::optional<AccessTypeObjects::Source> getSourceAccessObject() const override;

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    /// `StorageMergeTreeParts` is not registered in `StorageFactory`; `getSourceAccessObject` is
    /// overridden above, so this name is not used to derive the source to check access for.
    const char * getStorageEngineName() const override { return ""; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool/*is_insert_query*/) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

private:
    std::string structure;
    StorageMergeTreeParts::ReadFromPartsInfo read_from_parts_info;
};

}
