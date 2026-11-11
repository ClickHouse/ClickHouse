#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/** filesystem('path') - recursively iterates the path and represents the result as a table,
  * allowing to get files' metadata and, optionally, contents.
  */
class TableFunctionFilesystem : public ITableFunction
{
public:
    static constexpr auto name = "filesystem";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

protected:
    String path;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const override;

    /// Override directly because `Filesystem` is not registered as a storage engine
    /// (the storage is an implementation detail of this table function).
    std::optional<AccessTypeObjects::Source> getSourceAccessObject() const override
    {
        return AccessTypeObjects::Source::FILE;
    }

private:
    /// Unused: storage is constructed directly by `executeImpl`, not via `StorageFactory`.
    const char * getStorageEngineName() const override { return ""; }

    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr /* context */,
        const std::string & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const override;
};

}
