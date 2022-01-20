#pragma once
#include <Common/config.h>
#if 0
#include <TableFunctions/ITableFunction.h>

namespace  DB
{
class Context;
/** 
 * usage: hivecluster(db, table)
 * db.table must be a table that uses hive engine
 */
class TableFunctionHiveCluster : public ITableFunction
{
public:
    static constexpr auto function_name = "hiveCluster";
    static constexpr auto storage_type_name = "hiveCluster";
    std::string getName() const override
    {
        return function_name;
    }

    // The structure is defined by the table storage, not this table function.
    bool hasStaticStructure() const override { return false; }

    StoragePtr executeImpl(
        const ASTPtr & ast_function_,
        ContextPtr context_,
        const std::string & table_name_,
        ColumnDescription /*cached_columns_*/) const override;

    const char * getStorageTypeName() const override
    {
        return storage_type_name;
    }
    ColumnsDescription getActualTableStructrue(ContextPtr) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;

private:
    String db;
    String table_name;
};
} // namespace  DB

#endif
