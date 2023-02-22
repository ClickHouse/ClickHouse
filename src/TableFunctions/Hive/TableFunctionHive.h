#pragma once
#include <Common/config.h>
#if USE_HIVE
#include <TableFunctions/ITableFunction.h>
#include <Poco/Logger.h>
namespace DB
{
class Context;
class TableFunctionHive : public ITableFunction
{
public:
    static constexpr auto name = "hive";
    static constexpr auto storage_type_name = "hive";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return storage_type_name; }
    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;

private:
    Poco::Logger * logger = &Poco::Logger::get("TableFunctionHive");

    String cluster_name;
    String hive_metastore_url;
    String hive_database;
    String hive_table;
    String table_structure;
    String partition_by_def;

    ColumnsDescription actual_columns;
};
}
#endif
