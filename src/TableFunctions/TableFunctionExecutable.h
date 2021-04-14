#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>


namespace DB
{
class Context;
/* executable(path, format, structure, input_query) - creates a temporary storage from executable file
 *
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionExecutable : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "executable";
    std::string getName() const override
    {
        return name;
    }

private:
    StoragePtr getStorage(
        const String & source, const String & format, const ColumnsDescription & columns, Context & global_context, const std::string & table_name, const std::string & compression_method) const override;
    const char * getStorageTypeName() const override { return "Executable"; }
};}
