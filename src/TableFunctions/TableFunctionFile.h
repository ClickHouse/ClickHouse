#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>


namespace DB
{

/* file(path, format[, structure, compression]) - creates a temporary storage from file
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionFile : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "file";
    std::string getName() const override
    {
        return name;
    }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

private:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const std::string & compression_method_) const override;
    const char * getStorageTypeName() const override { return "File"; }
};

}
