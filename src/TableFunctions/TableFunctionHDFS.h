#pragma once

#include <Common/config.h>

#if USE_HDFS

#include <TableFunctions/ITableFunctionFileLike.h>


namespace DB
{

class Context;

/* hdfs(URI, format[, structure, compression]) - creates a temporary storage from hdfs files
 *
 */
class TableFunctionHDFS : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "hdfs";
    std::string getName() const override
    {
        return name;
    }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

private:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_) const override;
    const char * getStorageTypeName() const override { return "HDFS"; }
};

}

#endif
