#pragma once

#include <Common/config.h>

#if USE_HDFS

#include <TableFunctions/ITableFunctionFileLike.h>


namespace DB
{

class Context;

/* hdfs(name_node_ip:name_node_port, format, structure) - creates a temporary storage from hdfs file
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

private:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, Context & global_context,
        const std::string & table_name, const String & compression_method_) const override;
    const char * getStorageTypeName() const override { return "HDFS"; }
};

}

#endif
