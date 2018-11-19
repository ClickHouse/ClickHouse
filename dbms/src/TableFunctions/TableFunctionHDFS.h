#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>


namespace DB
{
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
        const String & source, const String & format, const Block & sample_block, Context & global_context) const override;
};
}
