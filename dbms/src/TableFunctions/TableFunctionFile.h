#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>


namespace DB
{
/* file(path, format, structure) - creates a temporary storage from file
 *
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

private:
    StoragePtr getStorage(
        const String & source, const String & format, const Block & sample_block, Context & global_context) const override;
};
}
