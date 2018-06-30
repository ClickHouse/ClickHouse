#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>


namespace DB
{
/* url(source, format, structure) - creates a temporary storage from url
 */
class TableFunctionURL : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "url";
    std::string getName() const override
    {
        return name;
    }

private:
    StoragePtr getStorage(
        const String & source, const String & format, const Block & sample_block, Context & global_context) const override;
};
}
