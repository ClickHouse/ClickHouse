#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>

namespace DB
{
/*
 * function(source, format, structure) - creates a temporary storage from formated source
 */
class ITableFunctionFileLike : public ITableFunction
{
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const override;
    virtual StoragePtr getStorage(
        const String & source, const String & format, const Block & sample_block, Context & global_context) const = 0;
};
}
