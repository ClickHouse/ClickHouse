#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Core/Types.h>


namespace DB
{

class TableFunctionWindow : public ITableFunction
{
public:
    static constexpr auto name = "window";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const override;
};


}
