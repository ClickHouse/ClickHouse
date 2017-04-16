#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/** Lets you get a table function by its name.
  */
class TableFunctionFactory
{
public:
    TableFunctionPtr get(
        const std::string & name,
        const Context & context) const;
};

}
