#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/** Позволяет получить табличную функцию по ее имени.
  */
class TableFunctionFactory
{
public:
    TableFunctionPtr get(
        const std::string & name,
        const Context & context) const;
};

}
