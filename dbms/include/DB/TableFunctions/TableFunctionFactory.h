#pragma once

#include <DB/TableFunctions/ITableFunction.h>


namespace DB
{

class Context;
class ITableFunction;


/** Позволяет получить табличную функцию по ее имени.
  */
class TableFunctionFactory
{
public:
	TableFunctionPtr get(
		const String & name,
		const Context & context) const;
};

}
