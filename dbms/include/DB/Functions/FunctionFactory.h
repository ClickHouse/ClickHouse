#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Functions/IFunction.h>


namespace DB
{

class Context;


/** Позволяет получить функцию по имени.
  * Функция при создании также может использовать для инициализации (например, захватить SharedPtr)
  *  какие-нибудь справочники, находящиеся в Context-е.
  */
class FunctionFactory
{
public:
	FunctionPtr get(
		const String & name,
		const Context & context) const;
};

}
