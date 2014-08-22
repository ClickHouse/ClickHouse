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
private:
	typedef std::function<IFunction* (const Context & context)> Creator;
	std::unordered_map<String, Creator> functions;

public:
	FunctionFactory();

	FunctionPtr get(const String & name, const Context & context) const;

	void registerFunction(const String & name, Creator creator)
	{
		functions[name] = creator;
	}
};

}
