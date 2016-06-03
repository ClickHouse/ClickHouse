#pragma once

#include <DB/Functions/IFunction.h>
#include <common/singleton.h>


namespace DB
{

class Context;


/** Позволяет получить функцию по имени.
  * Функция при создании также может использовать для инициализации (например, захватить SharedPtr)
  *  какие-нибудь справочники, находящиеся в Context-е.
  */
class FunctionFactory : public Singleton<FunctionFactory>
{
	friend class StorageSystemFunctions;

private:
	typedef IFunction* (*Creator)(const Context & context);	/// Не std::function, так как меньше indirection и размер объекта.
	std::unordered_map<String, Creator> functions;

public:
	FunctionFactory();

	FunctionPtr get(const String & name, const Context & context) const;	/// Кидает исключение, если не нашлось.
	FunctionPtr tryGet(const String & name, const Context & context) const;	/// Возвращает nullptr, если не нашлось.

	template <typename F> void registerFunction()
	{
		static_assert(std::is_same<decltype(&F::create), Creator>::value, "F::create has incorrect type");
		functions[F::name] = &F::create;
	}
};

}
