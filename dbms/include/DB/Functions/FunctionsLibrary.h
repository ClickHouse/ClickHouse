#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Functions/IFunction.h>


namespace DB
{

/// имя функции -> функция
typedef std::map<String, FunctionPtr> Functions;


/// Библиотека обычных функций.
namespace FunctionsLibrary
{
	/// Получить все функции.
	SharedPtr<Functions> get();
};

}
