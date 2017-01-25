#pragma once

#include <string>
#include <memory>


namespace DB
{

class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;


/** Интерфейс для табличных функций.
  *
  * Табличные функции не имеют отношения к другим функциям.
  * Табличная функция может быть указана в секции FROM вместо [db.]table
  * Табличная функция возвращает временный объект StoragePtr, который используется для выполнения запроса.
  *
  * Пример:
  * SELECT count() FROM remote('example01-01-1', merge, hits)
  * - пойти на example01-01-1, в БД merge, таблицу hits.
  */

class ITableFunction
{
public:
	/// Получить основное имя функции.
	virtual std::string getName() const = 0;

	/// Создать storage в соответствии с запросом
	virtual StoragePtr execute(ASTPtr ast_function, Context & context) const = 0;

	virtual ~ITableFunction() {};
};

using TableFunctionPtr = std::shared_ptr<ITableFunction>;


}
