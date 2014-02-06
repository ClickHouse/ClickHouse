#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Storages/StoragePtr.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Interpreters/Context.h>

namespace DB
{

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

protected:
	/// Сгенерировать уникальное имя для временной таблицы.
	std::string chooseName () const {
		String result = "TemproraryTable" + getName() + "Id";
		for (size_t i = 0; i < 10; ++i)
		{
			int x = rand() % 62;
			char now;
			if (x < 10)
				now = '0' + rand() % 10;
			else if (x < 36)
				now = 'a' + x - 10;
			else
				now = 'A' + x - 36;

			result += now;
		}
		return result;
	}
};

typedef SharedPtr<ITableFunction> TableFunctionPtr;


}
