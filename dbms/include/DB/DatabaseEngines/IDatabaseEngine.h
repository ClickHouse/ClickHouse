#pragma once

#include <DB/Core/Types.h>
#include <DB/Parsers/IAST.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

/** Позволяет проитерироваться по списку таблиц.
  */
class IDatabaseIterator
{
public:
	virtual StoragePtr next() = 0;
	virtual ~IDatabaseIterator() {}
};

using DatabaseIteratorPtr = std::unique_ptr<IDatabaseIterator>;


/** Движок баз данных.
  * Отвечает за:
  * - инициализацию множества таблиц;
  * - проверку существования и получение таблицы для работы;
  * - получение списка всех таблиц;
  * - создание и удаление таблиц;
  * - переименовывание таблиц и перенос между БД с одинаковыми движками.
  */

class IDatabaseEngine : protected std::enable_shared_from_this<IDatabaseEngine>
{
public:
	/// Проверить существование таблицы.
	virtual bool isTableExist(const String & name) const = 0;

	/// Получить таблицу для работы. Кинуть исключение, если таблицы не существует.
	virtual StoragePtr getTable(const String & name) = 0;

	/// Получить итератор, позволяющий перебрать все таблицы.
	/// Допустимо наличие "скрытых" таблиц, которые не видны при переборе, но видны, если получать их по имени, используя функции выше.
	virtual DatabaseIteratorPtr getIterator() = 0;

	/// Добавить таблицу в базу данных.
	virtual void addTable(const String & name, StoragePtr table) = 0;

	/// Убрать таблицу из базы данных и вернуть её.
	virtual StoragePtr detachTable(const String & name) = 0;

	/// Получить запрос CREATE TABLE для таблицы.
	virtual ASTPtr getCreateQuery(const String & name) const = 0;

	virtual ~IDatabaseEngine() {}
};

using DatabaseEnginePtr = std::shared_ptr<IDatabaseEngine>;

}
