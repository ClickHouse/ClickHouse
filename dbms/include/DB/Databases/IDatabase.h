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
	virtual void next() = 0;
	virtual bool isValid() const = 0;

	virtual const String & name() const = 0;
	virtual StoragePtr & table() const = 0;

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

class IDatabase : public std::enable_shared_from_this<IDatabase>
{
public:
	/// Получить имя движка базы данных.
	virtual String getEngineName() const = 0;

	/// Проверить существование таблицы.
	virtual bool isTableExist(const String & name) const = 0;

	/// Получить таблицу для работы. Вернуть nullptr, если таблицы нет.
	virtual StoragePtr tryGetTable(const String & name) = 0;

	/// Получить итератор, позволяющий перебрать все таблицы.
	/// Допустимо наличие "скрытых" таблиц, которые не видны при переборе, но видны, если получать их по имени, используя функции выше.
	virtual DatabaseIteratorPtr getIterator() = 0;

	/// Является ли БД пустой.
	virtual bool empty() const = 0;

	/// Добавить таблицу в базу данных. Прописать её наличие в метаданных.
	virtual void createTable(const String & name, const StoragePtr & table, const ASTPtr & query, const String & engine) = 0;

	/// Удалить таблицу из базы данных и вернуть её. Удалить метаданные.
	virtual void removeTable(const String & name) = 0;

	/// Добавить таблицу в базу данных, но не прописывать её в метаданных. БД может не поддерживать этот метод.
	virtual void attachTable(const String & name, const StoragePtr & table) = 0;

	/// Забыть про таблицу, не удаляя её, и вернуть её. БД может не поддерживать этот метод.
	virtual StoragePtr detachTable(const String & name) = 0;

	/// Переименовать таблицу и, возможно, переместить таблицу в другую БД.
	virtual void renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name) = 0;

	/// Получить запрос CREATE TABLE для таблицы. Может выдавать информацию и для detached таблиц, для которых есть метаданные.
	virtual ASTPtr getCreateQuery(const String & name) const = 0;

	/// Попросить все таблицы завершить фоновые потоки, которые они используют, и удалить все объекты таблиц.
	virtual void shutdown() = 0;

	/// Удалить метаданные, удаление которых отличается от рекурсивного удаления директории, если такие есть.
	virtual void drop() = 0;

	virtual ~IDatabase() {}
};

using DatabasePtr = std::shared_ptr<IDatabase>;
using Databases = std::map<String, DatabasePtr>;

}
