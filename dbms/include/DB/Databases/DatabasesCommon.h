#pragma once

#include <DB/Core/Types.h>
#include <DB/Parsers/IAST.h>
#include <DB/Storages/IStorage.h>
#include <DB/Databases/IDatabase.h>

/// Общая функциональность для нескольких разных движков баз данных.

namespace DB
{


/** Получить строку с определением таблицы на основе запроса CREATE.
  * Она представляет собой запрос ATTACH, который можно выполнить для создания таблицы, находясь в нужной БД.
  * См. реализацию.
  */
String getTableDefinitionFromCreateQuery(const ASTPtr & query);


/** Создать таблицу по её определению, без использования InterpreterCreateQuery.
  *  (InterpreterCreateQuery обладает более сложной функциональностью, и его нельзя использовать, если БД ещё не создана)
  * Возвращает имя таблицы и саму таблицу.
  */
std::pair<String, StoragePtr> createTableFromDefinition(
	const String & definition,
	const String & database_name,
	const String & database_data_path,
	Context & context,
	bool has_force_restore_data_flag,
	const String & description_for_error_message);


/// Copies list of tables and iterates through such snapshot.
class DatabaseSnaphotIterator : public IDatabaseIterator
{
private:
	Tables tables;
	Tables::iterator it;

public:
	DatabaseSnaphotIterator(Tables & tables_)
		: tables(tables_), it(tables.begin()) {}

	void next() override
	{
		++it;
	}

	bool isValid() const override
	{
		return it != tables.end();
	}

	const String & name() const override
	{
		return it->first;
	}

	StoragePtr & table() const override
	{
		return it->second;
	}
};

}
