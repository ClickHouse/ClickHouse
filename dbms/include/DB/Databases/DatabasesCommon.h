#pragma once

#include <DB/Core/Types.h>
#include <DB/Parsers/IAST.h>
#include <DB/Storages/IStorage.h>


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
	const String & description_for_error_message);


}
