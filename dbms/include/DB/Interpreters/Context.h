#pragma once

#include <map>
#include <set>

#include <Poco/SharedPtr.h>
#include <Poco/Mutex.h>

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/IStorage.h>
#include <DB/Functions/IFunction.h>
#include <DB/DataTypes/DataTypeFactory.h>


namespace DB
{

using Poco::SharedPtr;

/// имя функции -> функция
typedef std::map<String, FunctionPtr> Functions;

/// имя таблицы -> таблица
typedef std::map<String, StoragePtr> Tables;

/// имя БД -> таблицы
typedef std::map<String, Tables> Databases;


/** Набор известных объектов, которые могут быть использованы в запросе.
  */
struct Context
{
	String path;									/// Путь к директории с данными, со слешем на конце.
	SharedPtr<Databases> databases;					/// Список БД и таблиц в них.
	String current_database;						/// Текущая БД.
	SharedPtr<Functions> functions;					/// Обычные функции.
	SharedPtr<DataTypeFactory> data_type_factory;	/// Типы данных.
	NamesAndTypes columns;							/// Столбцы текущей обрабатываемой таблицы.

	SharedPtr<Poco::FastMutex> mutex;	/// Для доступа и модификации разделяемых объектов.

	Context() : databases(new Databases), functions(new Functions), mutex(new Poco::FastMutex) {}
};


}
