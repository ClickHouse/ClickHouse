#ifndef DBMS_STORAGES_ISTORAGE_H
#define DBMS_STORAGES_ISTORAGE_H

//#include <boost/property_tree/ptree.hpp>

#include <Poco/SharedPtr.h>

#include <DB/Core/ColumnNames.h>
#include <DB/Core/Exception.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>

#define DEFAULT_BLOCK_SIZE 1048576


namespace DB
{

typedef char ptree;	/// временная заглушка, вместо boost::property_tree::ptree
//using boost::property_tree::ptree;
using Poco::SharedPtr;

/** Хранилище. Отвечает за:
  * - хранение данных таблицы;
  * - определение, в каком файле (или не файле) хранятся данные;
  * - поиск данных и обновление данных;
  * - структура хранения данных (сжатие, etc.)
  * - конкуррентный доступ к данным (блокировки, etc.)
  */
class IStorage
{
public:
	/// Основное имя типа таблицы (например, StorageWithoutKey).
	virtual std::string getName() const = 0;

	/** Читать набор столбцов из таблицы.
	  * Принимает список столбцов, которых нужно прочитать, а также описание запроса,
	  *  из которого может быть извлечена информация о том, каким способом извлекать данные
	  *  (индексы, блокировки и т. п.)
	  * Возвращает объект, с помощью которого можно последовательно читать данные.
	  */
	virtual SharedPtr<IBlockInputStream> read(
		const ColumnNames & column_names,
		const ptree & query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE)
	{
		throw Exception("Method read() is not supported by storage " + getName());
	}

	/** Пишет данные в таблицу.
	  * Принимает описание запроса, в котором может содержаться информация о методе записи данных.
	  * Возвращает объект, с помощью которого можно последовательно писать данные.
	  */
	virtual SharedPtr<IBlockOutputStream> write(
		const ptree & query)
	{
		throw Exception("Method write() is not supported by storage " + getName());
	}

	virtual ~IStorage() {}
};

}

#endif
