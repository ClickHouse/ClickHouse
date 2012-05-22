#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Defines.h>
#include <DB/Core/Names.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Core/Exception.h>
#include <DB/Core/QueryProcessingStage.h>

#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>

#include <DB/Parsers/IAST.h>


namespace DB
{

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

	/// Имя самой таблицы (например, hits)
	virtual std::string getTableName() const = 0;

	/** Получить список имён и типов столбцов таблицы.
	  */
	virtual const NamesAndTypesList & getColumnsList() const = 0;

	NamesAndTypesMap getColumnsMap() const;
	const DataTypePtr getDataTypeByName(const String & column_name) const;

	/** То же самое, но в виде блока-образца.
	  */
	Block getSampleBlock() const;

	/** Читать набор столбцов из таблицы.
	  * Принимает список столбцов, которых нужно прочитать, а также описание запроса,
	  *  из которого может быть извлечена информация о том, каким способом извлекать данные
	  *  (индексы, блокировки и т. п.)
	  * Возвращает поток с помощью которого можно последовательно читать данные
	  *  или несколько потоков для параллельного чтения данных.
	  * Также в processed_stage записывается, до какой стадии запрос был обработан.
	  * (Обычно функция только читает столбцы из списка, но в других случаях,
	  *  например, запрос может быть частично обработан на удалённом сервере.)
	  *
	  * BlockInputStreams будет содержать по крайней мере один элемент и не больше max_threads элементов.
	  */
	virtual BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned max_threads = 1)
	{
		throw Exception("Method read() is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Пишет данные в таблицу.
	  * Принимает описание запроса, в котором может содержаться информация о методе записи данных.
	  * Возвращает объект, с помощью которого можно последовательно писать данные.
	  */
	virtual BlockOutputStreamPtr write(
		ASTPtr query)
	{
		throw Exception("Method write() is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Удалить данные таблицы. После вызова этого метода, использование объекта некорректно (его можно лишь уничтожить).
	  */
	virtual void drop()
	{
		throw Exception("Method drop() is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** ALTER таблицы в виде изменения столбцов, не затрагивающий изменение Storage или его параметров.
	  * (ALTER, затрагивающий изменение движка, делается внешним кодом, путём копирования данных.)
	  */
	virtual void alter(NamesAndTypesListPtr columns)
	{
		throw Exception("Method alter() is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	virtual ~IStorage() {}

	/** Проверить, что все запрошенные имена есть в таблице и заданы корректно.
	  * (список имён не пустой и имена не повторяются)
	  */
	void check(const Names & column_names) const;

	/** Проверить, что блок с данными для записи содержит все столбцы таблицы с правильными типами,
	  *  и содержит только столбцы таблицы.
	  */
	void check(const Block & block) const;
};

typedef SharedPtr<IStorage> StoragePtr;

}
