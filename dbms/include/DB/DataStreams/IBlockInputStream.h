#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для чтения данных по блокам из БД.
  * Реляционные операции предполагается делать также реализациями этого интерфейса.
  */
class IBlockInputStream
{
public:
	typedef SharedPtr<IBlockInputStream> BlockInputStreamPtr;
	typedef std::vector<BlockInputStreamPtr> BlockInputStreams;
	
	/** Прочитать следующий блок.
	  * Если блоков больше нет - вернуть пустой блок (для которого operator bool возвращает false).
	  */
	virtual Block read() = 0;

	virtual ~IBlockInputStream() {}

	/** Для вывода дерева преобразований потока данных (плана выполнения запроса).
	  */
	virtual String getName() const = 0;

	/** Создать копию объекта.
	  * Предполагается, что функция вызывается только до использования объекта (сразу после создания, до вызова других методов),
	  *  только для того, чтобы можно было преобразовать параметр, переданный по ссылке в shared ptr.
	  */
	virtual BlockInputStreamPtr clone() = 0;

	BlockInputStreams & getChildren() { return children; }
	void dumpTree(std::ostream & ostr, size_t indent = 0);

protected:
	BlockInputStreams children;
};


typedef IBlockInputStream::BlockInputStreamPtr BlockInputStreamPtr;
typedef IBlockInputStream::BlockInputStreams BlockInputStreams;

}

