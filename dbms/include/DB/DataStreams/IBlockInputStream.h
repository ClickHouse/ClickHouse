#ifndef DBMS_DATA_STREAMS_IBLOCKINPUTSTREAM_H
#ifndef DBMS_DATA_STREAMS_IBLOCKINPUTSTREAM_H

#include <ostream>

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

	/** Прочитать следующий блок.
	  * Если блоков больше нет - вернуть пустой блок (для которого operator bool возвращает false).
	  */
	virtual Block read() = 0;

	virtual ~IBlockInputStream() {}
};

}

#endif
