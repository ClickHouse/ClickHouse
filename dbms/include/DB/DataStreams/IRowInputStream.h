#pragma once

#include <boost/noncopyable.hpp>

#include <DB/Core/Row.h>


namespace DB
{

/** Интерфейс потока для чтения данных по строкам.
  */
class IRowInputStream : private boost::noncopyable
{
public:

	/** Прочитать следующую строку.
	  * Если строк больше нет - вернуть пустую строку.
	  */
	virtual bool read(Row & row) = 0;

	/// Прочитать разделитель
	virtual void readRowBetweenDelimiter() {};	/// разделитель между строками
	virtual void readPrefix() {};				/// разделитель перед началом результата
	virtual void readSuffix() {};				/// разделитель после конца результата

	virtual ~IRowInputStream() {}
};

typedef SharedPtr<IRowInputStream> RowInputStreamPtr;

}
