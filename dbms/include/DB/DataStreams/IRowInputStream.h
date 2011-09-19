#pragma once

#include <DB/Core/Row.h>


namespace DB
{

/** Интерфейс потока для чтения данных по строкам.
  */
class IRowInputStream
{
public:

	/** Прочитать следующую строку.
	  * Если строк больше нет - вернуть пустую строку.
	  */
	virtual Row read() = 0;

	virtual ~IRowInputStream() {}
};

}
