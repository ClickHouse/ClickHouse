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

	/// Прочитать разделитель
	virtual void readRowBetweenDelimiter() {};	/// разделитель между строками
	virtual void readPrefix() {};				/// разделитель перед началом результата
	virtual void readSuffix() {};				/// разделитель после конца результата

	/** Создать копию объекта.
	  * Предполагается, что функция вызывается только до использования объекта (сразу после создания, до вызова других методов),
	  *  только для того, чтобы можно было преобразовать параметр, переданный по ссылке в shared ptr.
	  */
	virtual SharedPtr<IRowInputStream> clone() = 0;

	virtual ~IRowInputStream() {}
};

typedef SharedPtr<IRowInputStream> RowInputStreamPtr;

}
