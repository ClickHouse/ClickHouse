#ifndef DBMS_DATA_STREAMS_IROWOUTPUTSTREAM_H
#define DBMS_DATA_STREAMS_IROWOUTPUTSTREAM_H

#include <DB/Core/Row.h>


namespace DB
{

/** Интерфейс потока для записи данных по строкам (например, для вывода в консоль).
  */
class IRowOutputStream
{
public:

	/** Записать строку.
	  * Есть реализация по умолчанию, которая использует методы для записи одиночных значений и разделителей.
	  */
	virtual void write(const Row & row);

	/** Записать значение. */
	virtual void writeField(const Field & field) = 0;

	/** Записать разделитель. */
	virtual void writeFieldDelimiter() {};
	virtual void writeRowStartDelimiter() {};
	virtual void writeRowEndDelimiter() {};

	virtual ~IRowOutputStream() {}
};

}

#endif
