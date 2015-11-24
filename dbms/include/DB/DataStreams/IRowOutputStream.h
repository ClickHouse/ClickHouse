#pragma once

#include <boost/noncopyable.hpp>

#include <DB/Core/Row.h>
#include <DB/Core/Block.h>


namespace DB
{

/** Интерфейс потока для записи данных по строкам (например, для вывода в консоль).
  */
class IRowOutputStream : private boost::noncopyable
{
public:

	/** Записать строку.
	  * Есть реализация по умолчанию, которая использует методы для записи одиночных значений и разделителей
	  * (кроме разделителя между строк (writeRowBetweenDelimiter())).
	  */
	virtual void write(const Row & row);

	/** Записать значение. */
	virtual void writeField(const Field & field) = 0;

	/** Записать разделитель. */
	virtual void writeFieldDelimiter() {};		/// разделитель между значениями
	virtual void writeRowStartDelimiter() {};	/// разделитель перед каждой строкой
	virtual void writeRowEndDelimiter() {};		/// разделитель после каждой строки
	virtual void writeRowBetweenDelimiter() {};	/// разделитель между строками
	virtual void writePrefix() {};				/// разделитель перед началом результата
	virtual void writeSuffix() {};				/// разделитель после конца результата

	/** Сбросить имеющиеся буферы для записи. */
	virtual void flush() {}

	/** Методы для установки дополнительной информации для вывода в поддерживающих её форматах.
	  */
	virtual void setRowsBeforeLimit(size_t rows_before_limit) {}
	virtual void setTotals(const Block & totals) {}
	virtual void setExtremes(const Block & extremes) {}

	/** Выставлять такой Content-Type при отдаче по HTTP. */
	virtual String getContentType() const { return "text/plain; charset=UTF-8"; }

	virtual ~IRowOutputStream() {}
};

typedef SharedPtr<IRowOutputStream> RowOutputStreamPtr;

}
