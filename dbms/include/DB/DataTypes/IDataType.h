#pragma once

#include <memory>

#include <DB/Core/Field.h>
#include <DB/Columns/IColumn.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

class IDataType;

using DataTypePtr = std::shared_ptr<IDataType>;
using DataTypes = std::vector<DataTypePtr>;


/** Метаданные типа для хранения (столбца).
  * Содержит методы для сериализации/десериализации.
  */
class IDataType
{
public:
	/// Основное имя типа (например, UInt64).
	virtual std::string getName() const = 0;

	/// Является ли тип числовым. Дата и дата-с-временем тоже считаются такими.
	virtual bool isNumeric() const { return false; }

	/// Если тип числовой, уместны ли с ним все арифметические операции и приведение типов.
	/// true для чисел, false для даты и даты-с-временем.
	virtual bool behavesAsNumber() const { return false; }

	/// Клонировать
	virtual DataTypePtr clone() const = 0;

	/** Бинарная сериализация диапазона значений столбца - для сохранения на диск / в сеть и т. п.
	  * offset и limit используются, чтобы сериализовать часть столбца.
	  * limit = 0 - означает - не ограничено.
	  * offset не должен быть больше размера столбца.
	  * offset + limit может быть больше размера столбца
	  *  - в этом случае, столбец сериализуется до конца.
	  */
	virtual void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const = 0;

	/** Считать не более limit значений и дописать их в конец столбца.
	  * avg_value_size_hint - если не 0, то может использоваться, чтобы избежать реаллокаций при чтении строкового столбца.
	  */
	virtual void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const = 0;

	/// Сериализация единичных значений.

	/// Для бинарной сериализации есть два варианта. Один вариант работает с Field.
	virtual void serializeBinary(const Field & field, WriteBuffer & ostr) const = 0;
	virtual void deserializeBinary(Field & field, ReadBuffer & istr) const = 0;

	/// Все остальные варианты сериализации работают со столбцом, что позволяет избежать создания временного объекта типа Field.
	/// При этом, столбец не должен быть константным.

	/// Сериализовать одно значение на указанной позиции в столбце.
	virtual void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;
	/// Десериализвать одно значение и вставить его в столбец.
	/// Если функция кидает исключение при чтении, то столбец будет находиться в таком же состоянии, как до вызова функции.
	virtual void deserializeBinary(IColumn & column, ReadBuffer & istr) const = 0;

	/** Текстовая сериализация с эскейпингом, но без квотирования.
	  */
	virtual void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;
	virtual void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const = 0;

	/** Текстовая сериализация в виде литерала, который может быть вставлен в запрос.
	  */
	virtual void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;
	virtual void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const = 0;

	/** Текстовая сериализация для формата CSV.
	  * delimiter - какого разделителя ожидать при чтении, если строковое значение не в кавычках (сам разделитель не съедается).
	  */
	virtual void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;
	virtual void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const = 0;

	/** Текстовая сериализация - для вывода на экран / сохранения в текстовый файл и т. п.
	  * Без эскейпинга и квотирования.
	  */
	virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;

	/** Текстовая сериализация в виде литерала для использования в формате JSON.
	  */
	virtual void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;
	virtual void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const = 0;

	/** Текстовая сериализация для подстановки в формат XML.
	  */
	virtual void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
	{
		serializeText(column, row_num, ostr);
	}

	/** Создать пустой столбец соответствующего типа.
	  */
	virtual ColumnPtr createColumn() const = 0;

	/** Создать столбец соответствующего типа, содержащий константу со значением Field, длины size.
	  */
	virtual ColumnPtr createConstColumn(size_t size, const Field & field) const = 0;

	/** Получить значение "по-умолчанию".
	  */
	virtual Field getDefault() const = 0;

	/// Вернуть приблизительный (оценочный) размер значения.
	virtual size_t getSizeOfField() const
	{
		throw Exception("getSizeOfField() method is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	virtual ~IDataType() {}
};


}

