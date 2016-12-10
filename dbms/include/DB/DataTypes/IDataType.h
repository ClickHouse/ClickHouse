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

	/// Is this type the null type?
	virtual bool isNull() const { return false; }

	/// Is this type nullable?
	virtual bool isNullable() const { return false; }

	/// Is this type numeric? Date and DateTime types are considered as such.
	virtual bool isNumeric() const { return false; }

	/// Is this type numeric and not nullable?
	virtual bool isNumericNotNullable() const { return isNumeric(); }

	/// If this type is numeric, are all the arithmetic operations and type casting
	/// relevant for it? True for numbers. False for Date and DateTime types.
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

	/** Text serialization with escaping but without quoting.
	  */
	virtual void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;

	virtual void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const = 0;

	/** Text serialization as a literal that may be inserted into a query.
	  */
	virtual void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;

	virtual void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const = 0;

	/** Text serialization for the CSV format.
	  */
	virtual void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;

	/** delimiter - the delimiter we expect when reading a string value that is not double-quoted
	  * (the delimiter is not consumed).
	  */
	virtual void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const = 0;

	/** Text serialization for displaying on a terminal or saving into a text file, and the like.
	  * Without escaping or quoting.
	  */
	virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;

	/** Text serialization intended for using in JSON format.
	  * force_quoting_64bit_integers parameter forces to brace UInt64 and Int64 types into quotes.
	  */
	virtual void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, bool force_quoting_64bit_integers) const = 0;
	virtual void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const = 0;

	/** Text serialization for putting into the XML format.
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

