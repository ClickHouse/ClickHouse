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

	/** Text serialization with escaping but without quoting.
	  */
	inline void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		serializeTextEscapedImpl(column, row_num, ostr, null_map);
	}

	inline void deserializeTextEscaped(IColumn & column, ReadBuffer & istr,
		PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		deserializeTextEscapedImpl(column, istr, null_map);
	}

	/** Text serialization as a literal that may be inserted into a query.
	  */
	inline void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		serializeTextQuotedImpl(column, row_num, ostr, null_map);
	}

	inline void deserializeTextQuoted(IColumn & column, ReadBuffer & istr,
		PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		deserializeTextQuotedImpl(column, istr, null_map);
	}

	/** Text serialization for the CSV format.
	  */
	inline void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		serializeTextCSVImpl(column, row_num, ostr, null_map);
	}

	/** delimiter - the delimiter we expect when reading a value that is not double-quoted.
	  */
	inline void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter,
		PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		deserializeTextCSVImpl(column, istr, delimiter, null_map);
	}

	/** Text serialization for displaying on a terminal or saving into a text file, and the like.
	  * Without escaping or quoting.
	  */
	inline void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		serializeTextImpl(column, row_num, ostr, null_map);
	}

	/** Text serialization as a literal for using with the JSON format.
	  */
	inline void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		serializeTextJSONImpl(column, row_num, ostr, null_map);
	}

	inline void deserializeTextJSON(IColumn & column, ReadBuffer & istr,
		PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		deserializeTextJSONImpl(column, istr, null_map);
	}

	/** Text serialization for putting into the XML format.
	  */
	inline void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map = nullptr) const
	{
		serializeTextXMLImpl(column, row_num, ostr, null_map);
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

protected:
	inline bool isNullValue(const NullValuesByteMap * null_map, size_t row_num) const
	{
		return (null_map != nullptr) && ((*null_map)[row_num] == 1);
	}

	virtual void serializeTextEscapedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const = 0;
	virtual void deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const = 0;
	virtual void serializeTextQuotedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const = 0;
	virtual void deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const = 0;
	virtual void serializeTextCSVImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const = 0;
	virtual void deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const char delimiter, NullValuesByteMap * null_map) const = 0;
	virtual void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const = 0;
	virtual void serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const = 0;
	virtual void deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const = 0;
	virtual void serializeTextXMLImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const
	{
		serializeTextImpl(column, row_num, ostr, null_map);
	}
};


}

