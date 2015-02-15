#pragma once

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;


class DataTypeArray : public IDataType
{
private:
	/// Тип элементов массивов.
	DataTypePtr nested;
	/// Тип смещений.
	DataTypePtr offsets;

public:
	DataTypeArray(DataTypePtr nested_);

	std::string getName() const
	{
		return "Array(" + nested->getName() + ")";
	}

	DataTypePtr clone() const
	{
		return new DataTypeArray(nested);
	}

	void serializeBinary(const Field & field, WriteBuffer & ostr) const;
	void deserializeBinary(Field & field, ReadBuffer & istr) const;

	void serializeText(const Field & field, WriteBuffer & ostr) const;
	void deserializeText(Field & field, ReadBuffer & istr) const;

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const;
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const;

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const;
	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const;

	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const;

	/** Потоковая сериализация массивов устроена по-особенному:
	  * - записываются/читаются элементы, уложенные подряд, без размеров массивов;
	  * - размеры записываются/читаются в отдельный столбец,
	  *   и о записи/чтении размеров должна позаботиться вызывающая сторона.
	  * Это нужно, так как при реализации вложенных структур, несколько массивов могут иметь общие размеры.
	  */

	/** Записать только значения, без размеров. Вызывающая сторона также должна куда-нибудь записать смещения. */
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const;

	/** Прочитать только значения, без размеров.
	  * При этом, в column уже заранее должны быть считаны все размеры.
	  */
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const;

	/** Записать размеры. */
	void serializeOffsets(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const;

	/** Прочитать размеры. Вызывайте этот метод перед чтением значений. */
	void deserializeOffsets(IColumn & column, ReadBuffer & istr, size_t limit) const;

	ColumnPtr createColumn() const;
	ColumnPtr createConstColumn(size_t size, const Field & field) const;

	Field getDefault() const
	{
		return Array();
	}

	const DataTypePtr & getNestedType() const { return nested; }
	const DataTypePtr & getOffsetsType() const { return offsets; }
};

}
