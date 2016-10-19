#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Functions/EnrichedDataTypePtr.h>


namespace DB
{


class DataTypeArray final : public IDataType
{
private:
	/// Расширенный тип элементов массивов.
	DataTypeTraits::EnrichedDataTypePtr enriched_nested;
	/// Тип элементов массивов.
	DataTypePtr nested;
	/// Тип смещений.
	DataTypePtr offsets;

public:
	DataTypeArray(DataTypePtr nested_);
	DataTypeArray(DataTypeTraits::EnrichedDataTypePtr enriched_nested_);

	std::string getName() const override
	{
		return "Array(" + nested->getName() + ")";
	}

	DataTypePtr clone() const override
	{
		return std::make_shared<DataTypeArray>(enriched_nested);
	}

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
	void deserializeBinary(Field & field, ReadBuffer & istr) const override;
	void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

	void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeText(IColumn & column, ReadBuffer & istr) const;

	void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;

	void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;

	void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, bool) const override;
	void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

	void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

	void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

	/** Потоковая сериализация массивов устроена по-особенному:
	  * - записываются/читаются элементы, уложенные подряд, без размеров массивов;
	  * - размеры записываются/читаются в отдельный столбец,
	  *   и о записи/чтении размеров должна позаботиться вызывающая сторона.
	  * Это нужно, так как при реализации вложенных структур, несколько массивов могут иметь общие размеры.
	  */

	/** Записать только значения, без размеров. Вызывающая сторона также должна куда-нибудь записать смещения. */
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const override;

	/** Прочитать только значения, без размеров.
	  * При этом, в column уже заранее должны быть считаны все размеры.
	  */
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

	/** Записать размеры. */
	void serializeOffsets(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const;

	/** Прочитать размеры. Вызывайте этот метод перед чтением значений. */
	void deserializeOffsets(IColumn & column, ReadBuffer & istr, size_t limit) const;

	ColumnPtr createColumn() const override;
	ColumnPtr createConstColumn(size_t size, const Field & field) const override;

	Field getDefault() const override
	{
		return Array();
	}

	const DataTypePtr & getNestedType() const { return nested; }
	const DataTypeTraits::EnrichedDataTypePtr & getEnrichedNestedType() const { return enriched_nested; }
	const DataTypePtr & getOffsetsType() const { return offsets; }

	/// Returns the data type found at the most nested level.
	const DataTypePtr & getMostNestedType() const;
};

}
