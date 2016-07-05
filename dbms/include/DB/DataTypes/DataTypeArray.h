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

private:
	void serializeTextInternal(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const;
	void deserializeTextQuotedInternal(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const;
	void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const override;
	void deserializeTextImpl(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const;
	void serializeTextEscapedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const override;
	void deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const override;
	void serializeTextQuotedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const override;
	void deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const override;
	void serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const override;
	void deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const override;
	void serializeTextXMLImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const override;
	void serializeTextCSVImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const override;
	void deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const char delimiter, NullValuesByteMap * null_map) const override;
};

}
