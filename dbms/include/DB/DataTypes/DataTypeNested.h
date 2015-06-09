#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Core/NamesAndTypes.h>

namespace DB
{

using Poco::SharedPtr;


class DataTypeNested final : public IDataType
{
private:
	/// Имена и типы вложенных массивов.
	NamesAndTypesListPtr nested;
	/// Тип смещений.
	DataTypePtr offsets;

public:
	DataTypeNested(NamesAndTypesListPtr nested_);

	std::string getName() const;

	static std::string concatenateNestedName(const std::string & nested_table_name, const std::string & nested_field_name);
	/// Возвращает префикс имени до первой точки '.'. Или имя без изменений, если точки нет.
	static std::string extractNestedTableName(const std::string & nested_name);
	/// Возвращает суффикс имени после первой точки справа '.'. Или имя без изменений, если точки нет.
	static std::string extractNestedColumnName(const std::string & nested_name);

	DataTypePtr clone() const
	{
		return new DataTypeNested(nested);
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
	  * Это нужно, так как несколько массивов имеют общие размеры.
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
		throw Exception("Method getDefault is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	const NamesAndTypesListPtr & getNestedTypesList() const { return nested; }
	const DataTypePtr & getOffsetsType() const { return offsets; }

	/// Создает новый список в котором колонки типа Nested заменены на несколько вида имя_колонки.имя_вложенной_ячейки
	static NamesAndTypesListPtr expandNestedColumns(const NamesAndTypesList & names_and_types);
};

}
