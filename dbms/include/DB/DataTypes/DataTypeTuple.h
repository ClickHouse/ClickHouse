#pragma once

#include <DB/DataTypes/IDataType.h>


namespace DB
{

/** Тип данных - кортеж.
  * Используется как промежуточный результат при вычислении выражений.
  * Также может быть использовать в качестве столбца - результата выполнения запроса.
  * Не может быть сохранён в таблицы.
  */
class DataTypeTuple final : public IDataType
{
private:
	DataTypes elems;
public:
	DataTypeTuple(DataTypes elems_) : elems(elems_) {}

	std::string getName() const override;
	SharedPtr<IDataType> clone() const override { return new DataTypeTuple(elems); }

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
	void deserializeBinary(Field & field, ReadBuffer & istr) const override;
	void serializeText(const Field & field, WriteBuffer & ostr) const override;
	void deserializeText(Field & field, ReadBuffer & istr) const override;
	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const override;
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const override;
	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const override;
	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const override;
	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const override;
	void serializeTextXML(const Field & field, WriteBuffer & ostr) const override;

	/// Кортежи в формате CSV будем сериализовать, как отдельные столбцы (то есть, теряя их вложенность в кортеж).
	void serializeTextCSV(const Field & field, WriteBuffer & ostr) const override;
	void deserializeTextCSV(Field & field, ReadBuffer & istr, const char delimiter) const override;

	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const override;

	/** limit обязательно должен быть в точности равен количеству сериализованных значений.
	  * Именно из-за этого (невозможности читать меньший кусок записанных данных), Tuple не могут быть использованы для хранения данных в таблицах.
	  * (Хотя могут быть использованы для передачи данных по сети в Native формате.)
	  */
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

	ColumnPtr createColumn() const override;
	ColumnPtr createConstColumn(size_t size, const Field & field) const override;

	Field getDefault() const override;
	const DataTypes & getElements() const { return elems; }
};

}

