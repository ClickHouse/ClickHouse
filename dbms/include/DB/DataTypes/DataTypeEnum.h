#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Common/HashTable/HashMap.h>
#include <vector>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


template <typename Type>
class DataTypeEnum final : public IDataType
{
public:
	using FieldType = Type;
	using ColumnType = ColumnVector<FieldType>;
	using ConstColumnType = ColumnConst<FieldType>;
	using Value = std::pair<std::string, FieldType>;
	using Values = std::vector<Value>;
	using NameToValueMap = HashMap<StringRef, FieldType, StringRefHash>;
	using ValueToNameMap = std::unordered_map<FieldType, StringRef>;

private:
	Values values;
	NameToValueMap name_to_value_map;
	ValueToNameMap value_to_name_map;
	std::string name;

	static std::string generateName(const Values & values);
	void fillMaps();

public:
	DataTypeEnum(const Values & values_);
	DataTypeEnum(const DataTypeEnum & other);

	const Values & getValues() const { return values; }
	std::string getName() const override { return name; }
	bool isNumeric() const override { return true; }
	bool behavesAsNumber() const override { return true; }

	const StringRef & getNameForValue(const FieldType & value) const
	{
		const auto it = value_to_name_map.find(value);
		if (it == std::end(value_to_name_map))
			throw Exception{
				"Unexpected value " + toString(value) + " for type " + getName(),
				ErrorCodes::LOGICAL_ERROR
			};

		return it->second;
	}

	FieldType getValue(StringRef name) const
	{
		const auto it = name_to_value_map.find(name);
		if (it == std::end(name_to_value_map))
			throw Exception{
				"Unknown element '" + name.toString() + "' for type " + getName(),
				ErrorCodes::LOGICAL_ERROR};

		return it->second;
	}

	DataTypePtr clone() const override;

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
	void deserializeBinary(Field & field, ReadBuffer & istr) const override;
	void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
	void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
	void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;
	void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;
	void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

	void serializeBinary(const IColumn & column, WriteBuffer & ostr, const size_t offset = 0, size_t limit = 0) const override;
	void deserializeBinary(IColumn & column, ReadBuffer & istr, const size_t limit, const double avg_value_size_hint) const override;

	size_t getSizeOfField() const override { return sizeof(FieldType); }

	ColumnPtr createColumn() const override { return new ColumnType; }
	ColumnPtr createConstColumn(const size_t size, const Field & field) const override;

	Field getDefault() const override;
};


using DataTypeEnum8 = DataTypeEnum<Int8>;
using DataTypeEnum16 = DataTypeEnum<Int16>;


}
