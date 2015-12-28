#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Common/HashTable/HashMap.h>
#include <vector>


namespace DB
{


template <typename FieldType> struct EnumName;
template <> struct EnumName<Int8> { static constexpr auto value = "Enum8"; };
template <> struct EnumName<Int16> { static constexpr auto value = "Enum16"; };

template <typename Type>
class DataTypeEnum final : public IDataType
{
public:
	using FieldType = Type;
	using ColumnType = ColumnVector<FieldType>;
	using ConstColumnType = ColumnConst<FieldType>;
	using Value = std::pair<std::string, FieldType>;
	using Values = std::vector<Value>;
	using Map = HashMap<StringRef, FieldType, StringRefHash>;

	Values values;
	std::string name;
	Map map;

	const Values & getValues() const { return values; }

	static std::string generateName(const Values & values)
	{
		std::string name;

		{
			WriteBufferFromString out{name};

			writeString(EnumName<FieldType>::value, out);
			writeChar('(', out);

			auto first = true;
			for (const auto & name_and_value : values)
			{
				if (!first)
					writeString(", ", out);

				first = false;

				writeQuotedString(name_and_value.first, out);
				writeString(" = ", out);
				writeText(name_and_value.second, out);
			}

			writeChar(')', out);
		}

		return name;
	}

	void fillMap()
	{
		for (const auto & name_and_value : values )
		{
			const auto pair = map.insert({ StringRef{name_and_value.first}, name_and_value.second });
			if (!pair.second)
				throw Exception{
					"Duplicate names in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
						+ " and '" + pair.first->first.toString() + "' = " + toString(pair.first->second),
					ErrorCodes::SYNTAX_ERROR
				};
		}
	}

	static void sortAndUnique(Values & values)
	{
		std::sort(std::begin(values), std::end(values), [] (auto & left, auto & right) {
			return left.second < right.second;
		});

		const auto unique_it = std::unique(std::begin(values), std::end(values), [] (auto & left, auto & right) {
			return left.second == right.second;
		});

		if (unique_it != std::end(values))
			throw Exception{
				"Duplicate values in enum: '" + unique_it->first + "' = " + toString(unique_it->second),
				ErrorCodes::SYNTAX_ERROR
			};
	}

public:
	DataTypeEnum(const Values & values_) : values{values_}
	{
		if (values.empty())
			throw Exception{
				"DataTypeEnum enumeration cannot be empty",
				ErrorCodes::EMPTY_DATA_PASSED
			};

		sortAndUnique(values);

		name = generateName(values);

		fillMap();
	}

	DataTypeEnum(const DataTypeEnum & other) : values{other.values}, name{other.name}
	{
		fillMap();
	}

	std::string getName() const override { return name; }

	bool isNumeric() const override { return true; }

	bool behavesAsNumber() const override { return true; }

	/// Returns length of textual name for an enum element (used in FunctionVisibleWidth)
	std::size_t getNameLength(const FieldType & value) const
	{
		return getNameForValue(value).size();
	}

	const std::string & getNameForValue(const FieldType & value) const
	{
		const auto it = std::lower_bound(std::begin(values), std::end(values), value, [] (const auto & left, const auto & right) {
			return left.second < right;
		});

		if (it == std::end(values) || it->second != value)
			throw Exception{
				"Unexpected value " + toString(value) + " for " + getName(),
				ErrorCodes::LOGICAL_ERROR
			};

		return it->first;
	}

	FieldType getValue(const std::string & name) const
	{
		const auto it = map.find(StringRef{name});
		if (it == std::end(map))
			throw Exception{
				"Unknown string '" + name + "' for " + getName(),
				ErrorCodes::LOGICAL_ERROR
			};

		return it->second;
	}

	DataTypePtr clone() const override
	{
		return new DataTypeEnum(*this);
	}

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override
	{
		const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
		writeBinary(x, ostr);
	}
	void deserializeBinary(Field & field, ReadBuffer & istr) const override
	{
		FieldType x;
		readBinary(x, istr);
		field = nearestFieldType(x);
	}

	void serializeText(const Field & field, WriteBuffer & ostr) const override
	{
		const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
		writeString(getNameForValue(x), ostr);
	}
	void deserializeText(Field & field, ReadBuffer & istr) const override
	{
		std::string name;
		readString(name, istr);
		field = nearestFieldType(getValue(name));
	}

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const override
	{
		const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
		writeEscapedString(getNameForValue(x), ostr);
	}
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const override
	{
		std::string name;
		readEscapedString(name, istr);
		field = nearestFieldType(getValue(name));
	}

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const override
	{
		const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
		writeQuotedString(getNameForValue(x), ostr);
	}
	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const override
	{
		std::string name;
		readQuotedString(name, istr);
		field = nearestFieldType(getValue(name));
	}

	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const override
	{
		const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
		writeJSONString(getNameForValue(x), ostr);
	}

	/** Потоковая сериализация массивов устроена по-особенному:
	  * - записываются/читаются элементы, уложенные подряд, без размеров массивов;
	  * - размеры записываются/читаются в отдельный столбец,
	  *   и о записи/чтении размеров должна позаботиться вызывающая сторона.
	  * Это нужно, так как при реализации вложенных структур, несколько массивов могут иметь общие размеры.
	  */

	/** Записать только значения, без размеров. Вызывающая сторона также должна куда-нибудь записать смещения. */
	void serializeBinary(
		const IColumn & column, WriteBuffer & ostr, const size_t offset = 0, size_t limit = 0) const override
	{
		const auto & x = typeid_cast<const ColumnType &>(column).getData();

		const auto size = x.size();

		if (limit == 0 || offset + limit > size)
			limit = size - offset;

		ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(FieldType) * limit);
	}

	/** Прочитать только значения, без размеров.
	  * При этом, в column уже заранее должны быть считаны все размеры.
	  */
	void deserializeBinary(
		IColumn & column, ReadBuffer & istr, const size_t limit, const double avg_value_size_hint) const override
	{
		auto & x = typeid_cast<ColumnType &>(column).getData();
		const auto initial_size = x.size();
		x.resize(initial_size + limit);
		const auto size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(FieldType) * limit);
		x.resize(initial_size + size / sizeof(FieldType));
	}

	size_t getSizeOfField() const override { return sizeof(FieldType); }

	ColumnPtr createColumn() const override { return new ColumnType; }
	ColumnPtr createConstColumn(const size_t size, const Field & field) const override
	{
		return new ConstColumnType(size, get<typename NearestFieldType<FieldType>::Type>(field));
	}

	Field getDefault() const override
	{
		return typename NearestFieldType<FieldType>::Type(values.front().second);
	}
};


using DataTypeEnum8 = DataTypeEnum<Int8>;
using DataTypeEnum16 = DataTypeEnum<Int16>;


}
