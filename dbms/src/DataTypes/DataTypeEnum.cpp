#include <DB/IO/WriteBufferFromString.h>
#include <DB/DataTypes/DataTypeEnum.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int SYNTAX_ERROR;
	extern const int EMPTY_DATA_PASSED;
}


template <typename FieldType> struct EnumName;
template <> struct EnumName<Int8> { static constexpr auto value = "Enum8"; };
template <> struct EnumName<Int16> { static constexpr auto value = "Enum16"; };


template <typename Type>
std::string DataTypeEnum<Type>::generateName(const Values & values)
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

template <typename Type>
void DataTypeEnum<Type>::fillMaps()
{
	for (const auto & name_and_value : values )
	{
		const auto name_to_value_pair = name_to_value_map.insert(
			{ StringRef{name_and_value.first}, name_and_value.second });
		if (!name_to_value_pair.second)
			throw Exception{
				"Duplicate names in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
					+ " and '" + name_to_value_pair.first->first.toString() + "' = " + toString(
						name_to_value_pair.first->second),
				ErrorCodes::SYNTAX_ERROR
			};

		const auto value_to_name_pair = value_to_name_map.insert(
			{ name_and_value.second, StringRef{name_and_value.first} });
		if (!value_to_name_pair.second)
			throw Exception{
				"Duplicate values in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
					+ " and '" + value_to_name_pair.first->second.toString() + "' = " + toString(
						value_to_name_pair.first->first),
				ErrorCodes::SYNTAX_ERROR
			};
	}
}

template <typename Type>
DataTypeEnum<Type>::DataTypeEnum(const Values & values_) : values{values_}
{
	if (values.empty())
		throw Exception{
			"DataTypeEnum enumeration cannot be empty",
			ErrorCodes::EMPTY_DATA_PASSED
		};

	fillMaps();

	std::sort(std::begin(values), std::end(values), [] (auto & left, auto & right) {
		return left.second < right.second;
	});

	name = generateName(values);
}

template <typename Type>
DataTypeEnum<Type>::DataTypeEnum(const DataTypeEnum & other) : values{other.values}, name{other.name}
{
	fillMaps();
}

template <typename Type>
DataTypePtr DataTypeEnum<Type>::clone() const
{
	return new DataTypeEnum(*this);
}

template <typename Type>
void DataTypeEnum<Type>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
	writeBinary(x, ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	FieldType x;
	readBinary(x, istr);
	field = nearestFieldType(x);
}

template <typename Type>
void DataTypeEnum<Type>::serializeText(const Field & field, WriteBuffer & ostr) const
{
	const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
	writeString(getNameForValue(x), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeText(Field & field, ReadBuffer & istr) const
{
	std::string name;
	readString(name, istr);
	field = nearestFieldType(getValue(StringRef(name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
	writeEscapedString(getNameForValue(x), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	std::string name;
	readEscapedString(name, istr);
	field = nearestFieldType(getValue(StringRef(name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
	writeQuotedString(getNameForValue(x), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	std::string name;
	readQuotedString(name, istr);
	field = nearestFieldType(getValue(StringRef(name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextJSON(const Field & field, WriteBuffer & ostr) const
{
	const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
	writeJSONString(getNameForValue(x), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextXML(const Field & field, WriteBuffer & ostr) const
{
	const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
	writeXMLString(getNameForValue(x), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextCSV(const Field & field, WriteBuffer & ostr) const
{
	const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
	writeCSVString<>(getNameForValue(x), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextCSV(Field & field, ReadBuffer & istr, const char delimiter) const
{
	std::string name;
	readCSVString(name, istr, delimiter);
	field = nearestFieldType(getValue(StringRef(name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeBinary(
	const IColumn & column, WriteBuffer & ostr, const size_t offset, size_t limit) const
{
	const auto & x = typeid_cast<const ColumnType &>(column).getData();
	const auto size = x.size();

	if (limit == 0 || offset + limit > size)
		limit = size - offset;

	ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(FieldType) * limit);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeBinary(
	IColumn & column, ReadBuffer & istr, const size_t limit, const double avg_value_size_hint) const
{
	auto & x = typeid_cast<ColumnType &>(column).getData();
	const auto initial_size = x.size();
	x.resize(initial_size + limit);
	const auto size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(FieldType) * limit);
	x.resize(initial_size + size / sizeof(FieldType));
}

template <typename Type>
ColumnPtr DataTypeEnum<Type>::createConstColumn(const size_t size, const Field & field) const
{
	return new ConstColumnType(size, get<typename NearestFieldType<FieldType>::Type>(field));
}

template <typename Type>
Field DataTypeEnum<Type>::getDefault() const
{
	return typename NearestFieldType<FieldType>::Type(values.front().second);
}


/// Явные инстанцирования.
template class DataTypeEnum<Int8>;
template class DataTypeEnum<Int16>;

}
