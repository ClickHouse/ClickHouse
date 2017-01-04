#include <DB/Core/FieldVisitors.h>
#include <DB/DataTypes/FieldToDataType.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <DB/DataTypes/DataTypeNull.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <ext/size.hpp>


namespace DB
{

namespace ErrorCodes
{
	extern const int EMPTY_DATA_PASSED;
	extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename T>
static void convertArrayToCommonType(Array & arr)
{
	for (auto & elem : arr)
	{
		if (!elem.isNull())
			elem = apply_visitor(FieldVisitorConvertToNumber<T>(), elem);
	}
}


DataTypePtr FieldToDataType::operator() (Array & x) const
{
	if (x.empty())
		throw Exception("Cannot infer type of empty array", ErrorCodes::EMPTY_DATA_PASSED);

	/** Тип массива нужно вывести по типу его элементов.
	  * Если элементы - числа, то нужно выбрать наименьший общий тип, если такой есть,
	  *  или кинуть исключение.
	  * Код похож на NumberTraits::ResultOfIf, но тем кодом трудно здесь непосредственно воспользоваться.
	  *
	  * Также заметим, что Float32 не выводится, вместо этого используется только Float64.
	  * Это сделано потому что литералов типа Float32 не бывает в запросе.
	  */

	bool has_string = false;
	bool has_array = false;
	bool has_float = false;
	bool has_tuple = false;
	bool has_null = false;
	int max_bits = 0;
	int max_signed_bits = 0;
	int max_unsigned_bits = 0;

	/// Wrap the specified type into an array type. If at least one element of
	/// the array is nullable, first turn the input argument into a nullable type.
	auto wrap_into_array = [&has_null](const DataTypePtr & type)
	{
		return std::make_shared<DataTypeArray>(
			has_null ? std::make_shared<DataTypeNullable>(type) : type);
	};

	for (const Field & elem : x)
	{
		switch (elem.getType())
		{
			case Field::Types::UInt64:
			{
				UInt64 num = elem.get<UInt64>();
				if (num <= std::numeric_limits<UInt8>::max())
					max_unsigned_bits = std::max(8, max_unsigned_bits);
				else if (num <= std::numeric_limits<UInt16>::max())
					max_unsigned_bits = std::max(16, max_unsigned_bits);
				else if (num <= std::numeric_limits<UInt32>::max())
					max_unsigned_bits = std::max(32, max_unsigned_bits);
				else
					max_unsigned_bits = 64;
				max_bits = std::max(max_unsigned_bits, max_bits);
				break;
			}
			case Field::Types::Int64:
			{
				Int64 num = elem.get<Int64>();
				if (num <= std::numeric_limits<Int8>::max() && num >= std::numeric_limits<Int8>::min())
					max_signed_bits = std::max(8, max_signed_bits);
				else if (num <= std::numeric_limits<Int16>::max() && num >= std::numeric_limits<Int16>::min())
					max_signed_bits = std::max(16, max_signed_bits);
				else if (num <= std::numeric_limits<Int32>::max() && num >= std::numeric_limits<Int32>::min())
					max_signed_bits = std::max(32, max_signed_bits);
				else
					max_signed_bits = 64;
				max_bits = std::max(max_signed_bits, max_bits);
				break;
			}
			case Field::Types::Float64:
			{
				has_float = true;
				break;
			}
			case Field::Types::String:
			{
				has_string = true;
				break;
			}
			case Field::Types::Array:
			{
				has_array = true;
				break;
			}
			case Field::Types::Tuple:
			{
				has_tuple = true;
				break;
			}
			case Field::Types::Null:
			{
				has_null = true;
				break;
			}
		}
	}

	if ((has_string + has_array + (max_bits > 0)) > 1)
		throw Exception("Incompatible types of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	if (has_array)
		throw Exception("Type inference of multidimensional arrays is not supported", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	if (has_tuple)
		throw Exception("Type inference of array of tuples is not supported", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	if (has_string)
		return wrap_into_array(std::make_shared<DataTypeString>());

	if (has_float && max_bits == 64)
		throw Exception("Incompatible types Float64 and UInt64/Int64 of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	if (has_float)
	{
		convertArrayToCommonType<Float64>(x);
		return wrap_into_array(std::make_shared<DataTypeFloat64>());
	}

	if (max_signed_bits == 64 && max_unsigned_bits == 64)
		throw Exception("Incompatible types UInt64 and Int64 of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	if (max_signed_bits && !max_unsigned_bits)
	{
		if (max_signed_bits == 8)
			return wrap_into_array(std::make_shared<DataTypeInt8>());
		if (max_signed_bits == 16)
			return wrap_into_array(std::make_shared<DataTypeInt16>());
		if (max_signed_bits == 32)
			return wrap_into_array(std::make_shared<DataTypeInt32>());
		if (max_signed_bits == 64)
			return wrap_into_array(std::make_shared<DataTypeInt64>());
	}

	if (!max_signed_bits && max_unsigned_bits)
	{
		if (max_unsigned_bits == 8)
			return wrap_into_array(std::make_shared<DataTypeUInt8>());
		if (max_unsigned_bits == 16)
			return wrap_into_array(std::make_shared<DataTypeUInt16>());
		if (max_unsigned_bits == 32)
			return wrap_into_array(std::make_shared<DataTypeUInt32>());
		if (max_unsigned_bits == 64)
			return wrap_into_array(std::make_shared<DataTypeUInt64>());
	}

	if (max_signed_bits && max_unsigned_bits)
	{
		convertArrayToCommonType<Int64>(x);

		if (max_unsigned_bits >= max_signed_bits)
		{
			/// Беззнаковый тип не помещается в знаковый. Надо увеличить количество бит.
			if (max_bits == 8)
				return wrap_into_array(std::make_shared<DataTypeInt16>());
			if (max_bits == 16)
				return wrap_into_array(std::make_shared<DataTypeInt32>());
			if (max_bits == 32)
				return wrap_into_array(std::make_shared<DataTypeInt64>());
			else
				throw Exception("Incompatible types UInt64 and signed integer of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		else
		{
			/// Беззнаковый тип помещается в знаковый.
			if (max_bits == 8)
				return wrap_into_array(std::make_shared<DataTypeInt8>());
			if (max_bits == 16)
				return wrap_into_array(std::make_shared<DataTypeInt16>());
			if (max_bits == 32)
				return wrap_into_array(std::make_shared<DataTypeInt32>());
			if (max_bits == 64)
				return wrap_into_array(std::make_shared<DataTypeInt64>());
		}
	}

	if (has_null)
	{
		/// Special case: an array of NULLs is represented as an array
		/// of Nullable(UInt8) because ColumnNull is actually ColumnConst<Null>.
		return wrap_into_array(std::make_shared<DataTypeUInt8>());
	}

	throw Exception("Incompatible types of elements of array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


DataTypePtr FieldToDataType::operator() (Tuple & x) const
{
	auto & tuple = static_cast<TupleBackend &>(x);
	if (tuple.empty())
		throw Exception("Cannot infer type of an empty tuple", ErrorCodes::EMPTY_DATA_PASSED);

	DataTypes element_types;
	element_types.reserve(ext::size(tuple));

	for (auto & element : tuple)
		element_types.push_back(apply_visitor(FieldToDataType{}, element));

	return std::make_shared<DataTypeTuple>(element_types);
}


}
