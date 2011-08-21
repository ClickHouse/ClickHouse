#pragma once

#include <Poco/NumberFormatter.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>

#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции сравнения: ==, !=, <, >, <=, >=.
  * Функции сравнения возвращают всегда 0 или 1 (UInt8).
  *
  * Сравнивать можно следующие типы:
  * - числа;
  * - строки и фиксированные строки;
  * - даты;
  * - даты-с-временем;
  *   внутри каждой группы, но не из разных групп.
  */

/** Игнорируем warning о сравнении signed и unsigned.
  * (Результат может быть некорректным.)
  */ 
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

template<typename A, typename B>
struct EqualsNumImpl
{
	static void vector_vector(const std::vector<A> & a, const std::vector<B> & b, std::vector<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] == b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] == b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<UInt8> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a == b[i];
	}

	static void constant_constant(A a, B b, UInt8 & c)
	{
		c = a == b;
	}
};

struct EqualsStringImpl
{
	static void string_vector_string_vector(
		const std::vector<UInt8> & a_data, const std::vector<size_t> & a_offsets,
		const std::vector<UInt8> & b_data, const std::vector<size_t> & b_offsets,
		std::vector<UInt8> & c)
	{
		size_t size = a_offsets.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = (i == 0)
				? (a_offsets[0] == b_offsets[0] && !memcmp(&a_data[0], &b_data[0], a_offsets[0]))
				: (a_offsets[i] - a_offsets[i - 1] == b_offsets[i] - b_offsets[i - 1]
					&& !memcmp(&a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], a_offsets[i] - a_offsets[i - 1]));
	}

	static void string_vector_fixed_string_vector(
		const std::vector<UInt8> & a_data, const std::vector<size_t> & a_offsets,
		const std::vector<UInt8> & b_data, size_t b_n,
		std::vector<UInt8> & c)
	{
		size_t size = a_offsets.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = (i == 0)
				? (a_offsets[0] == b_n + 1 && !memcmp(&a_data[0], &b_data[0], b_n))
				: (a_offsets[i] - a_offsets[i - 1] == b_n + 1
					&& !memcmp(&a_data[a_offsets[i - 1]], &b_data[b_n * i], b_n));
	}

	static void string_vector_constant(
		const std::vector<UInt8> & a_data, const std::vector<size_t> & a_offsets,
		const std::string & b,
		std::vector<UInt8> & c)
	{
		size_t size = a_offsets.size();
		size_t b_n = b.size();
		const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
		for (size_t i = 0; i < size; ++i)
			c[i] = (i == 0)
				? (a_offsets[0] == b_n + 1 && !memcmp(&a_data[0], b_data, b_n))
				: (a_offsets[i] - a_offsets[i - 1] == b_n + 1
					&& !memcmp(&a_data[a_offsets[i - 1]], b_data, b_n));
	}

	static void fixed_string_vector_string_vector(
		const std::vector<UInt8> & a_data, size_t a_n,
		const std::vector<UInt8> & b_data, const std::vector<size_t> & b_offsets,
		std::vector<UInt8> & c)
	{
		size_t size = b_offsets.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = (i == 0)
				? (b_offsets[0] == a_n + 1 && !memcmp(&b_data[0], &a_data[0], a_n))
				: (b_offsets[i] - b_offsets[i - 1] == a_n + 1
					&& !memcmp(&b_data[b_offsets[i - 1]], &a_data[a_n * i], a_n));
	}

	static void fixed_string_vector_fixed_string_vector(
		const std::vector<UInt8> & a_data, size_t a_n,
		const std::vector<UInt8> & b_data, size_t b_n,
		std::vector<UInt8> & c)
	{
		size_t size = a_data.size();
		for (size_t i = 0; i < size; i += a_n)
			c[i] = a_n == b_n && !memcmp(&a_data[i], &b_data[i], a_n);
	}

	static void fixed_string_vector_constant(
		const std::vector<UInt8> & a_data, size_t a_n,
		const std::string & b,
		std::vector<UInt8> & c)
	{
		size_t size = a_data.size();
		const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
		size_t b_n = b.size();
		for (size_t i = 0; i < size; i += a_n)
			c[i] = a_n == b_n && !memcmp(&a_data[i], b_data, a_n);
	}

	static void constant_string_vector(
		const std::string & a,
		const std::vector<UInt8> & b_data, const std::vector<size_t> & b_offsets,
		std::vector<UInt8> & c)
	{
		size_t size = b_offsets.size();
		size_t a_n = a.size();
		const UInt8 * a_data = reinterpret_cast<const UInt8 *>(a.data());
		for (size_t i = 0; i < size; ++i)
			c[i] = (i == 0)
				? (b_offsets[0] == a_n + 1 && !memcmp(&b_data[0], a_data, a_n))
				: (b_offsets[i] - b_offsets[i - 1] == a_n + 1
					&& !memcmp(&b_data[b_offsets[i - 1]], a_data, a_n));
	}

	static void constant_fixed_string_vector(
		const std::string & a,
		const std::vector<UInt8> & b_data, size_t b_n,
		std::vector<UInt8> & c)
	{
		size_t size = b_data.size();
		const UInt8 * a_data = reinterpret_cast<const UInt8 *>(a.data());
		size_t a_n = a.size();
		for (size_t i = 0; i < size; i += b_n)
			c[i] = a_n == b_n && !memcmp(&b_data[i], a_data, b_n);
	}

	static void constant_constant(
		const std::string & a,
		const std::string & b,
		UInt8 & c)
	{
		c = a == b;
	}
};

#pragma GCC diagnostic pop


template <
	template <typename, typename> class NumImpl,
	typename StringImpl,
	typename Name>
class FunctionComparison : public IFunction
{
private:

	template <typename T0, typename T1>
	bool executeNumRightType(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & result, const ColumnVector<T0> * col_left)
	{
		if (ColumnVector<T1> * col_right = dynamic_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			ColumnUInt8 * col_res = new ColumnUInt8;
			block.getByPosition(result[0]).column = col_res;

			ColumnUInt8::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			NumImpl<T0, T1>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T1> * col_right = dynamic_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			ColumnUInt8 * col_res = new ColumnUInt8;
			block.getByPosition(result[0]).column = col_res;

			ColumnUInt8::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			NumImpl<T0, T1>::vector_constant(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
			
		return false;
	}

	template <typename T0, typename T1>
	bool executeNumConstRightType(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & result, const ColumnConst<T0> * col_left)
	{
		if (ColumnVector<T1> * col_right = dynamic_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			ColumnUInt8 * col_res = new ColumnUInt8;
			block.getByPosition(result[0]).column = col_res;

			ColumnUInt8::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->size());
			NumImpl<T0, T1>::constant_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T1> * col_right = dynamic_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			UInt8 res = 0;
			NumImpl<T0, T1>::constant_constant(col_left->getData(), col_right->getData(), res);
			
			ColumnConstUInt8 * col_res = new ColumnConstUInt8(col_left->size(), res);
			block.getByPosition(result[0]).column = col_res;

			return true;
		}

		return false;
	}

	template <typename T0>
	bool executeNumLeftType(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & result)
	{
		if (ColumnVector<T0> * col_left = dynamic_cast<ColumnVector<T0> *>(&*block.getByPosition(arguments[0]).column))
		{
			if (	executeNumRightType<T0, UInt8>(block, arguments, result, col_left)
				||	executeNumRightType<T0, UInt16>(block, arguments, result, col_left)
				||	executeNumRightType<T0, UInt32>(block, arguments, result, col_left)
				||	executeNumRightType<T0, UInt64>(block, arguments, result, col_left)
				||	executeNumRightType<T0, Int8>(block, arguments, result, col_left)
				||	executeNumRightType<T0, Int16>(block, arguments, result, col_left)
				||	executeNumRightType<T0, Int32>(block, arguments, result, col_left)
				||	executeNumRightType<T0, Int64>(block, arguments, result, col_left)
				||	executeNumRightType<T0, Float32>(block, arguments, result, col_left)
				||	executeNumRightType<T0, Float64>(block, arguments, result, col_left))
				return true;
			else
				throw Exception("Illegal column of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (ColumnConst<T0> * col_left = dynamic_cast<ColumnConst<T0> *>(&*block.getByPosition(arguments[0]).column))
		{
			if (	executeNumConstRightType<T0, UInt8>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, UInt16>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, UInt32>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, UInt64>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, Int8>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, Int16>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, Int32>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, Int64>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, Float32>(block, arguments, result, col_left)
				||	executeNumConstRightType<T0, Float64>(block, arguments, result, col_left))
				return true;
			else
				throw Exception("Illegal column of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		
		return false;
	}

	void executeString(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & result)
	{
		IColumn * c0 = &*block.getByPosition(arguments[0]).column;
		IColumn * c1 = &*block.getByPosition(arguments[1]).column;

		ColumnString * c0_string = dynamic_cast<ColumnString *>(c0);
		ColumnString * c1_string = dynamic_cast<ColumnString *>(c1);
		ColumnFixedString * c0_fixed_string = dynamic_cast<ColumnFixedString *>(c0);
		ColumnFixedString * c1_fixed_string = dynamic_cast<ColumnFixedString *>(c1);
		ColumnConstString * c0_const = dynamic_cast<ColumnConstString *>(c0);
		ColumnConstString * c1_const = dynamic_cast<ColumnConstString *>(c1);

		if (c0_const && c1_const)
		{
			ColumnConstUInt8 * c_res = new ColumnConstUInt8(c0_const->size(), 0);
			block.getByPosition(result[0]).column = c_res;
			StringImpl::constant_constant(c0_const->getData(), c1_const->getData(), c_res->getData());
		}
		else
		{
			ColumnUInt8 * c_res = new ColumnUInt8;
			block.getByPosition(result[0]).column = c_res;
			ColumnUInt8::Container_t & vec_res = c_res->getData();
			vec_res.resize(c0->size());

			if (c0_string && c1_string)
				StringImpl::string_vector_string_vector(
					dynamic_cast<ColumnUInt8 &>(c0_string->getData()).getData(), c0_string->getOffsets(),
					dynamic_cast<ColumnUInt8 &>(c1_string->getData()).getData(), c1_string->getOffsets(),
					c_res->getData());
			else if (c0_string && c1_fixed_string)
				StringImpl::string_vector_fixed_string_vector(
					dynamic_cast<ColumnUInt8 &>(c0_string->getData()).getData(), c0_string->getOffsets(),
					dynamic_cast<ColumnUInt8 &>(c1_fixed_string->getData()).getData(), c1_fixed_string->getN(),
					c_res->getData());
			else if (c0_string && c1_const)
				StringImpl::string_vector_constant(
					dynamic_cast<ColumnUInt8 &>(c0_string->getData()).getData(), c0_string->getOffsets(),
					c1_const->getData(),
					c_res->getData());
			else if (c0_fixed_string && c1_string)
				StringImpl::fixed_string_vector_string_vector(
					dynamic_cast<ColumnUInt8 &>(c0_fixed_string->getData()).getData(), c0_fixed_string->getN(),
					dynamic_cast<ColumnUInt8 &>(c1_string->getData()).getData(), c1_string->getOffsets(),
					c_res->getData());
			else if (c0_fixed_string && c1_fixed_string)
				StringImpl::fixed_string_vector_fixed_string_vector(
					dynamic_cast<ColumnUInt8 &>(c0_fixed_string->getData()).getData(), c0_fixed_string->getN(),
					dynamic_cast<ColumnUInt8 &>(c1_fixed_string->getData()).getData(), c1_fixed_string->getN(),
					c_res->getData());
			else if (c0_fixed_string && c1_const)
				StringImpl::fixed_string_vector_constant(
					dynamic_cast<ColumnUInt8 &>(c0_fixed_string->getData()).getData(), c0_fixed_string->getN(),
					c1_const->getData(),
					c_res->getData());
			else if (c0_const && c1_string)
				StringImpl::constant_string_vector(
					c0_const->getData(),
					dynamic_cast<ColumnUInt8 &>(c1_string->getData()).getData(), c1_string->getOffsets(),
					c_res->getData());
			else if (c0_const && c1_fixed_string)
				StringImpl::constant_fixed_string_vector(
					c0_const->getData(),
					dynamic_cast<ColumnUInt8 &>(c1_fixed_string->getData()).getData(), c1_fixed_string->getN(),
					c_res->getData());
			else
				throw Exception("Illegal columns of arguments of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
	}
	
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypes getReturnTypes(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!(	(arguments[0]->isNumeric() && arguments[1]->isNumeric())
			||	(	(arguments[0]->getName() == "String" || arguments[0]->getName().substr(0, 11) == "FixedString")
				&& 	(arguments[1]->getName() == "String" || arguments[1]->getName().substr(0, 11) == "FixedString"))
			||	(arguments[0]->getName() == "Date" && arguments[1]->getName() == "Date")
			||	(arguments[0]->getName() == "DateTime" && arguments[1]->getName() == "DateTime")))
			throw Exception("Illegal types of arguments (" + arguments[0]->getName() + ", " + arguments[1]->getName() + ")"
				" of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
			
		DataTypes types_res;
		types_res.push_back(new DataTypeUInt8);
		return types_res;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & result)
	{
		if (result.size() != 1)
			throw Exception("Wrong number of result columns in function " + getName() + ", should be 1.",
				ErrorCodes::ILLEGAL_NUMBER_OF_RESULT_COLUMNS);

		if (block.getByPosition(arguments[0]).column->isNumeric())
		{
			if (!(	executeNumLeftType<UInt8>(block, arguments, result)
				||	executeNumLeftType<UInt16>(block, arguments, result)
				||	executeNumLeftType<UInt32>(block, arguments, result)
				||	executeNumLeftType<UInt64>(block, arguments, result)
				||	executeNumLeftType<Int8>(block, arguments, result)
				||	executeNumLeftType<Int16>(block, arguments, result)
				||	executeNumLeftType<Int32>(block, arguments, result)
				||	executeNumLeftType<Int64>(block, arguments, result)
				||	executeNumLeftType<Float32>(block, arguments, result)
				||	executeNumLeftType<Float64>(block, arguments, result)))
				throw Exception("Illegal column of first argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else
			executeString(block, arguments, result);
	}
};


struct NameEquals 			{ static const char * get() { return "equals"; } };

typedef FunctionComparison<EqualsNumImpl, 		EqualsStringImpl, 		NameEquals>			FunctionEquals;

}
