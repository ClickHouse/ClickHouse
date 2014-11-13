#pragma once

#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
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

template <typename A, typename B> struct EqualsOp 			{ static UInt8 apply(A a, B b) { return a == b; } };
template <typename A, typename B> struct NotEqualsOp 		{ static UInt8 apply(A a, B b) { return a != b; } };
template <typename A, typename B> struct LessOp 			{ static UInt8 apply(A a, B b) { return a < b; 	} };
template <typename A, typename B> struct GreaterOp 			{ static UInt8 apply(A a, B b) { return a > b; 	} };
template <typename A, typename B> struct LessOrEqualsOp 	{ static UInt8 apply(A a, B b) { return a <= b; } };
template <typename A, typename B> struct GreaterOrEqualsOp 	{ static UInt8 apply(A a, B b) { return a >= b; } };

#pragma GCC diagnostic pop



template<typename A, typename B, typename Op>
struct NumComparisonImpl
{
	static void vector_vector(const PODArray<A> & a, const PODArray<B> & b, PODArray<UInt8> & c)
	{
		/** GCC 4.8.2 векторизует цикл только если его записать в такой форме.
		  * В данном случае, если сделать цикл по индексу массива (код будет выглядеть проще),
		  *  цикл не будет векторизовываться.
		  */

		size_t size = a.size();
		const A * a_pos = &a[0];
		const B * b_pos = &b[0];
		UInt8 * c_pos = &c[0];
		const A * a_end = a_pos + size;

		while (a_pos < a_end)
		{
			*c_pos = Op::apply(*a_pos, *b_pos);
			++a_pos;
			++b_pos;
			++c_pos;
		}
	}

	static void vector_constant(const PODArray<A> & a, B b, PODArray<UInt8> & c)
	{
		size_t size = a.size();
		const A * a_pos = &a[0];
		UInt8 * c_pos = &c[0];
		const A * a_end = a_pos + size;

		while (a_pos < a_end)
		{
			*c_pos = Op::apply(*a_pos, b);
			++a_pos;
			++c_pos;
		}
	}

	static void constant_vector(A a, const PODArray<B> & b, PODArray<UInt8> & c)
	{
		size_t size = b.size();
		const B * b_pos = &b[0];
		UInt8 * c_pos = &c[0];
		const B * b_end = b_pos + size;

		while (b_pos < b_end)
		{
			*c_pos = Op::apply(a, *b_pos);
			++b_pos;
			++c_pos;
		}
	}

	static void constant_constant(A a, B b, UInt8 & c)
	{
		c = Op::apply(a, b);
	}
};


template <typename Op>
struct StringComparisonImpl
{
	static void string_vector_string_vector(
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		PODArray<UInt8> & c)
	{
		size_t size = a_offsets.size();
		for (size_t i = 0; i < size; ++i)
		{
			if (i == 0)
			{
				/// Завершающий ноль в меньшей по длине строке входит в сравнение.
				c[i] = Op::apply(memcmp(&a_data[0], &b_data[0], std::min(a_offsets[0], b_offsets[0])), 0);
			}
			else
			{
				c[i] = Op::apply(memcmp(&a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]],
					std::min(a_offsets[i] - a_offsets[i - 1], b_offsets[i] - b_offsets[i - 1])), 0);
			}
		}
	}

	static void string_vector_fixed_string_vector(
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
		PODArray<UInt8> & c)
	{
		size_t size = a_offsets.size();
		for (size_t i = 0; i < size; ++i)
		{
			if (i == 0)
			{
				int res = memcmp(&a_data[0], &b_data[0], std::min(a_offsets[0] - 1, b_n));
				c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_offsets[0], b_n + 1));
			}
			else
			{
				int res = memcmp(&a_data[a_offsets[i - 1]], &b_data[i * b_n],
					std::min(a_offsets[i] - a_offsets[i - 1] - 1, b_n));
				c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_offsets[i] - a_offsets[i - 1], b_n + 1));
			}
		}
	}

	static void string_vector_constant(
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const std::string & b,
		PODArray<UInt8> & c)
	{
		size_t size = a_offsets.size();
		ColumnString::Offset_t b_n = b.size();
		const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
		for (size_t i = 0; i < size; ++i)
		{
			if (i == 0)
			{
				c[i] = Op::apply(memcmp(&a_data[0], b_data, std::min(a_offsets[0], b_n + 1)), 0);
			}
			else
			{
				c[i] = Op::apply(memcmp(&a_data[a_offsets[i - 1]], b_data,
					std::min(a_offsets[i] - a_offsets[i - 1], b_n + 1)), 0);
			}
		}
	}

	static void fixed_string_vector_string_vector(
		const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		PODArray<UInt8> & c)
	{
		size_t size = b_offsets.size();
		for (size_t i = 0; i < size; ++i)
		{
			if (i == 0)
			{
				int res = memcmp(&a_data[0], &b_data[0], std::min(b_offsets[0] - 1, a_n));
				c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_n + 1, b_offsets[0]));
			}
			else
			{
				int res = memcmp(&a_data[i * a_n], &b_data[b_offsets[i - 1]],
					std::min(b_offsets[i] - b_offsets[i - 1] - 1, a_n));
				c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_n + 1, b_offsets[i] - b_offsets[i - 1]));
			}
		}
	}

	static void fixed_string_vector_fixed_string_vector(
		const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
		const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
		PODArray<UInt8> & c)
	{
		size_t size = a_data.size();
		for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
		{
			int res = memcmp(&a_data[i], &b_data[i], std::min(a_n, b_n));
			c[j] = Op::apply(res, 0) || (res == 0 && Op::apply(a_n, b_n));
		}
	}

	static void fixed_string_vector_constant(
		const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
		const std::string & b,
		PODArray<UInt8> & c)
	{
		size_t size = a_data.size();
		const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
		ColumnString::Offset_t b_n = b.size();
		for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
		{
			int res = memcmp(&a_data[i], b_data, std::min(a_n, b_n));
			c[j] = Op::apply(res, 0) || (res == 0 && Op::apply(a_n, b_n));
		}
	}

	static void constant_string_vector(
		const std::string & a,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		PODArray<UInt8> & c)
	{
		size_t size = b_offsets.size();
		ColumnString::Offset_t a_n = a.size();
		const UInt8 * a_data = reinterpret_cast<const UInt8 *>(a.data());
		for (size_t i = 0; i < size; ++i)
		{
			if (i == 0)
			{
				c[i] = Op::apply(memcmp(a_data, &b_data[0], std::min(b_offsets[0], a_n + 1)), 0);
			}
			else
			{
				c[i] = Op::apply(memcmp(a_data, &b_data[b_offsets[i - 1]],
					std::min(b_offsets[i] - b_offsets[i - 1], a_n + 1)), 0);
			}
		}
	}

	static void constant_fixed_string_vector(
		const std::string & a,
		const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
		PODArray<UInt8> & c)
	{
		size_t size = b_data.size();
		const UInt8 * a_data = reinterpret_cast<const UInt8 *>(a.data());
		ColumnString::Offset_t a_n = a.size();
		for (size_t i = 0, j = 0; i < size; i += b_n, ++j)
		{
			int res = memcmp(a_data, &b_data[i], std::min(a_n, b_n));
			c[j] = Op::apply(res, 0) || (res == 0 && Op::apply(b_n, a_n));
		}
	}

	static void constant_constant(
		const std::string & a,
		const std::string & b,
		UInt8 & c)
	{
		c = Op::apply(memcmp(a.data(), b.data(), std::min(a.size(), b.size()) + 1), 0);
	}
};


/// Сравнения на равенство/неравенство реализованы несколько более эффективно.
template <bool positive>
struct StringEqualsImpl
{
	static void string_vector_string_vector(
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		PODArray<UInt8> & c)
	{
		size_t size = a_offsets.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = positive == ((i == 0)
				? (a_offsets[0] == b_offsets[0] && !memcmp(&a_data[0], &b_data[0], a_offsets[0] - 1))
				: (a_offsets[i] - a_offsets[i - 1] == b_offsets[i] - b_offsets[i - 1]
					&& !memcmp(&a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], a_offsets[i] - a_offsets[i - 1] - 1)));
	}

	static void string_vector_fixed_string_vector(
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
		PODArray<UInt8> & c)
	{
		size_t size = a_offsets.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = positive == ((i == 0)
				? (a_offsets[0] == b_n + 1 && !memcmp(&a_data[0], &b_data[0], b_n))
				: (a_offsets[i] - a_offsets[i - 1] == b_n + 1
					&& !memcmp(&a_data[a_offsets[i - 1]], &b_data[b_n * i], b_n)));
	}

	static void string_vector_constant(
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const std::string & b,
		PODArray<UInt8> & c)
	{
		size_t size = a_offsets.size();
		ColumnString::Offset_t b_n = b.size();
		const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
		for (size_t i = 0; i < size; ++i)
			c[i] = positive == ((i == 0)
				? (a_offsets[0] == b_n + 1 && !memcmp(&a_data[0], b_data, b_n))
				: (a_offsets[i] - a_offsets[i - 1] == b_n + 1
					&& !memcmp(&a_data[a_offsets[i - 1]], b_data, b_n)));
	}

	static void fixed_string_vector_fixed_string_vector(
		const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
		const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
		PODArray<UInt8> & c)
	{
		size_t size = a_data.size();
		for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
			c[j] = positive == (a_n == b_n && !memcmp(&a_data[i], &b_data[i], a_n));
	}

	static void fixed_string_vector_constant(
		const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
		const std::string & b,
		PODArray<UInt8> & c)
	{
		size_t size = a_data.size();
		const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
		ColumnString::Offset_t b_n = b.size();
		for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
			c[j] = positive == (a_n == b_n && !memcmp(&a_data[i], b_data, a_n));
	}

	static void constant_constant(
		const std::string & a,
		const std::string & b,
		UInt8 & c)
	{
		c = positive == (a == b);
	}

	static void fixed_string_vector_string_vector(
		const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		PODArray<UInt8> & c)
	{
		string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
	}

	static void constant_string_vector(
		const std::string & a,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		PODArray<UInt8> & c)
	{
		string_vector_constant(b_data, b_offsets, a, c);
	}

	static void constant_fixed_string_vector(
		const std::string & a,
		const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
		PODArray<UInt8> & c)
	{
		fixed_string_vector_constant(b_data, b_n, a, c);
	}
};


template <typename A, typename B>
struct StringComparisonImpl<EqualsOp<A, B>> : StringEqualsImpl<true> {};

template <typename A, typename B>
struct StringComparisonImpl<NotEqualsOp<A, B>> : StringEqualsImpl<false> {};


template <
	template <typename, typename> class Op,
	typename Name>
class FunctionComparison : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionComparison; };

private:
	template <typename T0, typename T1>
	bool executeNumRightType(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<T0> * col_left)
	{
		if (ColumnVector<T1> * col_right = typeid_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			ColumnUInt8 * col_res = new ColumnUInt8;
			block.getByPosition(result).column = col_res;

			ColumnUInt8::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T1> * col_right = typeid_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			ColumnUInt8 * col_res = new ColumnUInt8;
			block.getByPosition(result).column = col_res;

			ColumnUInt8::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_constant(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}

		return false;
	}

	template <typename T0, typename T1>
	bool executeNumConstRightType(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnConst<T0> * col_left)
	{
		if (ColumnVector<T1> * col_right = typeid_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			ColumnUInt8 * col_res = new ColumnUInt8;
			block.getByPosition(result).column = col_res;

			ColumnUInt8::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->size());
			NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T1> * col_right = typeid_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			UInt8 res = 0;
			NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_constant(col_left->getData(), col_right->getData(), res);

			ColumnConstUInt8 * col_res = new ColumnConstUInt8(col_left->size(), res);
			block.getByPosition(result).column = col_res;

			return true;
		}

		return false;
	}

	template <typename T0>
	bool executeNumLeftType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (ColumnVector<T0> * col_left = typeid_cast<ColumnVector<T0> *>(&*block.getByPosition(arguments[0]).column))
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
				throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
					+ " of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (ColumnConst<T0> * col_left = typeid_cast<ColumnConst<T0> *>(&*block.getByPosition(arguments[0]).column))
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
				throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
					+ " of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}

		return false;
	}

	void executeString(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		IColumn * c0 = &*block.getByPosition(arguments[0]).column;
		IColumn * c1 = &*block.getByPosition(arguments[1]).column;

		ColumnString * c0_string = typeid_cast<ColumnString *>(c0);
		ColumnString * c1_string = typeid_cast<ColumnString *>(c1);
		ColumnFixedString * c0_fixed_string = typeid_cast<ColumnFixedString *>(c0);
		ColumnFixedString * c1_fixed_string = typeid_cast<ColumnFixedString *>(c1);
		ColumnConstString * c0_const = typeid_cast<ColumnConstString *>(c0);
		ColumnConstString * c1_const = typeid_cast<ColumnConstString *>(c1);

		using StringImpl = StringComparisonImpl<Op<int, int>>;

		if (c0_const && c1_const)
		{
			ColumnConstUInt8 * c_res = new ColumnConstUInt8(c0_const->size(), 0);
			block.getByPosition(result).column = c_res;
			StringImpl::constant_constant(c0_const->getData(), c1_const->getData(), c_res->getData());
		}
		else
		{
			ColumnUInt8 * c_res = new ColumnUInt8;
			block.getByPosition(result).column = c_res;
			ColumnUInt8::Container_t & vec_res = c_res->getData();
			vec_res.resize(c0->size());

			if (c0_string && c1_string)
				StringImpl::string_vector_string_vector(
					c0_string->getChars(), c0_string->getOffsets(),
					c1_string->getChars(), c1_string->getOffsets(),
					c_res->getData());
			else if (c0_string && c1_fixed_string)
				StringImpl::string_vector_fixed_string_vector(
					c0_string->getChars(), c0_string->getOffsets(),
					c1_fixed_string->getChars(), c1_fixed_string->getN(),
					c_res->getData());
			else if (c0_string && c1_const)
				StringImpl::string_vector_constant(
					c0_string->getChars(), c0_string->getOffsets(),
					c1_const->getData(),
					c_res->getData());
			else if (c0_fixed_string && c1_string)
				StringImpl::fixed_string_vector_string_vector(
					c0_fixed_string->getChars(), c0_fixed_string->getN(),
					c1_string->getChars(), c1_string->getOffsets(),
					c_res->getData());
			else if (c0_fixed_string && c1_fixed_string)
				StringImpl::fixed_string_vector_fixed_string_vector(
					c0_fixed_string->getChars(), c0_fixed_string->getN(),
					c1_fixed_string->getChars(), c1_fixed_string->getN(),
					c_res->getData());
			else if (c0_fixed_string && c1_const)
				StringImpl::fixed_string_vector_constant(
					c0_fixed_string->getChars(), c0_fixed_string->getN(),
					c1_const->getData(),
					c_res->getData());
			else if (c0_const && c1_string)
				StringImpl::constant_string_vector(
					c0_const->getData(),
					c1_string->getChars(), c1_string->getOffsets(),
					c_res->getData());
			else if (c0_const && c1_fixed_string)
				StringImpl::constant_fixed_string_vector(
					c0_const->getData(),
					c1_fixed_string->getChars(), c1_fixed_string->getN(),
					c_res->getData());
			else
				throw Exception("Illegal columns "
					+ block.getByPosition(arguments[0]).column->getName() + " and "
					+ block.getByPosition(arguments[1]).column->getName()
					+ " of arguments of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
	}

public:
	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!(	(	arguments[0]->isNumeric() && arguments[0]->behavesAsNumber()
				&&	arguments[1]->isNumeric() && arguments[1]->behavesAsNumber())
			||	(	(arguments[0]->getName() == "String" || arguments[0]->getName().substr(0, 11) == "FixedString")
				&& 	(arguments[1]->getName() == "String" || arguments[1]->getName().substr(0, 11) == "FixedString"))
			||	(arguments[0]->getName() == "Date" && arguments[1]->getName() == "Date")
			||	(arguments[0]->getName() == "DateTime" && arguments[1]->getName() == "DateTime")))
			throw Exception("Illegal types of arguments (" + arguments[0]->getName() + ", " + arguments[1]->getName() + ")"
				" of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt8;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
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
				throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else
			executeString(block, arguments, result);
	}
};


struct NameEquals 			{ static constexpr auto name = "equals"; };
struct NameNotEquals 		{ static constexpr auto name = "notEquals"; };
struct NameLess 			{ static constexpr auto name = "less"; };
struct NameGreater 			{ static constexpr auto name = "greater"; };
struct NameLessOrEquals 	{ static constexpr auto name = "lessOrEquals"; };
struct NameGreaterOrEquals 	{ static constexpr auto name = "greaterOrEquals"; };

typedef FunctionComparison<EqualsOp, 			NameEquals>				FunctionEquals;
typedef FunctionComparison<NotEqualsOp, 		NameNotEquals>			FunctionNotEquals;
typedef FunctionComparison<LessOp, 				NameLess>				FunctionLess;
typedef FunctionComparison<GreaterOp, 			NameGreater>			FunctionGreater;
typedef FunctionComparison<LessOrEqualsOp, 		NameLessOrEquals>		FunctionLessOrEquals;
typedef FunctionComparison<GreaterOrEqualsOp,	NameGreaterOrEquals>	FunctionGreaterOrEquals;

}
