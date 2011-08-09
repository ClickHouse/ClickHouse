#pragma once

#include <Poco/NumberFormatter.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/NumberTraits.h>


namespace DB
{

/** Арифметические функции: +, -, *, /, %,
  * div (целочисленное деление),
  * TODO: <<, >>, <<<, >>>, &, |, ^, &&, ||, ^^, !
  */

template<typename A, typename B>
struct PlusImpl
{
	typedef typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type ResultType;
		
	static void vector_vector(const std::vector<A> & a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] + b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] + b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a + b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = a + b;
	}
};

template<typename A, typename B>
struct MultiplyImpl
{
	typedef typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type ResultType;

	static void vector_vector(const std::vector<A> & a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] * b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] * b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a * b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = a * b;
	}
};

template<typename A, typename B>
struct MinusImpl
{
	typedef typename NumberTraits::ResultOfSubtraction<A, B>::Type ResultType;

	static void vector_vector(const std::vector<A> & a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] - b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] - b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a - b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = a - b;
	}
};

template<typename A, typename B>
struct DivideFloatingImpl
{
	typedef typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type ResultType;

	static void vector_vector(const std::vector<A> & a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a[i]) / b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a[i]) / b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a) / b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = static_cast<ResultType>(a) / b;
	}
};

template<typename A, typename B>
struct DivideIntegralImpl
{
	typedef typename NumberTraits::ResultOfIntegerDivision<A, B>::Type ResultType;

	static void vector_vector(const std::vector<A> & a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] / b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] / b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a / b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = a / b;
	}
};

template<typename A, typename B>
struct ModuloImpl
{
	typedef typename NumberTraits::ResultOfModulo<A, B>::Type ResultType;

	static void vector_vector(const std::vector<A> & a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] % b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] % b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a % b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = a % b;
	}
};


template <template <typename, typename> class Impl, typename Name>
class FunctionBinaryArithmetic : public IFunction
{
public:
	/// Получить все имена функции.
	Names getNames() const
	{
		Names names;
		names.push_back(Name::get());
		return names;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypes getReturnTypes(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		DataTypes types_res;

		#define CHECK_RIGHT_TYPE(TYPE0, TYPE1) \
			if (dynamic_cast<const DataType ## TYPE1 *>(&*arguments[1])) \
				types_res.push_back(new typename DataTypeFromFieldType<typename Impl<TYPE0, TYPE1>::ResultType>::Type);

		#define CHECK_LEFT_TYPE(TYPE0) 											\
			if (dynamic_cast<const DataType ## TYPE0 *>(&*arguments[0])) 		\
			{ 																	\
						CHECK_RIGHT_TYPE(TYPE0, UInt8)		\
				else 	CHECK_RIGHT_TYPE(TYPE0, UInt16)		\
				else	CHECK_RIGHT_TYPE(TYPE0, UInt32)		\
				else	CHECK_RIGHT_TYPE(TYPE0, UInt64)		\
				else	CHECK_RIGHT_TYPE(TYPE0, Int8)		\
				else	CHECK_RIGHT_TYPE(TYPE0, Int16)		\
				else	CHECK_RIGHT_TYPE(TYPE0, Int32)		\
				else	CHECK_RIGHT_TYPE(TYPE0, Int64)		\
				else										\
					throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),	\
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT); \
			}

				CHECK_LEFT_TYPE(UInt8)
		else 	CHECK_LEFT_TYPE(UInt16)
		else 	CHECK_LEFT_TYPE(UInt32)
		else 	CHECK_LEFT_TYPE(UInt64)
		else 	CHECK_LEFT_TYPE(Int8)
		else 	CHECK_LEFT_TYPE(Int16)
		else 	CHECK_LEFT_TYPE(Int32)
		else 	CHECK_LEFT_TYPE(Int64)
		else
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		#undef CHECK_LEFT_TYPE
		#undef CHECK_RIGHT_TYPE

		return types_res;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & result)
	{
		if (result.size() != 1)
			throw Exception("Wrong number of result columns in function " + getName() + ", should be 1.",
				ErrorCodes::ILLEGAL_NUMBER_OF_RESULT_COLUMNS);

		#define CHECK_RIGHT_TYPE(TYPE0, TYPE1) \
			if (Column ## TYPE1 * col_right = dynamic_cast<Column ## TYPE1 *>(&*block.getByPosition(arguments[1]).column))	\
			{																												\
				typedef typename Impl<TYPE0, TYPE1>::ResultType ResultType;													\
																															\
				typename ColumnVector<ResultType>::Container_t & vec_res =													\
					dynamic_cast<ColumnVector<ResultType> &>(*block.getByPosition(result[0]).column).getData();				\
																															\
				vec_res.resize(col_left->getData().size());																	\
																															\
				Impl<TYPE0, TYPE1>::vector_vector(col_left->getData(), col_right->getData(), vec_res);						\
			}

		#define CHECK_LEFT_TYPE(TYPE0) 																						\
			if (Column ## TYPE0 * col_left = dynamic_cast<Column ## TYPE0 *>(&*block.getByPosition(arguments[0]).column)) 	\
			{ 												\
						CHECK_RIGHT_TYPE(TYPE0, UInt8)		\
				else 	CHECK_RIGHT_TYPE(TYPE0, UInt16)		\
				else	CHECK_RIGHT_TYPE(TYPE0, UInt32)		\
				else	CHECK_RIGHT_TYPE(TYPE0, UInt64)		\
				else	CHECK_RIGHT_TYPE(TYPE0, Int8)		\
				else	CHECK_RIGHT_TYPE(TYPE0, Int16)		\
				else	CHECK_RIGHT_TYPE(TYPE0, Int32)		\
				else	CHECK_RIGHT_TYPE(TYPE0, Int64)		\
				else										\
				    throw Exception("Illegal column of second argument of function " + getName(),	\
						ErrorCodes::ILLEGAL_COLUMN);												\
			}

				CHECK_LEFT_TYPE(UInt8)
		else 	CHECK_LEFT_TYPE(UInt16)
		else 	CHECK_LEFT_TYPE(UInt32)
		else 	CHECK_LEFT_TYPE(UInt64)
		else 	CHECK_LEFT_TYPE(Int8)
		else 	CHECK_LEFT_TYPE(Int16)
		else 	CHECK_LEFT_TYPE(Int32)
		else 	CHECK_LEFT_TYPE(Int64)
		else
		   throw Exception("Illegal column of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		#undef CHECK_LEFT_TYPE
		#undef CHECK_RIGHT_TYPE
	}
};


struct NamePlus 			{ static const char * get() { return "plus"; } };
struct NameMinus 			{ static const char * get() { return "minus"; } };
struct NameMultiply 		{ static const char * get() { return "multiply"; } };
struct NameDivideFloating	{ static const char * get() { return "divide"; } };
struct NameDivideIntegral 	{ static const char * get() { return "div"; } };
struct NameModulo 			{ static const char * get() { return "modulo"; } };

typedef FunctionBinaryArithmetic<PlusImpl, 				NamePlus> 			FunctionPlus;
typedef FunctionBinaryArithmetic<MinusImpl, 			NameMinus> 			FunctionMinus;
typedef FunctionBinaryArithmetic<MultiplyImpl, 			NameMultiply> 		FunctionMultiply;
typedef FunctionBinaryArithmetic<DivideFloatingImpl, 	NameDivideFloating> FunctionDivideFloating;
typedef FunctionBinaryArithmetic<DivideIntegralImpl, 	NameDivideIntegral> FunctionDivideIntegral;
typedef FunctionBinaryArithmetic<ModuloImpl, 			NameModulo> 		FunctionModulo;


}
