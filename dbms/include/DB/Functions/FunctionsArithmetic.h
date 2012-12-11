#pragma once

#include <Poco/NumberFormatter.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/NumberTraits.h>


namespace DB
{

/** Арифметические функции: +, -, *, /, %,
  * intDiv (целочисленное деление), унарный минус.
  */

template<typename A, typename B>
struct PlusImpl
{
	typedef typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type ResultType;
		
	static void vector_vector(const std::vector<A> & a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		/// Далее везде, static_cast - чтобы не было неправильного результата в выражениях вида Int64 c = UInt32(a) * Int32(-1).
		
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a[i]) + b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a[i]) + b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a) + b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = static_cast<ResultType>(a) + b;
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
			c[i] = static_cast<ResultType>(a[i]) * b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a[i]) * b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a) * b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = static_cast<ResultType>(a) * b;
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
			c[i] = static_cast<ResultType>(a[i]) - b[i];
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a[i]) - b;
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a) - b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = static_cast<ResultType>(a) - b;
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
		size_t size = b.size();
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
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = static_cast<ResultType>(a) / b[i];
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = static_cast<ResultType>(a) / b;
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
			c[i] = typename NumberTraits::ToInteger<A>::Type(a[i])
				% typename NumberTraits::ToInteger<A>::Type(b[i]);
	}

	static void vector_constant(const std::vector<A> & a, B b, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = typename NumberTraits::ToInteger<A>::Type(a[i])
				% typename NumberTraits::ToInteger<A>::Type(b);
	}

	static void constant_vector(A a, const std::vector<B> & b, std::vector<ResultType> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = typename NumberTraits::ToInteger<A>::Type(a)
				% typename NumberTraits::ToInteger<A>::Type(b[i]);
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = typename NumberTraits::ToInteger<A>::Type(a)
			% typename NumberTraits::ToInteger<A>::Type(b);
	}
};

template<typename A>
struct NegateImpl
{
	typedef typename NumberTraits::ResultOfNegate<A>::Type ResultType;

	static void vector(const std::vector<A> & a, std::vector<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = -a[i];
	}

	static void constant(A a, ResultType & c)
	{
		c = -a;
	}
};


template <template <typename, typename> class Impl, typename Name>
class FunctionBinaryArithmetic : public IFunction
{
private:
	template <typename T0, typename T1>
	bool checkRightType(const DataTypes & arguments, DataTypePtr & type_res) const
	{
		if (dynamic_cast<const T1 *>(&*arguments[1]))
		{
			type_res = new typename DataTypeFromFieldType<
				typename Impl<typename T0::FieldType, typename T1::FieldType>::ResultType>::Type;
			return true;
		}
		return false;
	}

	template <typename T0>
	bool checkLeftType(const DataTypes & arguments, DataTypePtr & type_res) const
	{
		if (dynamic_cast<const T0 *>(&*arguments[0]))
		{
			if (	checkRightType<T0, DataTypeUInt8>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt16>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt32>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt64>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt8>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt16>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt32>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt64>(arguments, type_res)
				||	checkRightType<T0, DataTypeFloat32>(arguments, type_res)
				||	checkRightType<T0, DataTypeFloat64>(arguments, type_res)
				||	checkRightType<T0, DataTypeVarUInt>(arguments, type_res)
				||	checkRightType<T0, DataTypeVarInt>(arguments, type_res))
				return true;
			else
				throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		return false;
	}


	template <typename T0, typename T1>
	bool executeRightType(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<T0> * col_left)
	{
		if (ColumnVector<T1> * col_right = dynamic_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			typedef typename Impl<T0, T1>::ResultType ResultType;

			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			Impl<T0, T1>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T1> * col_right = dynamic_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			typedef typename Impl<T0, T1>::ResultType ResultType;

			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			Impl<T0, T1>::vector_constant(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
			
		return false;
	}

	template <typename T0, typename T1>
	bool executeConstRightType(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnConst<T0> * col_left)
	{
		if (ColumnVector<T1> * col_right = dynamic_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			typedef typename Impl<T0, T1>::ResultType ResultType;

			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->size());
			Impl<T0, T1>::constant_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T1> * col_right = dynamic_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			typedef typename Impl<T0, T1>::ResultType ResultType;

			ResultType res = 0;
			Impl<T0, T1>::constant_constant(col_left->getData(), col_right->getData(), res);
			
			ColumnConst<ResultType> * col_res = new ColumnConst<ResultType>(col_left->size(), res);
			block.getByPosition(result).column = col_res;

			return true;
		}

		return false;
	}

	template <typename T0>
	bool executeLeftType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (ColumnVector<T0> * col_left = dynamic_cast<ColumnVector<T0> *>(&*block.getByPosition(arguments[0]).column))
		{
			if (	executeRightType<T0, UInt8>(block, arguments, result, col_left)
				||	executeRightType<T0, UInt16>(block, arguments, result, col_left)
				||	executeRightType<T0, UInt32>(block, arguments, result, col_left)
				||	executeRightType<T0, UInt64>(block, arguments, result, col_left)
				||	executeRightType<T0, Int8>(block, arguments, result, col_left)
				||	executeRightType<T0, Int16>(block, arguments, result, col_left)
				||	executeRightType<T0, Int32>(block, arguments, result, col_left)
				||	executeRightType<T0, Int64>(block, arguments, result, col_left)
				||	executeRightType<T0, Float32>(block, arguments, result, col_left)
				||	executeRightType<T0, Float64>(block, arguments, result, col_left))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
					+ " of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (ColumnConst<T0> * col_left = dynamic_cast<ColumnConst<T0> *>(&*block.getByPosition(arguments[0]).column))
		{
			if (	executeConstRightType<T0, UInt8>(block, arguments, result, col_left)
				||	executeConstRightType<T0, UInt16>(block, arguments, result, col_left)
				||	executeConstRightType<T0, UInt32>(block, arguments, result, col_left)
				||	executeConstRightType<T0, UInt64>(block, arguments, result, col_left)
				||	executeConstRightType<T0, Int8>(block, arguments, result, col_left)
				||	executeConstRightType<T0, Int16>(block, arguments, result, col_left)
				||	executeConstRightType<T0, Int32>(block, arguments, result, col_left)
				||	executeConstRightType<T0, Int64>(block, arguments, result, col_left)
				||	executeConstRightType<T0, Float32>(block, arguments, result, col_left)
				||	executeConstRightType<T0, Float64>(block, arguments, result, col_left))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
					+ " of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		
		return false;
	}
	
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		DataTypePtr type_res;

		if (!(	checkLeftType<DataTypeUInt8>(arguments, type_res)
			||	checkLeftType<DataTypeUInt16>(arguments, type_res)
			||	checkLeftType<DataTypeUInt32>(arguments, type_res)
			||	checkLeftType<DataTypeUInt64>(arguments, type_res)
			||	checkLeftType<DataTypeInt8>(arguments, type_res)
			||	checkLeftType<DataTypeInt16>(arguments, type_res)
			||	checkLeftType<DataTypeInt32>(arguments, type_res)
			||	checkLeftType<DataTypeInt64>(arguments, type_res)
			||	checkLeftType<DataTypeFloat32>(arguments, type_res)
			||	checkLeftType<DataTypeFloat64>(arguments, type_res)
			||	checkLeftType<DataTypeVarUInt>(arguments, type_res)
			||	checkLeftType<DataTypeVarInt>(arguments, type_res)))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return type_res;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (!(	executeLeftType<UInt8>(block, arguments, result)
			||	executeLeftType<UInt16>(block, arguments, result)
			||	executeLeftType<UInt32>(block, arguments, result)
			||	executeLeftType<UInt64>(block, arguments, result)
			||	executeLeftType<Int8>(block, arguments, result)
			||	executeLeftType<Int16>(block, arguments, result)
			||	executeLeftType<Int32>(block, arguments, result)
			||	executeLeftType<Int64>(block, arguments, result)
			||	executeLeftType<Float32>(block, arguments, result)
			||	executeLeftType<Float64>(block, arguments, result)))
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <template <typename> class Impl, typename Name>
class FunctionUnaryArithmetic : public IFunction
{
private:
	template <typename T0>
	bool checkType(const DataTypes & arguments, DataTypePtr & result) const
	{
		if (dynamic_cast<const T0 *>(&*arguments[0]))
		{
			result = new typename DataTypeFromFieldType<
				typename Impl<typename T0::FieldType>::ResultType>::Type;
			return true;
		}
		return false;
	}

	template <typename T0>
	bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (ColumnVector<T0> * col = dynamic_cast<ColumnVector<T0> *>(&*block.getByPosition(arguments[0]).column))
		{
			typedef typename Impl<T0>::ResultType ResultType;

			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->getData().size());
			Impl<T0>::vector(col->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T0> * col = dynamic_cast<ColumnConst<T0> *>(&*block.getByPosition(arguments[0]).column))
		{
			typedef typename Impl<T0>::ResultType ResultType;

			ResultType res = 0;
			Impl<T0>::constant(col->getData(), res);

			ColumnConst<ResultType> * col_res = new ColumnConst<ResultType>(col->size(), res);
			block.getByPosition(result).column = col_res;

			return true;
		}

		return false;
	}

public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		DataTypePtr result;

		if (!(	checkType<DataTypeUInt8>(arguments, result)
			||	checkType<DataTypeUInt16>(arguments, result)
			||	checkType<DataTypeUInt32>(arguments, result)
			||	checkType<DataTypeUInt64>(arguments, result)
			||	checkType<DataTypeInt8>(arguments, result)
			||	checkType<DataTypeInt16>(arguments, result)
			||	checkType<DataTypeInt32>(arguments, result)
			||	checkType<DataTypeInt64>(arguments, result)
			||	checkType<DataTypeFloat32>(arguments, result)
			||	checkType<DataTypeFloat64>(arguments, result)
			||	checkType<DataTypeVarUInt>(arguments, result)
			||	checkType<DataTypeVarInt>(arguments, result)))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return result;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (!(	executeType<UInt8>(block, arguments, result)
			||	executeType<UInt16>(block, arguments, result)
			||	executeType<UInt32>(block, arguments, result)
			||	executeType<UInt64>(block, arguments, result)
			||	executeType<Int8>(block, arguments, result)
			||	executeType<Int16>(block, arguments, result)
			||	executeType<Int32>(block, arguments, result)
			||	executeType<Int64>(block, arguments, result)
			||	executeType<Float32>(block, arguments, result)
			||	executeType<Float64>(block, arguments, result)))
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NamePlus 			{ static const char * get() { return "plus"; } };
struct NameMinus 			{ static const char * get() { return "minus"; } };
struct NameMultiply 		{ static const char * get() { return "multiply"; } };
struct NameDivideFloating	{ static const char * get() { return "divide"; } };
struct NameDivideIntegral 	{ static const char * get() { return "intDiv"; } };
struct NameModulo 			{ static const char * get() { return "modulo"; } };
struct NameNegate 			{ static const char * get() { return "negate"; } };

typedef FunctionBinaryArithmetic<PlusImpl, 				NamePlus> 			FunctionPlus;
typedef FunctionBinaryArithmetic<MinusImpl, 			NameMinus> 			FunctionMinus;
typedef FunctionBinaryArithmetic<MultiplyImpl, 			NameMultiply> 		FunctionMultiply;
typedef FunctionBinaryArithmetic<DivideFloatingImpl, 	NameDivideFloating> FunctionDivideFloating;
typedef FunctionBinaryArithmetic<DivideIntegralImpl, 	NameDivideIntegral> FunctionDivideIntegral;
typedef FunctionBinaryArithmetic<ModuloImpl, 			NameModulo> 		FunctionModulo;
typedef FunctionUnaryArithmetic<NegateImpl, 			NameNegate> 		FunctionNegate;


}
