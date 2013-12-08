#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции - логические связки: and, or, not, xor.
  * Принимают любые числовые типы, возвращают UInt8, содержащий 0 или 1.
  */

template<typename A, typename B>
struct AndImpl
{
	static void vector_vector(const PODArray<A> & a, const PODArray<B> & b, PODArray<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] && b[i];
	}

	static void vector_constant(const PODArray<A> & a, B b, PODArray<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] && b;
	}

	static void constant_vector(A a, const PODArray<B> & b, PODArray<UInt8> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a && b[i];
	}

	static void constant_constant(A a, B b, UInt8 & c)
	{
		c = a && b;
	}
};

template<typename A, typename B>
struct OrImpl
{
	static void vector_vector(const PODArray<A> & a, const PODArray<B> & b, PODArray<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] || b[i];
	}

	static void vector_constant(const PODArray<A> & a, B b, PODArray<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] || b;
	}

	static void constant_vector(A a, const PODArray<B> & b, PODArray<UInt8> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a || b[i];
	}

	static void constant_constant(A a, B b, UInt8 & c)
	{
		c = a || b;
	}
};

template<typename A, typename B>
struct XorImpl
{
	static void vector_vector(const PODArray<A> & a, const PODArray<B> & b, PODArray<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = (a[i] && !b[i]) || (!a[i] && b[i]);
	}

	static void vector_constant(const PODArray<A> & a, B b, PODArray<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = (a[i] && !b) || (!a[i] && b);
	}

	static void constant_vector(A a, const PODArray<B> & b, PODArray<UInt8> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = (a && !b[i]) || (!a && b[i]);
	}

	static void constant_constant(A a, B b, UInt8 & c)
	{
		c = (a && !b) || (!a && b);
	}
};

template<typename A>
struct NotImpl
{
	static void vector(const PODArray<A> & a, PODArray<UInt8> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = !a[i];
	}

	static void constant(A a, UInt8 & c)
	{
		c = !a;
	}
};


template <template <typename, typename> class Impl, typename Name>
class FunctionBinaryLogical : public IFunction
{
private:

	template <typename T0, typename T1>
	bool executeRightType(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<T0> * col_left)
	{
		if (ColumnVector<T1> * col_right = dynamic_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			ColumnVector<UInt8> * col_res = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<UInt8>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			Impl<T0, T1>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T1> * col_right = dynamic_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			ColumnVector<UInt8> * col_res = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<UInt8>::Container_t & vec_res = col_res->getData();
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
			ColumnVector<UInt8> * col_res = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<UInt8>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col_left->size());
			Impl<T0, T1>::constant_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T1> * col_right = dynamic_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[1]).column))
		{
			UInt8 res = 0;
			Impl<T0, T1>::constant_constant(col_left->getData(), col_right->getData(), res);
			
			ColumnConst<UInt8> * col_res = new ColumnConst<UInt8>(col_left->size(), res);
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
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!(arguments[0]->isNumeric() && arguments[1]->isNumeric()))
			throw Exception("Illegal types ("
				+ arguments[0]->getName() + ", " + arguments[1]->getName()
				+ ") of arguments of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt8;
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
class FunctionUnaryLogical : public IFunction
{
private:

	template <typename T>
	bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (ColumnVector<T> * col = dynamic_cast<ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<UInt8> * col_res = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<UInt8>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->getData().size());
			Impl<T>::vector(col->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T> * col = dynamic_cast<ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			UInt8 res = 0;
			Impl<T>::constant(col->getData(), res);

			ColumnConst<UInt8> * col_res = new ColumnConst<UInt8>(col->size(), res);
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
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!arguments[0]->isNumeric())
			throw Exception("Illegal type ("
				+ arguments[0]->getName()
				+ ") of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt8;
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


struct NameAnd	{ static const char * get() { return "and"; } };
struct NameOr	{ static const char * get() { return "or"; } };
struct NameXor	{ static const char * get() { return "xor"; } };
struct NameNot	{ static const char * get() { return "not"; } };

typedef FunctionBinaryLogical<AndImpl,	NameAnd>	FunctionAnd;
typedef FunctionBinaryLogical<OrImpl,	NameOr>		FunctionOr;
typedef FunctionBinaryLogical<XorImpl,	NameXor>	FunctionXor;
typedef FunctionUnaryLogical<NotImpl,	NameNot>	FunctionNot;

}
