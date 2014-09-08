#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/NumberTraits.h>


namespace DB
{

/** Арифметические функции: +, -, *, /, %,
  * intDiv (целочисленное деление), унарный минус.
  * Битовые функции: |, &, ^, ~.
  */

template<typename A, typename B, typename Op, typename ResultType_ = typename Op::ResultType>
struct BinaryOperationImplBase
{
	typedef ResultType_ ResultType;

	static void vector_vector(const PODArray<A> & a, const PODArray<B> & b, PODArray<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = Op::template apply<ResultType>(a[i], b[i]);
	}

	static void vector_constant(const PODArray<A> & a, B b, PODArray<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = Op::template apply<ResultType>(a[i], b);
	}

	static void constant_vector(A a, const PODArray<B> & b, PODArray<ResultType> & c)
	{
		size_t size = b.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = Op::template apply<ResultType>(a, b[i]);
	}

	static void constant_constant(A a, B b, ResultType & c)
	{
		c = Op::template apply<ResultType>(a, b);
	}
};

template<typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl : BinaryOperationImplBase<A, B, Op, ResultType>
{
};


template<typename A, typename Op>
struct UnaryOperationImpl
{
	typedef typename Op::ResultType ResultType;

	static void vector(const PODArray<A> & a, PODArray<ResultType> & c)
	{
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = Op::apply(a[i]);
	}

	static void constant(A a, ResultType & c)
	{
		c = Op::apply(a);
	}
};


template<typename A, typename B>
struct PlusImpl
{
	typedef typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		/// Далее везде, static_cast - чтобы не было неправильного результата в выражениях вида Int64 c = UInt32(a) * Int32(-1).
		return static_cast<Result>(a) + b;
	}
};


template<typename A, typename B>
struct MultiplyImpl
{
	typedef typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		return static_cast<Result>(a) * b;
	}
};

template<typename A, typename B>
struct MinusImpl
{
	typedef typename NumberTraits::ResultOfSubtraction<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		return static_cast<Result>(a) - b;
	}
};

template<typename A, typename B>
struct DivideFloatingImpl
{
	typedef typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		return static_cast<Result>(a) / b;
	}
};


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

template <typename A, typename B>
inline void throwIfDivisionLeadsToFPE(A a, B b)
{
	/// Возможно, лучше вместо проверок использовать siglongjmp?

	if (unlikely(b == 0))
		throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

	/// http://avva.livejournal.com/2548306.html
	if (unlikely(std::is_signed<A>::value && std::is_signed<B>::value && a == std::numeric_limits<A>::min() && b == -1))
		throw Exception("Division of minimal signed number by minus one", ErrorCodes::ILLEGAL_DIVISION);
}

#pragma GCC diagnostic pop


template<typename A, typename B>
struct DivideIntegralImpl
{
	typedef typename NumberTraits::ResultOfIntegerDivision<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		throwIfDivisionLeadsToFPE(a, b);
		return static_cast<Result>(a) / b;
	}
};

template<typename A, typename B>
struct ModuloImpl
{
	typedef typename NumberTraits::ResultOfModulo<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<A>::Type(b));
		return typename NumberTraits::ToInteger<A>::Type(a)
			% typename NumberTraits::ToInteger<A>::Type(b);
	}
};

template<typename A, typename B>
struct BitAndImpl
{
	typedef typename NumberTraits::ResultOfBit<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		return static_cast<Result>(a)
			& static_cast<Result>(b);
	}
};

template<typename A, typename B>
struct BitOrImpl
{
	typedef typename NumberTraits::ResultOfBit<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		return static_cast<Result>(a)
			| static_cast<Result>(b);
	}
};

template<typename A, typename B>
struct BitXorImpl
{
	typedef typename NumberTraits::ResultOfBit<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		return static_cast<Result>(a)
			^ static_cast<Result>(b);
	}
};

template<typename A, typename B>
struct BitShiftLeftImpl
{
	typedef typename NumberTraits::ResultOfBit<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		return static_cast<Result>(a)
			<< static_cast<Result>(b);
	}
};

template<typename A, typename B>
struct BitShiftRightImpl
{
	typedef typename NumberTraits::ResultOfBit<A, B>::Type ResultType;

	template <typename Result = ResultType>
	static inline Result apply(A a, B b)
	{
		return static_cast<Result>(a)
			>> static_cast<Result>(b);
	}
};

template<typename A>
struct NegateImpl
{
	typedef typename NumberTraits::ResultOfNegate<A>::Type ResultType;

	static inline ResultType apply(A a)
	{
		return -static_cast<ResultType>(a);
	}
};

template<typename A>
struct BitNotImpl
{
	typedef typename NumberTraits::ResultOfBitNot<A>::Type ResultType;

	static inline ResultType apply(A a)
	{
		return ~static_cast<ResultType>(a);
	}
};


/// this one is just for convenience
template <bool B, typename T1, typename T2> using If = typename std::conditional<B, T1, T2>::type;
/// these ones for better semantics
template <typename T> using Then = T;
template <typename T> using Else = T;

/// Used to indicate undefined operation
struct InvalidType { using FieldType = Int8; };

/// Used for overloading resolution
template <bool B> struct BoolToType {
	static constexpr auto value = B;
	explicit constexpr operator bool() const { return value; }
};

template <typename DataType> struct IsValid
{
	static constexpr BoolToType<!std::is_same<DataType, InvalidType>::value> value{};
};

using Valid = BoolToType<true>;
using Invalid = BoolToType<false>;

template <typename DataType> struct IsIntegral { static constexpr auto value = false; };
template <> struct IsIntegral<DataTypeUInt8> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeUInt16> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeUInt32> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeUInt64> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeInt8> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeInt16> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeInt32> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeInt64> { static constexpr auto value = true; };

template <typename DataType> struct IsDate { static constexpr auto value = false; };
template <> struct IsDate<DataTypeDate> { static constexpr auto value = true; };
template <> struct IsDate<DataTypeDateTime> { static constexpr auto value = true; };

/** Returns appropriate result type for binary operator on dates:
 *  Date + Integral -> Date
 *  Integral + Date -> Date
 *  Date - Date		-> Int32
 *  Date - Integral -> Date
 *  All other operations are not defined and return InvalidType, operations on
 *  distinct date types are also undefined (e.g. DataTypeDate - DataTypeDateTime) */
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct DateBinaryOperationTraits
{
	using T0 = typename LeftDataType::FieldType;
	using T1 = typename RightDataType::FieldType;
	using Op = Operation<T0, T1>;

	using ResultDataType =
		If<std::is_same<Op, PlusImpl<T0, T1>>::value,
			Then<
				If<IsDate<LeftDataType>::value && IsIntegral<RightDataType>::value,
					Then<LeftDataType>,
					Else<
						If<IsIntegral<LeftDataType>::value && IsDate<RightDataType>::value,
							Then<RightDataType>,
							Else<InvalidType>
						>
					>
				>
			>,
			Else<
				If<std::is_same<Op, MinusImpl<T0, T1>>::value,
					Then<
						If<IsDate<LeftDataType>::value,
							Then<
								If<std::is_same<LeftDataType, RightDataType>::value,
									Then<DataTypeInt32>,
									Else<
										If<IsIntegral<RightDataType>::value,
											Then<LeftDataType>,
											Else<InvalidType>
										>
									>
								>
							>,
							Else<InvalidType>
						>
					>,
					Else<InvalidType>
				>
			>
		>;
};


/// Decides among date and numeric operations
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct BinaryOperationTraits
{
	using ResultDataType =
		If<IsDate<LeftDataType>::value || IsDate<RightDataType>::value,
			Then<
				typename DateBinaryOperationTraits<
					Operation, LeftDataType, RightDataType
				>::ResultDataType
			>,
			Else<
				typename DataTypeFromFieldType<
					typename Operation<
						typename LeftDataType::FieldType,
						typename RightDataType::FieldType
					>::ResultType
				>::Type
			>
		>;
};


template <template <typename, typename> class Op, typename Name>
class FunctionBinaryArithmetic : public IFunction
{
private:
	template <typename>
	bool checkRightTypeImpl(DataTypePtr & type_res, Invalid) const
	{
		return false;
	}

	template <typename ResultDataType>
	bool checkRightTypeImpl(DataTypePtr & type_res, Valid) const
	{
		type_res = new ResultDataType;
		return true;
	}

	template <typename LeftDataType, typename RightDataType>
	bool checkRightType(const DataTypes & arguments, DataTypePtr & type_res) const
	{
		using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

		if (typeid_cast<const RightDataType *>(&*arguments[1]))
			return checkRightTypeImpl<ResultDataType>(type_res, IsValid<ResultDataType>::value);

		return false;
	}

	template <typename T0>
	bool checkLeftType(const DataTypes & arguments, DataTypePtr & type_res) const
	{
		if (typeid_cast<const T0 *>(&*arguments[0]))
		{
			if (	checkRightType<T0, DataTypeDate>(arguments, type_res)
				||  checkRightType<T0, DataTypeDateTime>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt8>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt16>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt32>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt64>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt8>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt16>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt32>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt64>(arguments, type_res)
				||	checkRightType<T0, DataTypeFloat32>(arguments, type_res)
				||	checkRightType<T0, DataTypeFloat64>(arguments, type_res))
				return true;
			else
				throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		return false;
	}

	template <typename T0, typename T1, typename ResultType = typename Op<T0, T1>::ResultType>
	bool executeRightType(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<T0> * col_left)
	{
		if (auto col_right = typeid_cast<ColumnVector<T1> *>(block.getByPosition(arguments[1]).column.get()))
		{
			auto col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			auto & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (auto col_right = typeid_cast<ColumnConst<T1> *>(block.getByPosition(arguments[1]).column.get()))
		{
			auto col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			auto & vec_res = col_res->getData();
			vec_res.resize(col_left->getData().size());
			BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::vector_constant(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}

		return false;
	}

	template <typename T0, typename T1, typename ResultType = typename Op<T0, T1>::ResultType>
	bool executeConstRightType(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnConst<T0> * col_left)
	{
		if (auto col_right = typeid_cast<ColumnVector<T1> *>(block.getByPosition(arguments[1]).column.get()))
		{
			auto col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			auto & vec_res = col_res->getData();
			vec_res.resize(col_left->size());
			BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::constant_vector(col_left->getData(), col_right->getData(), vec_res);

			return true;
		}
		else if (auto col_right = typeid_cast<ColumnConst<T1> *>(block.getByPosition(arguments[1]).column.get()))
		{
			ResultType res = 0;
			BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::constant_constant(col_left->getData(), col_right->getData(), res);

			auto col_res = new ColumnConst<ResultType>(col_left->size(), res);
			block.getByPosition(result).column = col_res;

			return true;
		}

		return false;
	}

	template <typename T0>
	bool executeLeftType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		using DataType = typename DataTypeFromFieldType<T0>::Type;

		auto & col = block.getByPosition(arguments[0]);

		if (auto col_left = typeid_cast<ColumnVector<T0> *>(col.column.get()))
		{
			if (	executeRightTypeDate<DataType, DataTypeDate>(block, arguments, result, col_left)
				||  executeRightTypeDate<DataType, DataTypeDateTime>(block, arguments, result, col_left)
				||	executeRightType<T0, UInt8>(block, arguments, result, col_left)
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
		else if (auto col_left = typeid_cast<ColumnConst<T0> *>(col.column.get()))
		{
			if (	executeConstRightTypeDate<DataType, DataTypeDate>(block, arguments, result, col_left)
				||  executeConstRightTypeDate<DataType, DataTypeDateTime>(block, arguments, result, col_left)
				||	executeConstRightType<T0, UInt8>(block, arguments, result, col_left)
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

	template <typename LeftDataType, typename RightDataType, typename ResultDataType>
	bool executeRightTypeDateImpl(Block & block, const ColumnNumbers & arguments, const size_t result,
								  const ColumnVector<typename LeftDataType::FieldType> * col_left,
								  Invalid)
	{
		return false;
	}

	template <typename LeftDataType, typename RightDataType, typename ResultDataType>
	bool executeRightTypeDateImpl(Block & block, const ColumnNumbers & arguments, const size_t result,
								  const ColumnVector<typename LeftDataType::FieldType> * col_left,
								  Valid)
	{
		using T0 = typename LeftDataType::FieldType;
		using T1 = typename RightDataType::FieldType;
		using ResultType = typename ResultDataType::FieldType;

		return executeRightType<T0, T1, ResultType>(block, arguments, result, col_left);
	}

	template <typename LeftDataType, typename RightDataType>
	bool executeRightTypeDate(Block & block, const ColumnNumbers & arguments, size_t result,
							  const ColumnVector<typename LeftDataType::FieldType> * col_left)
	{
		if (!typeid_cast<const RightDataType *>(block.getByPosition(arguments[1]).type.get()))
			return false;

		using ResultDataType = typename DateBinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

		return executeRightTypeDateImpl<LeftDataType, RightDataType, ResultDataType>(
			block, arguments, result, col_left, IsValid<ResultDataType>::value);
	}

	template <typename LeftDataType, typename RightDataType, typename ResultDataType>
	bool executeConstRightTypeDateImpl(Block & block, const ColumnNumbers & arguments, const size_t result,
									   const ColumnConst<typename LeftDataType::FieldType> * col_left,
									   Invalid)
	{
		return false;
	}

	template <typename LeftDataType, typename RightDataType, typename ResultDataType>
	bool executeConstRightTypeDateImpl(Block & block, const ColumnNumbers & arguments, const size_t result,
									   const ColumnConst<typename LeftDataType::FieldType> * col_left,
									   Valid)
	{
		using T0 = typename LeftDataType::FieldType;
		using T1 = typename RightDataType::FieldType;
		using ResultType = typename ResultDataType::FieldType;

		return executeConstRightType<T0, T1, ResultType>(block, arguments, result, col_left);
	}

	template <typename LeftDataType, typename RightDataType>
	bool executeConstRightTypeDate(Block & block, const ColumnNumbers & arguments, size_t result,
							   const ColumnConst<typename LeftDataType::FieldType> * col_left)
	{
		if (!typeid_cast<const RightDataType *>(block.getByPosition(arguments[1]).type.get()))
			return false;

		using ResultDataType = typename DateBinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

		return executeConstRightTypeDateImpl<LeftDataType, RightDataType, ResultDataType>(
			block, arguments, result, col_left, IsValid<ResultDataType>::value);
	}

	template <typename DataType>
	bool executeLeftTypeDate(Block & block, const ColumnNumbers & arguments, const size_t result)
	{
		auto & col = block.getByPosition(arguments[0]);

		if (!typeid_cast<const DataType *>(col.type.get()))
			return false;

		using T0 = typename DataType::FieldType;

		if (auto col_left = typeid_cast<ColumnVector<T0> *>(col.column.get()))
		{
			if (	executeRightTypeDate<DataType, DataTypeDate>(block, arguments, result, col_left)
				||  executeRightTypeDate<DataType, DataTypeDateTime>(block, arguments, result, col_left)
				||  executeRightTypeDate<DataType, DataTypeUInt8>(block, arguments, result, col_left)
				||	executeRightTypeDate<DataType, DataTypeUInt16>(block, arguments, result, col_left)
				||	executeRightTypeDate<DataType, DataTypeUInt32>(block, arguments, result, col_left)
				||	executeRightTypeDate<DataType, DataTypeUInt64>(block, arguments, result, col_left)
				||	executeRightTypeDate<DataType, DataTypeInt8>(block, arguments, result, col_left)
				||	executeRightTypeDate<DataType, DataTypeInt16>(block, arguments, result, col_left)
				||	executeRightTypeDate<DataType, DataTypeInt32>(block, arguments, result, col_left)
				||	executeRightTypeDate<DataType, DataTypeInt64>(block, arguments, result, col_left))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
					+ " of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (auto col_left = typeid_cast<ColumnConst<T0> *>(col.column.get()))
		{
			if (	executeConstRightTypeDate<DataType, DataTypeDate>(block, arguments, result, col_left)
				||  executeConstRightTypeDate<DataType, DataTypeDateTime>(block, arguments, result, col_left)
				||	executeConstRightTypeDate<DataType, DataTypeUInt8>(block, arguments, result, col_left)
				||	executeConstRightTypeDate<DataType, DataTypeUInt16>(block, arguments, result, col_left)
				||	executeConstRightTypeDate<DataType, DataTypeUInt32>(block, arguments, result, col_left)
				||	executeConstRightTypeDate<DataType, DataTypeUInt64>(block, arguments, result, col_left)
				||	executeConstRightTypeDate<DataType, DataTypeInt8>(block, arguments, result, col_left)
				||	executeConstRightTypeDate<DataType, DataTypeInt16>(block, arguments, result, col_left)
				||	executeConstRightTypeDate<DataType, DataTypeInt32>(block, arguments, result, col_left)
				||	executeConstRightTypeDate<DataType, DataTypeInt64>(block, arguments, result, col_left))
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

		DataTypePtr type_res;

		if (!(	checkLeftType<DataTypeDate>(arguments, type_res)
			||	checkLeftType<DataTypeDateTime>(arguments, type_res)
			||	checkLeftType<DataTypeUInt8>(arguments, type_res)
			||	checkLeftType<DataTypeUInt16>(arguments, type_res)
			||	checkLeftType<DataTypeUInt32>(arguments, type_res)
			||	checkLeftType<DataTypeUInt64>(arguments, type_res)
			||	checkLeftType<DataTypeInt8>(arguments, type_res)
			||	checkLeftType<DataTypeInt16>(arguments, type_res)
			||	checkLeftType<DataTypeInt32>(arguments, type_res)
			||	checkLeftType<DataTypeInt64>(arguments, type_res)
			||	checkLeftType<DataTypeFloat32>(arguments, type_res)
			||	checkLeftType<DataTypeFloat64>(arguments, type_res)))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return type_res;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (!(  executeLeftTypeDate<DataTypeDate>(block, arguments, result)
			||  executeLeftTypeDate<DataTypeDateTime>(block, arguments, result)
			||  executeLeftType<UInt8>(block, arguments, result)
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


template <template <typename> class Op, typename Name>
class FunctionUnaryArithmetic : public IFunction
{
private:
	template <typename T0>
	bool checkType(const DataTypes & arguments, DataTypePtr & result) const
	{
		if (typeid_cast<const T0 *>(&*arguments[0]))
		{
			result = new typename DataTypeFromFieldType<
				typename Op<typename T0::FieldType>::ResultType>::Type;
			return true;
		}
		return false;
	}

	template <typename T0>
	bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (ColumnVector<T0> * col = typeid_cast<ColumnVector<T0> *>(&*block.getByPosition(arguments[0]).column))
		{
			typedef typename Op<T0>::ResultType ResultType;

			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->getData().size());
			UnaryOperationImpl<T0, Op<T0> >::vector(col->getData(), vec_res);

			return true;
		}
		else if (ColumnConst<T0> * col = typeid_cast<ColumnConst<T0> *>(&*block.getByPosition(arguments[0]).column))
		{
			typedef typename Op<T0>::ResultType ResultType;

			ResultType res = 0;
			UnaryOperationImpl<T0, Op<T0> >::constant(col->getData(), res);

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
				+ toString(arguments.size()) + ", should be 1.",
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
			||	checkType<DataTypeFloat64>(arguments, result)))
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
struct NameBitAnd			{ static const char * get() { return "bitAnd"; } };
struct NameBitOr			{ static const char * get() { return "bitOr"; } };
struct NameBitXor			{ static const char * get() { return "bitXor"; } };
struct NameBitNot			{ static const char * get() { return "bitNot"; } };
struct NameBitShiftLeft		{ static const char * get() { return "bitShiftLeft"; } };
struct NameBitShiftRight	{ static const char * get() { return "bitShiftRight"; } };

typedef FunctionBinaryArithmetic<PlusImpl,				NamePlus> 				FunctionPlus;
typedef FunctionBinaryArithmetic<MinusImpl, 			NameMinus> 				FunctionMinus;
typedef FunctionBinaryArithmetic<MultiplyImpl,			NameMultiply> 			FunctionMultiply;
typedef FunctionBinaryArithmetic<DivideFloatingImpl, 	NameDivideFloating>	 	FunctionDivideFloating;
typedef FunctionBinaryArithmetic<DivideIntegralImpl, 	NameDivideIntegral> 	FunctionDivideIntegral;
typedef FunctionBinaryArithmetic<ModuloImpl, 			NameModulo> 			FunctionModulo;
typedef FunctionUnaryArithmetic<NegateImpl, 			NameNegate> 			FunctionNegate;
typedef FunctionBinaryArithmetic<BitAndImpl,			NameBitAnd> 			FunctionBitAnd;
typedef FunctionBinaryArithmetic<BitOrImpl,				NameBitOr> 				FunctionBitOr;
typedef FunctionBinaryArithmetic<BitXorImpl,			NameBitXor> 			FunctionBitXor;
typedef FunctionUnaryArithmetic<BitNotImpl,				NameBitNot> 			FunctionBitNot;
typedef FunctionBinaryArithmetic<BitShiftLeftImpl,		NameBitShiftLeft> 		FunctionBitShiftLeft;
typedef FunctionBinaryArithmetic<BitShiftRightImpl,		NameBitShiftRight> 		FunctionBitShiftRight;



/// Оптимизации для целочисленного деления на константу.

#define LIBDIVIDE_USE_SSE2 1
#include <libdivide.h>


template <typename A, typename B>
struct DivideIntegralByConstantImpl
	: BinaryOperationImplBase<A, B, DivideIntegralImpl<A, B>>
{
	typedef typename DivideIntegralImpl<A, B>::ResultType ResultType;

	static void vector_constant(const PODArray<A> & a, B b, PODArray<ResultType> & c)
	{
		if (unlikely(b == 0))
			throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

		if (unlikely(std::is_signed<B>::value && b == -1))
		{
			size_t size = a.size();
			for (size_t i = 0; i < size; ++i)
				c[i] = -c[i];
			return;
		}

#pragma GCC diagnostic pop

		libdivide::divider<A> divider(b);

		size_t size = a.size();
		const A * a_pos = &a[0];
		const A * a_end = a_pos + size;
		ResultType * c_pos = &c[0];
		static constexpr size_t values_per_sse_register = 16 / sizeof(A);
		const A * a_end_sse = a_pos + size / values_per_sse_register * values_per_sse_register;

		while (a_pos < a_end_sse)
		{
			_mm_storeu_si128(reinterpret_cast<__m128i *>(c_pos),
				_mm_loadu_si128(reinterpret_cast<const __m128i *>(a_pos)) / divider);

			a_pos += values_per_sse_register;
			c_pos += values_per_sse_register;
		}

		while (a_pos < a_end)
		{
			*c_pos = *a_pos / divider;
			++a_pos;
			++c_pos;
		}
	}
};

template <typename A, typename B>
struct ModuloByConstantImpl
	: BinaryOperationImplBase<A, B, ModuloImpl<A, B>>
{
	typedef typename ModuloImpl<A, B>::ResultType ResultType;

	static void vector_constant(const PODArray<A> & a, B b, PODArray<ResultType> & c)
	{
		if (unlikely(b == 0))
			throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

		if (unlikely((std::is_signed<B>::value && b == -1) || b == 1))
		{
			size_t size = a.size();
			for (size_t i = 0; i < size; ++i)
				c[i] = 0;
			return;
		}

#pragma GCC diagnostic pop

		libdivide::divider<A> divider(b);

		/// Тут не удалось сделать так, чтобы SSE вариант из libdivide давал преимущество.
		size_t size = a.size();
		for (size_t i = 0; i < size; ++i)
			c[i] = a[i] - (a[i] / divider) * b;	/// NOTE: возможно, не сохраняется семантика деления с остатком отрицательных чисел.
	}
};


/** Прописаны специализации для деления чисел типа UInt64 и UInt32 на числа той же знаковости.
  * Можно дополнить до всех возможных комбинаций, но потребуется больше кода.
  */

template <> struct BinaryOperationImpl<UInt64, UInt8, 	DivideIntegralImpl<UInt64, UInt8>> 	: DivideIntegralByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16,	DivideIntegralImpl<UInt64, UInt16>> : DivideIntegralByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, 	DivideIntegralImpl<UInt64, UInt32>> : DivideIntegralByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, 	DivideIntegralImpl<UInt64, UInt64>> : DivideIntegralByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, 	DivideIntegralImpl<UInt32, UInt8>> 	: DivideIntegralByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, 	DivideIntegralImpl<UInt32, UInt16>> : DivideIntegralByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, 	DivideIntegralImpl<UInt32, UInt32>> : DivideIntegralByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, 	DivideIntegralImpl<UInt32, UInt64>> : DivideIntegralByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, 	DivideIntegralImpl<Int64, Int8>> 	: DivideIntegralByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, 	DivideIntegralImpl<Int64, Int16>> 	: DivideIntegralByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, 	DivideIntegralImpl<Int64, Int32>> 	: DivideIntegralByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, 	DivideIntegralImpl<Int64, Int64>> 	: DivideIntegralByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, 	DivideIntegralImpl<Int32, Int8>> 	: DivideIntegralByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, 	DivideIntegralImpl<Int32, Int16>> 	: DivideIntegralByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, 	DivideIntegralImpl<Int32, Int32>> 	: DivideIntegralByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, 	DivideIntegralImpl<Int32, Int64>> 	: DivideIntegralByConstantImpl<Int32, Int64> {};


template <> struct BinaryOperationImpl<UInt64, UInt8, 	ModuloImpl<UInt64, UInt8>> 	: ModuloByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16,	ModuloImpl<UInt64, UInt16>> : ModuloByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, 	ModuloImpl<UInt64, UInt32>> : ModuloByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, 	ModuloImpl<UInt64, UInt64>> : ModuloByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, 	ModuloImpl<UInt32, UInt8>> 	: ModuloByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, 	ModuloImpl<UInt32, UInt16>> : ModuloByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, 	ModuloImpl<UInt32, UInt32>> : ModuloByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, 	ModuloImpl<UInt32, UInt64>> : ModuloByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, 	ModuloImpl<Int64, Int8>> 	: ModuloByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, 	ModuloImpl<Int64, Int16>> 	: ModuloByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, 	ModuloImpl<Int64, Int32>> 	: ModuloByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, 	ModuloImpl<Int64, Int64>> 	: ModuloByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, 	ModuloImpl<Int32, Int8>> 	: ModuloByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, 	ModuloImpl<Int32, Int16>> 	: ModuloByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, 	ModuloImpl<Int32, Int32>> 	: ModuloByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, 	ModuloImpl<Int32, Int64>> 	: ModuloByConstantImpl<Int32, Int64> {};

}
