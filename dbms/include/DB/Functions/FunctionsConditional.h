#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/NumberTraits.h>


namespace DB
{

/** Функция выбора по условию: if(cond, then, else).
  * cond - UInt8
  * then, else - либо числа/даты/даты-с-временем, либо строки.
  */


template <typename A, typename B, typename ResultType>
struct NumIfImpl
{
private:
	static PODArray<ResultType> & result_vector(Block & block, size_t result, size_t size)
	{
		ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
		block.getByPosition(result).column = col_res;

		typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
		vec_res.resize(size);

		return vec_res;
	}
public:
	static void vector_vector(
		const PODArray<UInt8> & cond,
		const PODArray<A> & a, const PODArray<B> & b,
		Block & block,
		size_t result)
	{
		size_t size = cond.size();
		PODArray<ResultType> & res = result_vector(block, result, size);
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
	}

	static void vector_constant(
		const PODArray<UInt8> & cond,
		const PODArray<A> & a, B b,
		Block & block,
		size_t result)
	{
		size_t size = cond.size();
		PODArray<ResultType> & res = result_vector(block, result, size);
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
	}

	static void constant_vector(
		const PODArray<UInt8> & cond,
		A a, const PODArray<B> & b,
		Block & block,
		size_t result)
	{
		size_t size = cond.size();
		PODArray<ResultType> & res = result_vector(block, result, size);
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
	}

	static void constant_constant(
		const PODArray<UInt8> & cond,
		A a, B b,
		Block & block,
		size_t result)
	{
		size_t size = cond.size();
		PODArray<ResultType> & res = result_vector(block, result, size);
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
	}
};

template <typename A, typename B>
struct NumIfImpl<A, B, NumberTraits::Error>
{
private:
	static void throw_error()
	{
		throw Exception("Internal logic error: invalid types of arguments 2 and 3 of if", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
public:
	static void vector_vector(
		const PODArray<UInt8> & cond,
		const PODArray<A> & a, const PODArray<B> & b,
		Block & block,
		size_t result)
	{
		throw_error();
	}

	static void vector_constant(
		const PODArray<UInt8> & cond,
		const PODArray<A> & a, B b,
		Block & block,
		size_t result)
	{
		throw_error();
	}

	static void constant_vector(
		const PODArray<UInt8> & cond,
		A a, const PODArray<B> & b,
		Block & block,
		size_t result)
	{
		throw_error();
	}

	static void constant_constant(
		const PODArray<UInt8> & cond,
		A a, B b,
		Block & block,
		size_t result)
	{
		throw_error();
	}
};


struct StringIfImpl
{
	static void vector_vector(
		const PODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve(std::max(a_data.size(), b_data.size()));

		ColumnString::Offset_t a_prev_offset = 0;
		ColumnString::Offset_t b_prev_offset = 0;
		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
			{
				size_t size_to_write = a_offsets[i] - a_prev_offset;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], &a_data[a_prev_offset], size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b_offsets[i] - b_prev_offset;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], &b_data[b_prev_offset], size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}

			a_prev_offset = a_offsets[i];
			b_prev_offset = b_offsets[i];
		}
	}

	static void vector_constant(
		const PODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const String & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve(a_data.size());

		ColumnString::Offset_t a_prev_offset = 0;
		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
			{
				size_t size_to_write = a_offsets[i] - a_prev_offset;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], &a_data[a_prev_offset], size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b.size() + 1;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], b.data(), size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}

			a_prev_offset = a_offsets[i];
		}
	}

	static void constant_vector(
		const PODArray<UInt8> & cond,
		const String & a,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve(b_data.size());

		ColumnString::Offset_t b_prev_offset = 0;
		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
			{
				size_t size_to_write = a.size() + 1;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], a.data(), size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b_offsets[i] - b_prev_offset;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], &b_data[b_prev_offset], size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}

			b_prev_offset = b_offsets[i];
		}
	}

	static void constant_constant(
		const PODArray<UInt8> & cond,
		const String & a, const String & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve((std::max(a.size(), b.size()) + 1) * size);

		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
			{
				size_t size_to_write = a.size() + 1;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], a.data(), size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b.size() + 1;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], b.data(), size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
		}
	}
};


template <typename T>
struct DataTypeFromFieldTypeOrError
{
	static DataTypePtr getDataType()
	{
		return new typename DataTypeFromFieldType<T>::Type;
	}
};

template <>
struct DataTypeFromFieldTypeOrError<NumberTraits::Error>
{
	static DataTypePtr getDataType()
	{
		return nullptr;
	}
};


class FunctionIf : public IFunction
{
public:
	static constexpr auto name = "if";
	static IFunction * create(const Context & context) { return new FunctionIf; }

private:
	template <typename T0, typename T1>
	bool checkRightType(const DataTypes & arguments, DataTypePtr & type_res) const
	{
		if (typeid_cast<const T1 *>(&*arguments[2]))
		{
			typedef typename NumberTraits::ResultOfIf<typename T0::FieldType, typename T1::FieldType>::Type ResultType;
			type_res = DataTypeFromFieldTypeOrError<ResultType>::getDataType();
			if (!type_res)
				throw Exception("Arguments 2 and 3 of function " + getName() + " are not upscalable to a common type without loss of precision: "
					+ arguments[1]->getName() + " and " + arguments[2]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
			return true;
		}
		return false;
	}

	template <typename T0>
	bool checkLeftType(const DataTypes & arguments, DataTypePtr & type_res) const
	{
		if (typeid_cast<const T0 *>(&*arguments[1]))
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
				||	checkRightType<T0, DataTypeFloat64>(arguments, type_res))
				return true;
			else
				throw Exception("Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		return false;
	}

	template <typename T0, typename T1>
	bool executeRightType(
		const ColumnVector<UInt8> * cond_col,
		Block & block,
		const ColumnNumbers & arguments,
		size_t result,
		const ColumnVector<T0> * col_left)
	{
		ColumnVector<T1> * col_right_vec = typeid_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[2]).column);
		ColumnConst<T1> * col_right_const = typeid_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[2]).column);

		if (!col_right_vec && !col_right_const)
			return false;

		typedef typename NumberTraits::ResultOfIf<T0, T1>::Type ResultType;

		if (col_right_vec)
			NumIfImpl<T0, T1, ResultType>::vector_vector(cond_col->getData(), col_left->getData(), col_right_vec->getData(), block, result);
		else
			NumIfImpl<T0, T1, ResultType>::vector_constant(cond_col->getData(), col_left->getData(), col_right_const->getData(), block, result);

		return true;
	}

	template <typename T0, typename T1>
	bool executeConstRightType(
		const ColumnVector<UInt8> * cond_col,
		Block & block,
		const ColumnNumbers & arguments,
		size_t result,
		const ColumnConst<T0> * col_left)
	{
		ColumnVector<T1> * col_right_vec = typeid_cast<ColumnVector<T1> *>(&*block.getByPosition(arguments[2]).column);
		ColumnConst<T1> * col_right_const = typeid_cast<ColumnConst<T1> *>(&*block.getByPosition(arguments[2]).column);

		if (!col_right_vec && !col_right_const)
			return false;

		typedef typename NumberTraits::ResultOfIf<T0, T1>::Type ResultType;

		if (col_right_vec)
			NumIfImpl<T0, T1, ResultType>::constant_vector(cond_col->getData(), col_left->getData(), col_right_vec->getData(), block, result);
		else
			NumIfImpl<T0, T1, ResultType>::constant_constant(cond_col->getData(), col_left->getData(), col_right_const->getData(), block, result);

		return true;
	}

	template <typename T0>
	bool executeLeftType(const ColumnVector<UInt8> * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (ColumnVector<T0> * col_left = typeid_cast<ColumnVector<T0> *>(&*block.getByPosition(arguments[1]).column))
		{
			if (	executeRightType<T0, UInt8>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, UInt16>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, UInt32>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, UInt64>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Int8>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Int16>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Int32>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Int64>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Float32>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Float64>(cond_col, block, arguments, result, col_left))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
				+ " of third argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (ColumnConst<T0> * col_left = typeid_cast<ColumnConst<T0> *>(&*block.getByPosition(arguments[1]).column))
		{
			if (	executeConstRightType<T0, UInt8>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, UInt16>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, UInt32>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, UInt64>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, Int8>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, Int16>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, Int32>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, Int64>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, Float32>(cond_col, block, arguments, result, col_left)
				||	executeConstRightType<T0, Float64>(cond_col, block, arguments, result, col_left))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
					+ " of third argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}

		return false;
	}

	bool executeString(const ColumnVector<UInt8> * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnString * col_then = typeid_cast<ColumnString *>(&*block.getByPosition(arguments[1]).column);
		ColumnString * col_else = typeid_cast<ColumnString *>(&*block.getByPosition(arguments[2]).column);
		ColumnConstString * col_then_const = typeid_cast<ColumnConstString *>(&*block.getByPosition(arguments[1]).column);
		ColumnConstString * col_else_const = typeid_cast<ColumnConstString *>(&*block.getByPosition(arguments[2]).column);

		ColumnString * col_res = new ColumnString;
		block.getByPosition(result).column = col_res;

		ColumnString::Chars_t & res_vec = col_res->getChars();
		ColumnString::Offsets_t & res_offsets = col_res->getOffsets();

		if (col_then && col_else)
			StringIfImpl::vector_vector(
				cond_col->getData(),
										col_then->getChars(), col_then->getOffsets(),
										col_else->getChars(), col_else->getOffsets(),
										res_vec, res_offsets);
			else if (col_then && col_else_const)
				StringIfImpl::vector_constant(
					cond_col->getData(),
											  col_then->getChars(), col_then->getOffsets(),
											  col_else_const->getData(),
											  res_vec, res_offsets);
				else if (col_then_const && col_else)
					StringIfImpl::constant_vector(
						cond_col->getData(),
												  col_then_const->getData(),
												  col_else->getChars(), col_else->getOffsets(),
												  res_vec, res_offsets);
					else if (col_then_const && col_else_const)
						StringIfImpl::constant_constant(
							cond_col->getData(),
														col_then_const->getData(),
														col_else_const->getData(),
														res_vec, res_offsets);
						else
							return false;

						return true;
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
		if (arguments.size() != 3)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 3.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeUInt8 *>(&*arguments[0]))
			throw Exception("Illegal type of first argument (condition) of function if. Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments[1]->behavesAsNumber() && arguments[2]->behavesAsNumber())
		{
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
				||	checkLeftType<DataTypeFloat64>(arguments, type_res)))
				throw Exception("Internal error: unexpected type " + arguments[1]->getName() + " of first argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
			return type_res;
		}
		else if (arguments[1]->getName() != arguments[2]->getName())
		{
			throw Exception("Incompatible second and third arguments for function " + getName() + ": "
				+ arguments[1]->getName() + " and " + arguments[2]->getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}

		return arguments[1];
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnVector<UInt8> * cond_col = typeid_cast<const ColumnVector<UInt8> *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConst<UInt8> * cond_const_col = typeid_cast<const ColumnConst<UInt8> *>(&*block.getByPosition(arguments[0]).column);
		ColumnPtr materialized_cond_col;

		if (cond_const_col)
		{
			if (block.getByPosition(arguments[1]).type->getName() ==
				block.getByPosition(arguments[2]).type->getName())
			{
				block.getByPosition(result).column = cond_const_col->getData()
					? block.getByPosition(arguments[1]).column
					: block.getByPosition(arguments[2]).column;
				return;
			}
			else
			{
				materialized_cond_col = cond_const_col->convertToFullColumn();
				cond_col = typeid_cast<const ColumnVector<UInt8> *>(&*materialized_cond_col);
			}
		}
		if (cond_col)
		{
			if (!(	executeLeftType<UInt8>(cond_col, block, arguments, result)
				||	executeLeftType<UInt16>(cond_col, block, arguments, result)
				||	executeLeftType<UInt32>(cond_col, block, arguments, result)
				||	executeLeftType<UInt64>(cond_col, block, arguments, result)
				||	executeLeftType<Int8>(cond_col, block, arguments, result)
				||	executeLeftType<Int16>(cond_col, block, arguments, result)
				||	executeLeftType<Int32>(cond_col, block, arguments, result)
				||	executeLeftType<Int64>(cond_col, block, arguments, result)
				||	executeLeftType<Float32>(cond_col, block, arguments, result)
				||	executeLeftType<Float64>(cond_col, block, arguments, result)
				|| 	executeString(cond_col, block, arguments, result)))
				throw Exception("Illegal columns " + block.getByPosition(arguments[1]).column->getName()
					+ " and " + block.getByPosition(arguments[2]).column->getName()
					+ " of second (then) and third (else) arguments of function " + getName(),
								ErrorCodes::ILLEGAL_COLUMN);
		}
		else
			throw Exception("Illegal column " + cond_col->getName() + " of first argument of function " + getName() + ". Must be ColumnUInt8 or ColumnConstUInt8.",
							ErrorCodes::ILLEGAL_COLUMN);
	}
};

}
