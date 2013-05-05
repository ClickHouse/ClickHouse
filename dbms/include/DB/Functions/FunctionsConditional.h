#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функция выбора по условию: if(cond, then, else).
  * cond - UInt8
  * then, else - одинакового типа - либо числа/даты/даты-с-временем, либо строки.
  */


template <typename T>
struct NumIfImpl
{
	static void vector_vector(
		const std::vector<UInt8> & cond,
		const std::vector<T> & a, const std::vector<T> & b,
		std::vector<T> & res)
	{
		size_t size = cond.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? a[i] : b[i];
	}

	static void vector_constant(
		const std::vector<UInt8> & cond,
		const std::vector<T> & a, T b,
		std::vector<T> & res)
	{
		size_t size = cond.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? a[i] : b;
	}

	static void constant_vector(
		const std::vector<UInt8> & cond,
		T a, const std::vector<T> & b,
		std::vector<T> & res)
	{
		size_t size = cond.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? a : b[i];
	}

	static void constant_constant(
		const std::vector<UInt8> & cond,
		T a, T b,
		std::vector<T> & res)
	{
		size_t size = cond.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? a : b;
	}
};


struct StringIfImpl
{
	static void vector_vector(
		const std::vector<UInt8> & cond,
		const std::vector<UInt8> & a_data, const ColumnString::Offsets_t & a_offsets,
		const std::vector<UInt8> & b_data, const ColumnString::Offsets_t & b_offsets,
		std::vector<UInt8> & c_data, ColumnString::Offsets_t & c_offsets)
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
		const std::vector<UInt8> & cond,
		const std::vector<UInt8> & a_data, const ColumnString::Offsets_t & a_offsets,
		const String & b,
		std::vector<UInt8> & c_data, ColumnString::Offsets_t & c_offsets)
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
		const std::vector<UInt8> & cond,
		const String & a,
		const std::vector<UInt8> & b_data, const ColumnString::Offsets_t & b_offsets,
		std::vector<UInt8> & c_data, ColumnString::Offsets_t & c_offsets)
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
		const std::vector<UInt8> & cond,
		const String & a, const String & b,
		std::vector<UInt8> & c_data, ColumnString::Offsets_t & c_offsets)
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


class FunctionIf : public IFunction
{
private:
	template <typename T>
	bool executeType(const ColumnVector<UInt8> * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnVector<T> * col_then_vec = dynamic_cast<ColumnVector<T> *>(&*block.getByPosition(arguments[1]).column);
		ColumnVector<T> * col_else_vec = dynamic_cast<ColumnVector<T> *>(&*block.getByPosition(arguments[2]).column);
		ColumnConst<T> * col_then_const = dynamic_cast<ColumnConst<T> *>(&*block.getByPosition(arguments[1]).column);
		ColumnConst<T> * col_else_const = dynamic_cast<ColumnConst<T> *>(&*block.getByPosition(arguments[2]).column);

		ColumnVector<T> * col_res = new ColumnVector<T>;
		block.getByPosition(result).column = col_res;

		typename ColumnVector<T>::Container_t & vec_res = col_res->getData();
		vec_res.resize(block.getByPosition(arguments[0]).column->size());

		if (col_then_vec && col_else_vec)
			NumIfImpl<T>::vector_vector(cond_col->getData(), col_then_vec->getData(), col_else_vec->getData(), vec_res);
		else if (col_then_vec && col_else_const)
			NumIfImpl<T>::vector_constant(cond_col->getData(), col_then_vec->getData(), col_else_const->getData(), vec_res);
		else if (col_then_const && col_else_vec)
			NumIfImpl<T>::constant_vector(cond_col->getData(), col_then_const->getData(), col_else_vec->getData(), vec_res);
		else if (col_then_const && col_else_const)
			NumIfImpl<T>::constant_constant(cond_col->getData(), col_then_const->getData(), col_else_const->getData(), vec_res);
		else
			return false;

		return true;
	}

	bool executeString(const ColumnVector<UInt8> * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnString * col_then = dynamic_cast<ColumnString *>(&*block.getByPosition(arguments[1]).column);
		ColumnString * col_else = dynamic_cast<ColumnString *>(&*block.getByPosition(arguments[2]).column);
		ColumnConstString * col_then_const = dynamic_cast<ColumnConstString *>(&*block.getByPosition(arguments[1]).column);
		ColumnConstString * col_else_const = dynamic_cast<ColumnConstString *>(&*block.getByPosition(arguments[2]).column);

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
		return "if";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 3)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 3.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeUInt8 *>(&*arguments[0]))
			throw Exception("Illegal type of first argument (condition) of function if. Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments[1]->getName() != arguments[2]->getName())
			throw Exception("Second and third arguments for function " + getName() + " must have same type. Passed: "
				+ arguments[1]->getName() + " and " + arguments[2]->getName() + ".",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arguments[1];
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnVector<UInt8> * cond_col = dynamic_cast<const ColumnVector<UInt8> *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConst<UInt8> * cond_const_col = dynamic_cast<const ColumnConst<UInt8> *>(&*block.getByPosition(arguments[0]).column);

		if (cond_const_col)
		{
			block.getByPosition(result).column = cond_const_col->getData()
				? block.getByPosition(arguments[1]).column
				: block.getByPosition(arguments[2]).column;
		}
		else if (cond_col)
		{
			if (!(	executeType<UInt8>(cond_col, block, arguments, result)
				||	executeType<UInt16>(cond_col, block, arguments, result)
				||	executeType<UInt32>(cond_col, block, arguments, result)
				||	executeType<UInt64>(cond_col, block, arguments, result)
				||	executeType<Int8>(cond_col, block, arguments, result)
				||	executeType<Int16>(cond_col, block, arguments, result)
				||	executeType<Int32>(cond_col, block, arguments, result)
				||	executeType<Int64>(cond_col, block, arguments, result)
				||	executeType<Float32>(cond_col, block, arguments, result)
				||	executeType<Float64>(cond_col, block, arguments, result)
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
