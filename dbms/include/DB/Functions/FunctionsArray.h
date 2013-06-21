#pragma once

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnString.h>

#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции по работе с массивами:
  *
  * array(с1, с2, ...) - создать массив из констант.
  * arrayElement(arr, i) - получить элемент массива.
  * has(arr, x) - есть ли в массиве элемент x.
  * indexOf(arr, x) - возвращает индекс элемента x (начиная с 1), если он есть в массиве, или 0, если его нет.
  */

class FunctionArray : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "array";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.empty())
			throw Exception("Function array requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		for (size_t i = 1, size = arguments.size(); i < size; ++i)
			if (arguments[i]->getName() != arguments[0]->getName())
				throw Exception("Arguments for function array must have same type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeArray(arguments[0]);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Все аргументы должны быть константами.
		for (size_t i = 0, size = arguments.size(); i < size; ++i)
			if (!block.getByPosition(arguments[i]).column->isConst())
				throw Exception("Arguments for function array must be constant.", ErrorCodes::ILLEGAL_COLUMN);;

		Array arr;
		for (size_t i = 0, size = arguments.size(); i < size; ++i)
			arr.push_back((*block.getByPosition(arguments[i]).column)[0]);

		block.getByPosition(result).column = new ColumnConstArray(block.getByPosition(arguments[0]).column->size(), arr, new DataTypeArray(block.getByPosition(arguments[0]).type));
	}
};


template <typename T>
struct ArrayElementNumImpl
{
	static void vector(
		const std::vector<T> & data, const ColumnArray::Offsets_t & offsets,
		const ColumnArray::Offset_t index,	/// Передаётся индекс начиная с нуля, а не с единицы.
		std::vector<T> & result)
	{
		size_t size = offsets.size();
		result.resize(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t array_size = offsets[i] - current_offset;

			if (index < array_size)
				result[i] = data[current_offset + index];
			/// Иначе - ничего не делаем (оставим значение по-умолчанию, которое уже лежит в векторе).
				
			current_offset = offsets[i];
		}
	}
};

struct ArrayElementStringImpl
{
	static void vector(
		const std::vector<UInt8> & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
		const ColumnArray::Offset_t index,	/// Передаётся индекс начиная с нуля, а не с единицы.
		std::vector<UInt8> & result_data, ColumnArray::Offsets_t & result_offsets)
	{
		size_t size = offsets.size();
		result_offsets.resize(size);
		result_data.reserve(data.size());

		ColumnArray::Offset_t current_offset = 0;
		ColumnArray::Offset_t current_result_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t array_size = offsets[i] - current_offset;

			if (index < array_size)
			{
				ColumnArray::Offset_t string_pos = current_offset == 0 && index == 0
					? 0
					: string_offsets[current_offset + index - 1];

				ColumnArray::Offset_t string_size = string_offsets[current_offset + index] - string_pos;
				
				result_data.resize(current_result_offset + string_size);
				memcpy(&result_data[current_result_offset], &data[string_pos], string_size);
				current_result_offset += string_size;
				result_offsets[i] = current_result_offset;
			}
			else
			{
				/// Вставим пустую строку.
				result_data.resize(current_result_offset + 1);
				current_result_offset += 1;
				result_offsets[i] = current_result_offset;
			}

			current_offset = offsets[i];
		}
	}
};


class FunctionArrayElement : public IFunction
{
private:
	template <typename T>
	bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result, UInt64 index)
	{
		const ColumnArray * col_array = dynamic_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnVector<T> * col_nested = dynamic_cast<const ColumnVector<T> *>(&col_array->getData());

		if (!col_nested)
			return false;
		
		ColumnVector<T> * col_res = new ColumnVector<T>;
		block.getByPosition(result).column = col_res;

		ArrayElementNumImpl<T>::vector(col_nested->getData(), col_array->getOffsets(), index, col_res->getData());

		return true;
	}

	bool executeString(Block & block, const ColumnNumbers & arguments, size_t result, UInt64 index)
	{
		const ColumnArray * col_array = dynamic_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnString * col_nested = dynamic_cast<const ColumnString *>(&col_array->getData());

		if (!col_nested)
			return false;

		ColumnString * col_res = new ColumnString;
		block.getByPosition(result).column = col_res;

		ArrayElementStringImpl::vector(
			col_nested->getChars(),
			col_array->getOffsets(),  
			col_nested->getOffsets(),
			index,
			col_res->getChars(),
			col_res->getOffsets());

		return true;
	}

	bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result, UInt64 index)
	{
		const ColumnConstArray * col_array = dynamic_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		Field value = col_array->getData().at(index);

		block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(
			block.rowsInFirstColumn(),
			value);

		return true;
	}

public:
	/// Получить имя функции.
	String getName() const
	{
		return "arrayElement";
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = dynamic_cast<const DataTypeArray *>(&*arguments[0]);
		if (!array_type)
			throw Exception("First argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!arguments[1]->isNumeric() || 0 != arguments[1]->getName().compare(0, 4, "UInt"))
			throw Exception("Second argument for function " + getName() + " must have UInt type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return array_type->getNestedType();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (!block.getByPosition(arguments[1]).column->isConst())
			throw Exception("Second argument for function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

		UInt64 index = safeGet<UInt64>((*block.getByPosition(arguments[1]).column)[0]);

		if (index == 0)
			throw Exception("Array indices is 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
		index -= 1;
		
		if (!(	executeNumber<UInt8>	(block, arguments, result, index)
			||	executeNumber<UInt16>	(block, arguments, result, index)
			||	executeNumber<UInt32>	(block, arguments, result, index)
			||	executeNumber<UInt64>	(block, arguments, result, index)
			||	executeNumber<Int8>		(block, arguments, result, index)
			||	executeNumber<Int16>	(block, arguments, result, index)
			||	executeNumber<Int32>	(block, arguments, result, index)
			||	executeNumber<Int64>	(block, arguments, result, index)
			||	executeNumber<Float32>	(block, arguments, result, index)
			||	executeNumber<Float64>	(block, arguments, result, index)
			||	executeConst			(block, arguments, result, index)
			||	executeString			(block, arguments, result, index)))
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Для has.
struct IndexToOne
{
	typedef UInt8 ResultType;
	static inline bool apply(size_t j, ResultType & current) { current = 1; return false; }
};

/// Для indexOf.
struct IndexIdentity
{
	typedef UInt64 ResultType;
	/// Индекс возвращается начиная с единицы.
	static inline bool apply(size_t j, ResultType & current) { current = j + 1; return false; }
};

/// Для countEqual.
struct IndexCount
{
	typedef UInt32 ResultType;
	static inline bool apply(size_t j, ResultType & current) { ++current; return true; }
};


template <typename T, typename IndexConv>
struct ArrayIndexNumImpl
{
	static void vector(
		const std::vector<T> & data, const ColumnArray::Offsets_t & offsets,
		const T value,
		std::vector<typename IndexConv::ResultType> & result)
	{
		size_t size = offsets.size();
		result.resize(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t array_size = offsets[i] - current_offset;
			typename IndexConv::ResultType current = 0;
			
			for (size_t j = 0; j < array_size; ++j)
			{
				if (data[current_offset + j] == value)
				{
					if (!IndexConv::apply(j, current))
						break;
				}
			}
			
			result[i] = current;
			current_offset = offsets[i];
		}
	}
};

template <typename IndexConv>
struct ArrayIndexStringImpl
{
	static void vector(
		const std::vector<UInt8> & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
		const String & value,
		std::vector<typename IndexConv::ResultType> & result)
	{
		size_t size = offsets.size();
		size_t value_size = value.size();
		result.resize(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t array_size = offsets[i] - current_offset;
			typename IndexConv::ResultType current = 0;

			for (size_t j = 0; j < array_size; ++j)
			{
				ColumnArray::Offset_t string_pos = current_offset == 0 && j == 0
					? 0
					: string_offsets[current_offset + j - 1];

				ColumnArray::Offset_t string_size = string_offsets[current_offset + j] - string_pos;

				if (string_size == value_size + 1 && 0 == memcmp(value.data(), &data[string_pos], value_size))
				{
					if (!IndexConv::apply(j, current))
						break;
				}
			}

			result[i] = current;
			current_offset = offsets[i];
		}
	}
};


template <typename IndexConv, typename Name>
class FunctionArrayIndex : public IFunction
{
private:
	typedef ColumnVector<typename IndexConv::ResultType> ResultColumnType;
	
	template <typename T>
	bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result, const Field & value)
	{
		const ColumnArray * col_array = dynamic_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnVector<T> * col_nested = dynamic_cast<const ColumnVector<T> *>(&col_array->getData());

		if (!col_nested)
			return false;

		ResultColumnType * col_res = new ResultColumnType;
		block.getByPosition(result).column = col_res;

		ArrayIndexNumImpl<T, IndexConv>::vector(
			col_nested->getData(),
			col_array->getOffsets(),
			safeGet<typename NearestFieldType<T>::Type>(value),
			col_res->getData());

		return true;
	}

	bool executeString(Block & block, const ColumnNumbers & arguments, size_t result, const Field & value)
	{
		const ColumnArray * col_array = dynamic_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnString * col_nested = dynamic_cast<const ColumnString *>(&col_array->getData());

		if (!col_nested)
			return false;

		ResultColumnType * col_res = new ResultColumnType;
		block.getByPosition(result).column = col_res;

		ArrayIndexStringImpl<IndexConv>::vector(
			col_nested->getChars(),
			col_array->getOffsets(),
			col_nested->getOffsets(),
			safeGet<const String &>(value),
			col_res->getData());

		return true;
	}

	bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & value)
	{
		const ColumnConstArray * col_array = dynamic_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const Array & arr = col_array->getData();

		size_t i = 0;
		size_t size = arr.size();
		typename IndexConv::ResultType current = 0;
		
		for (; i < size; ++i)
		{
			if (arr[i] == value)
			{
				if (!IndexConv::apply(i, current))
					break;
			}
		}

		block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(
			block.rowsInFirstColumn(),
			static_cast<typename NearestFieldType<typename IndexConv::ResultType>::Type>(current));

		return true;
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

		const DataTypeArray * array_type = dynamic_cast<const DataTypeArray *>(&*arguments[0]);
		if (!array_type)
			throw Exception("First argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (array_type->getNestedType()->getName() != arguments[1]->getName())
			throw Exception("Type of array elements and second argument for function " + getName() + " must be same."
				" Passed: " + arguments[0]->getName() + " and " + arguments[1]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new typename DataTypeFromFieldType<typename IndexConv::ResultType>::Type;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (!block.getByPosition(arguments[1]).column->isConst())
			throw Exception("Second argument for function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

		Field value = (*block.getByPosition(arguments[1]).column)[0];

		if (!(	executeNumber<UInt8>	(block, arguments, result, value)
			||	executeNumber<UInt16>	(block, arguments, result, value)
			||	executeNumber<UInt32>	(block, arguments, result, value)
			||	executeNumber<UInt64>	(block, arguments, result, value)
			||	executeNumber<Int8>		(block, arguments, result, value)
			||	executeNumber<Int16>	(block, arguments, result, value)
			||	executeNumber<Int32>	(block, arguments, result, value)
			||	executeNumber<Int64>	(block, arguments, result, value)
			||	executeNumber<Float32>	(block, arguments, result, value)
			||	executeNumber<Float64>	(block, arguments, result, value)
			||	executeConst			(block, arguments, result, value)
			||	executeString			(block, arguments, result, value)))
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameHas			{ static const char * get() { return "has"; } };
struct NameIndexOf		{ static const char * get() { return "indexOf"; } };
struct NameCountEqual	{ static const char * get() { return "countEqual"; } };

typedef FunctionArrayIndex<IndexToOne, 		NameHas>	FunctionHas;
typedef FunctionArrayIndex<IndexIdentity, 	NameIndexOf>	FunctionIndexOf;
typedef FunctionArrayIndex<IndexCount, 	NameCountEqual>	FunctionCountEqual;


}
