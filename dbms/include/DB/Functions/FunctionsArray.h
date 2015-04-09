#pragma once

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnTuple.h>

#include <DB/Functions/IFunction.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/HashTable/ClearableHashMap.h>
#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/FunctionsConditional.h>

#include <statdaemons/ext/range.hpp>

#include <unordered_map>


namespace DB
{

/** Функции по работе с массивами:
  *
  * array(с1, с2, ...) - создать массив из констант.
  * arrayElement(arr, i) - получить элемент массива по индексу.
  *  Индекс начинается с 1. Также индекс может быть отрицательным - тогда он считается с конца массива.
  * has(arr, x) - есть ли в массиве элемент x.
  * indexOf(arr, x) - возвращает индекс элемента x (начиная с 1), если он есть в массиве, или 0, если его нет.
  * arrayEnumerate(arr) - возаращает массив [1,2,3,..., length(arr)]
  * arrayEnumerateUniq(arr) - возаращает массив,  параллельный данному, где для каждого элемента указано,
  * 						  какой он по счету среди элементов с таким значением.
  * 						  Например: arrayEnumerateUniq([10, 20, 10, 30]) = [1,  1,  2,  1]
  */




class FunctionArray : public IFunction
{
public:
	static constexpr auto name = "array";
	static IFunction * create(const Context & context) { return new FunctionArray; }

private:
	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	template <typename T0, typename T1>
	bool checkRightType(DataTypePtr left, DataTypePtr right, DataTypePtr & type_res) const
	{
		if (typeid_cast<const T1 *>(&*right))
		{
			typedef typename NumberTraits::ResultOfIf<typename T0::FieldType, typename T1::FieldType>::Type ResultType;
			type_res = DataTypeFromFieldTypeOrError<ResultType>::getDataType();
			if (!type_res)
				throw Exception("Arguments of function " + getName() + " are not upscalable to a common type without loss of precision.",
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
			return true;
		}
		return false;
	}

	template <typename T0>
	bool checkLeftType(DataTypePtr left, DataTypePtr right, DataTypePtr & type_res) const
	{
		if (typeid_cast<const T0 *>(&*left))
		{
			if (	checkRightType<T0, DataTypeUInt8>(left, right, type_res)
				||	checkRightType<T0, DataTypeUInt16>(left, right, type_res)
				||	checkRightType<T0, DataTypeUInt32>(left, right, type_res)
				||	checkRightType<T0, DataTypeUInt64>(left, right, type_res)
				||	checkRightType<T0, DataTypeInt8>(left, right, type_res)
				||	checkRightType<T0, DataTypeInt16>(left, right, type_res)
				||	checkRightType<T0, DataTypeInt32>(left, right, type_res)
				||	checkRightType<T0, DataTypeInt64>(left, right, type_res)
				||	checkRightType<T0, DataTypeFloat32>(left, right, type_res)
				||	checkRightType<T0, DataTypeFloat64>(left, right, type_res))
				return true;
			else
				throw Exception("Illegal type " + right->getName() + " as argument of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		return false;
	}

	template <typename T0, typename T1>
	bool tryAddField(DataTypePtr type_res, const Field & f, Array & arr) const
	{
		if (typeid_cast<const T0 *>(&*type_res))
		{
			arr.push_back(apply_visitor(FieldVisitorConvertToNumber<typename T1::FieldType>(), f));
			return true;
		}
		return false;
	}

	bool addField(DataTypePtr type_res, const Field & f, Array & arr) const
	{
		/// Иначе необходимо
		if (	tryAddField<DataTypeUInt8, DataTypeUInt64>(type_res, f, arr)
			||	tryAddField<DataTypeUInt16, DataTypeUInt64>(type_res, f, arr)
			||	tryAddField<DataTypeUInt32, DataTypeUInt64>(type_res, f, arr)
			||	tryAddField<DataTypeUInt64, DataTypeUInt64>(type_res, f, arr)
			||	tryAddField<DataTypeInt8, DataTypeInt64>(type_res, f, arr)
			||	tryAddField<DataTypeInt16, DataTypeInt64>(type_res, f, arr)
			||	tryAddField<DataTypeInt32, DataTypeInt64>(type_res, f, arr)
			||	tryAddField<DataTypeInt64, DataTypeInt64>(type_res, f, arr)
			||	tryAddField<DataTypeFloat32, DataTypeFloat64>(type_res, f, arr)
			||	tryAddField<DataTypeFloat64, DataTypeFloat64>(type_res, f, arr) )
			return true;
		else
			throw Exception("Illegal result type " + type_res->getName() + " of function " + getName(),
							ErrorCodes::LOGICAL_ERROR);
	}

	DataTypePtr getLeastCommonType(DataTypePtr left, DataTypePtr right) const
	{
		DataTypePtr type_res;
		if (!(	checkLeftType<DataTypeUInt8>(left, right, type_res)
			||	checkLeftType<DataTypeUInt16>(left, right, type_res)
			||	checkLeftType<DataTypeUInt32>(left, right, type_res)
			||	checkLeftType<DataTypeUInt64>(left, right, type_res)
			||	checkLeftType<DataTypeInt8>(left, right, type_res)
			||	checkLeftType<DataTypeInt16>(left, right, type_res)
			||	checkLeftType<DataTypeInt32>(left, right, type_res)
			||	checkLeftType<DataTypeInt64>(left, right, type_res)
			||	checkLeftType<DataTypeFloat32>(left, right, type_res)
			||	checkLeftType<DataTypeFloat64>(left, right, type_res)))
			throw Exception("Internal error: unexpected type " + left->getName() + " as argument of function " + getName(),
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		return type_res;
	}

public:
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.empty())
			throw Exception("Function array requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		DataTypePtr result_type = arguments[0];

		if (result_type->behavesAsNumber())
		{
			/// Если тип числовой, пробуем выделить наименьший общий тип
			for (size_t i = 1, size = arguments.size(); i < size; ++i)
				result_type = getLeastCommonType(result_type, arguments[i]);
		}
		else
		{
			/// Иначе все аргументы должны быть одинаковыми
			for (size_t i = 1, size = arguments.size(); i < size; ++i)
				if (arguments[i]->getName() != arguments[0]->getName())
					throw Exception("Arguments for function array must have same type or behave as number.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}

		return new DataTypeArray(result_type);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Все аргументы должны быть константами.
		for (size_t i = 0, size = arguments.size(); i < size; ++i)
			if (!block.getByPosition(arguments[i]).column->isConst())
				throw Exception("Arguments for function array must be constant.", ErrorCodes::ILLEGAL_COLUMN);

		DataTypePtr result_type = block.getByPosition(arguments[0]).type;
		if (result_type->behavesAsNumber())
		{
			/// Если тип числовой, вычисляем наименьший общий тип
			for (size_t i = 1, size = arguments.size(); i < size; ++i)
				result_type = getLeastCommonType(result_type, block.getByPosition(arguments[i]).type);
		}

		Array arr;
		for (size_t i = 0, size = arguments.size(); i < size; ++i)
			if (block.getByPosition(arguments[i]).type->getName() == result_type->getName())
				/// Если элемент такого же типа как результат, просто добавляем его в ответ
				arr.push_back((*block.getByPosition(arguments[i]).column)[0]);
			else
				/// Иначе необходимо привести его к типу результата
				addField(result_type, (*block.getByPosition(arguments[i]).column)[0], arr);

		block.getByPosition(result).column = new ColumnConstArray(block.getByPosition(arguments[0]).column->size(), arr, new DataTypeArray(result_type));
	}
};


template <typename T>
struct ArrayElementNumImpl
{
	/** Процедура для константного идекса
	  * Если negative = false - передаётся индекс с начала массива, начиная с нуля.
	  * Если negative = true - передаётся индекс с конца массива, начиная с нуля.
	  */
	template <bool negative>
	static void vectorConst(
		const PODArray<T> & data, const ColumnArray::Offsets_t & offsets,
		const ColumnArray::Offset_t index,
		PODArray<T> & result)
	{
		size_t size = offsets.size();
		result.resize(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t array_size = offsets[i] - current_offset;

			if (index < array_size)
				result[i] = !negative ? data[current_offset + index] : data[offsets[i] - index - 1];
			else
				result[i] = T();

			current_offset = offsets[i];
		}
	}

	/** Процедура для неконстантного идекса
	  * index_type - тип данных идекса
	  */
	template <typename index_type>
	static void vector(
		const PODArray<T> & data, const ColumnArray::Offsets_t & offsets,
		const ColumnVector<index_type> & index,
		PODArray<T> & result)
	{
		size_t size = offsets.size();
		result.resize(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t array_size = offsets[i] - current_offset;

			if (index[i].getType() == Field::Types::UInt64)
			{
				UInt64 cur_id = safeGet<UInt64>(index[i]);
				if (cur_id > 0 && cur_id <= array_size)
					result[i] = data[current_offset + cur_id - 1];
				else
					result[i] = T();
			}
			else if (index[i].getType() == Field::Types::Int64)
			{
				Int64 cur_id = safeGet<Int64>(index[i]);
				if (cur_id > 0 && static_cast<UInt64>(cur_id) <= array_size)
					result[i] = data[current_offset + cur_id - 1];
				else if (cur_id < 0 && static_cast<UInt64>(-cur_id) <= array_size)
					result[i] = data[offsets[i] + cur_id];
				else
					result[i] = T();
			}
			else
				throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

			current_offset = offsets[i];
		}
	}
};

struct ArrayElementStringImpl
{
	/** Процедура для константного идекса
	  * Если negative = false - передаётся индекс с начала массива, начиная с нуля.
	  * Если negative = true - передаётся индекс с конца массива, начиная с нуля.
	  */
	template <bool negative>
	static void vectorConst(
		const ColumnString::Chars_t & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
		const ColumnArray::Offset_t index,
		ColumnString::Chars_t & result_data, ColumnArray::Offsets_t & result_offsets)
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
				size_t adjusted_index = !negative ? index : (array_size - index - 1);

				ColumnArray::Offset_t string_pos = current_offset == 0 && adjusted_index == 0
					? 0
					: string_offsets[current_offset + adjusted_index - 1];

				ColumnArray::Offset_t string_size = string_offsets[current_offset + adjusted_index] - string_pos;

				result_data.resize(current_result_offset + string_size);
				memcpy(&result_data[current_result_offset], &data[string_pos], string_size);
				current_result_offset += string_size;
				result_offsets[i] = current_result_offset;
			}
			else
			{
				/// Вставим пустую строку.
				result_data.resize(current_result_offset + 1);
				result_data[current_result_offset] = 0;
				current_result_offset += 1;
				result_offsets[i] = current_result_offset;
			}

			current_offset = offsets[i];
		}
	}

	/** Процедура для неконстантного идекса
	  * index_type - тип данных идекса
	  */
	template <typename index_type>
	static void vector(
		const ColumnString::Chars_t & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
		const ColumnVector<index_type> & index,
		ColumnString::Chars_t & result_data, ColumnArray::Offsets_t & result_offsets)
	{
		size_t size = offsets.size();
		result_offsets.resize(size);
		result_data.reserve(data.size());

		ColumnArray::Offset_t current_offset = 0;
		ColumnArray::Offset_t current_result_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t array_size = offsets[i] - current_offset;
			size_t adjusted_index;

			if (index[i].getType() == Field::Types::UInt64)
			{
				UInt64 cur_id = safeGet<UInt64>(index[i]);
				if (cur_id > 0 && cur_id <= array_size)
					adjusted_index = cur_id - 1;
				else
					adjusted_index = array_size; /// Индекс не вписывается в рамки массива, заменяем заведомо слишком большим
			}
			else if (index[i].getType() == Field::Types::Int64)
			{
				Int64 cur_id = safeGet<Int64>(index[i]);
				if (cur_id > 0 && static_cast<UInt64>(cur_id) <= array_size)
					adjusted_index = cur_id - 1;
				else if (cur_id < 0 && static_cast<UInt64>(-cur_id) <= array_size)
					adjusted_index = array_size + cur_id;
				else
					adjusted_index = array_size; /// Индекс не вписывается в рамки массива, заменяем слишком большим
			}
			else
				throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

			if (adjusted_index < array_size)
			{
				ColumnArray::Offset_t string_pos = current_offset == 0 && adjusted_index == 0
					? 0
					: string_offsets[current_offset + adjusted_index - 1];

				ColumnArray::Offset_t string_size = string_offsets[current_offset + adjusted_index] - string_pos;

				result_data.resize(current_result_offset + string_size);
				memcpy(&result_data[current_result_offset], &data[string_pos], string_size);
				current_result_offset += string_size;
				result_offsets[i] = current_result_offset;
			}
			else
			{
				/// Вставим пустую строку.
				result_data.resize(current_result_offset + 1);
				result_data[current_result_offset] = 0;
				current_result_offset += 1;
				result_offsets[i] = current_result_offset;
			}

			current_offset = offsets[i];
		}
	}
};


class FunctionArrayElement : public IFunction
{
public:
	static constexpr auto name = "arrayElement";
	static IFunction * create(const Context & context) { return new FunctionArrayElement; }

private:
	template <typename T>
	bool executeNumberConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index)
	{
		const ColumnArray * col_array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnVector<T> * col_nested = typeid_cast<const ColumnVector<T> *>(&col_array->getData());

		if (!col_nested)
			return false;

		ColumnVector<T> * col_res = new ColumnVector<T>;
		block.getByPosition(result).column = col_res;

		if (index.getType() == Field::Types::UInt64)
			ArrayElementNumImpl<T>::template vectorConst<false>(col_nested->getData(), col_array->getOffsets(), safeGet<UInt64>(index) - 1, col_res->getData());
		else if (index.getType() == Field::Types::Int64)
			ArrayElementNumImpl<T>::template vectorConst<true>(col_nested->getData(), col_array->getOffsets(), -safeGet<Int64>(index) - 1, col_res->getData());
		else
			throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

		return true;
	}

	template <typename index_type, typename data_type>
	bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<index_type> & index)
	{
		const ColumnArray * col_array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnVector<data_type> * col_nested = typeid_cast<const ColumnVector<data_type> *>(&col_array->getData());

		if (!col_nested)
			return false;

		ColumnVector<data_type> * col_res = new ColumnVector<data_type>;
		block.getByPosition(result).column = col_res;

		ArrayElementNumImpl<data_type>::template vector<index_type>(col_nested->getData(), col_array->getOffsets(), index, col_res->getData());

		return true;
	}

	bool executeStringConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index)
	{
		const ColumnArray * col_array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnString * col_nested = typeid_cast<const ColumnString *>(&col_array->getData());

		if (!col_nested)
			return false;

		ColumnString * col_res = new ColumnString;
		block.getByPosition(result).column = col_res;

		if (index.getType() == Field::Types::UInt64)
			ArrayElementStringImpl::vectorConst<false>(
				col_nested->getChars(),
				col_array->getOffsets(),
				col_nested->getOffsets(),
				safeGet<UInt64>(index) - 1,
				col_res->getChars(),
				col_res->getOffsets());
		else if (index.getType() == Field::Types::Int64)
			ArrayElementStringImpl::vectorConst<true>(
				col_nested->getChars(),
				col_array->getOffsets(),
				col_nested->getOffsets(),
				-safeGet<Int64>(index) - 1,
				col_res->getChars(),
				col_res->getOffsets());
		else
			throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

		return true;
	}

	template <typename index_type>
	bool executeString(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<index_type> & index)
	{
		const ColumnArray * col_array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnString * col_nested = typeid_cast<const ColumnString *>(&col_array->getData());

		if (!col_nested)
			return false;

		ColumnString * col_res = new ColumnString;
		block.getByPosition(result).column = col_res;

		ArrayElementStringImpl::vector<index_type>(
				col_nested->getChars(),
				col_array->getOffsets(),
				col_nested->getOffsets(),
				index,
				col_res->getChars(),
				col_res->getOffsets());

		return true;
	}

	bool executeConstConst(Block & block, const ColumnNumbers & arguments, size_t result, const Field & index)
	{
		const ColumnConstArray * col_array = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const DB::Array & array = col_array->getData();
		size_t array_size = array.size();
		size_t real_index = 0;

		if (index.getType() == Field::Types::UInt64)
			real_index = safeGet<UInt64>(index) - 1;
		else if (index.getType() == Field::Types::Int64)
			real_index = array_size + safeGet<Int64>(index);
		else
			throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

		Field value = col_array->getData().at(real_index);

		block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(
			block.rowsInFirstColumn(),
			value);

		return true;
	}

	template <typename index_type>
	bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<index_type> & index)
	{
		const ColumnConstArray * col_array = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const DB::Array & array = col_array->getData();
		size_t array_size = array.size();

		block.getByPosition(result).column = block.getByPosition(result).type->createColumn();

		for (size_t i = 0; i < col_array->size(); ++i)
		{
			if (index[i].getType() == Field::Types::UInt64)
			{
				UInt64 cur_id = safeGet<UInt64>(index[i]);
				if (cur_id > 0 && cur_id <= array_size)
					block.getByPosition(result).column->insert(array[cur_id - 1]);
				else
					block.getByPosition(result).column->insertDefault();
			}
			else if (index[i].getType() == Field::Types::Int64)
			{
				Int64 cur_id = safeGet<Int64>(index[i]);
				if (cur_id > 0 && static_cast<UInt64>(cur_id) <= array_size)
					block.getByPosition(result).column->insert(array[cur_id - 1]);
				else if (cur_id < 0 && static_cast<UInt64>(-cur_id) <= array_size)
					block.getByPosition(result).column->insert(array[array_size + cur_id]);
				else
					block.getByPosition(result).column->insertDefault();
			}
			else
				throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);
		}

		return true;
	}

	template <typename index_type>
	bool executeArgument(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnVector<index_type> * index = typeid_cast<const ColumnVector<index_type> *> (&*block.getByPosition(arguments[1]).column);

		if (!index)
			return false;

		if (!(	executeNumber<index_type, UInt8>	(block, arguments, result, *index)
			||	executeNumber<index_type, UInt16>	(block, arguments, result, *index)
			||	executeNumber<index_type, UInt32>	(block, arguments, result, *index)
			||	executeNumber<index_type, UInt64>	(block, arguments, result, *index)
			||	executeNumber<index_type, Int8>		(block, arguments, result, *index)
			||	executeNumber<index_type, Int16>	(block, arguments, result, *index)
			||	executeNumber<index_type, Int32>	(block, arguments, result, *index)
			||	executeNumber<index_type, Int64>	(block, arguments, result, *index)
			||	executeNumber<index_type, Float32>	(block, arguments, result, *index)
			||	executeNumber<index_type, Float64>	(block, arguments, result, *index)
			||	executeConst <index_type>			(block, arguments, result, *index)
			||	executeString<index_type>			(block, arguments, result, *index)))
		throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

		return true;
	}

	/** Для массива кортежей функция вычисляется покомпонентно - для каждого элемента кортежа.
	  */
	bool executeTuple(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnArray * col_array = typeid_cast<ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		ColumnTuple * col_nested = typeid_cast<ColumnTuple *>(&col_array->getData());

		if (!col_nested)
			return false;

		Block & tuple_block = col_nested->getData();
		size_t tuple_size = tuple_block.columns();

		/** Будем вычислять функцию для кортежа внутренностей массива.
		  * Для этого создадим временный блок.
		  * Он будет состоять из следующих столбцов:
		  * - индекс массива, который нужно взять;
		  * - массив из первых элементов кортежей;
		  * - результат взятия элементов по индексу для массива из первых элементов кортежей;
		  * - массив из вторых элементов кортежей;
		  * - результат взятия элементов по индексу для массива из вторых элементов кортежей;
		  * ...
		  */
		Block block_of_temporary_results;
		block_of_temporary_results.insert(block.getByPosition(arguments[1]));

		/// результаты взятия элементов по индексу для массивов из каждых элементов кортежей;
		Block result_tuple_block;

		for (size_t i = 0; i < tuple_size; ++i)
		{
			ColumnWithNameAndType array_of_tuple_section;
			array_of_tuple_section.column = new ColumnArray(tuple_block.getByPosition(i).column, col_array->getOffsetsColumn());
			array_of_tuple_section.type = new DataTypeArray(tuple_block.getByPosition(i).type);
			block_of_temporary_results.insert(array_of_tuple_section);

			ColumnWithNameAndType array_elements_of_tuple_section;
			block_of_temporary_results.insert(array_elements_of_tuple_section);

			execute(block_of_temporary_results, ColumnNumbers{i * 2 + 1, 0}, i * 2 + 2);

			result_tuple_block.insert(block_of_temporary_results.getByPosition(i * 2 + 2));
		}

		ColumnTuple * col_res = new ColumnTuple(result_tuple_block);
		block.getByPosition(result).column = col_res;

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
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[0]);
		if (!array_type)
			throw Exception("First argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!arguments[1]->isNumeric()
			|| (0 != arguments[1]->getName().compare(0, 4, "UInt") && 0 != arguments[1]->getName().compare(0, 3, "Int")))
			throw Exception("Second argument for function " + getName() + " must have UInt or Int type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return array_type->getNestedType();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (executeTuple(block, arguments, result))
		{
		}
		else if (!block.getByPosition(arguments[1]).column->isConst())
		{
			if (!(	executeArgument<UInt8>	(block, arguments, result)
				||	executeArgument<UInt16>	(block, arguments, result)
				||	executeArgument<UInt32>	(block, arguments, result)
				||	executeArgument<UInt64>	(block, arguments, result)
				||	executeArgument<Int8>	(block, arguments, result)
				||	executeArgument<Int16>	(block, arguments, result)
				||	executeArgument<Int32>	(block, arguments, result)
				||	executeArgument<Int64>	(block, arguments, result)))
			throw Exception("Second argument for function " + getName() + " must must have UInt or Int type.",
							ErrorCodes::ILLEGAL_COLUMN);
		}
		else
		{
			Field index = (*block.getByPosition(arguments[1]).column)[0];

			if (index == UInt64(0))
				throw Exception("Array indices is 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);

			if (!(	executeNumberConst<UInt8>	(block, arguments, result, index)
				||	executeNumberConst<UInt16>	(block, arguments, result, index)
				||	executeNumberConst<UInt32>	(block, arguments, result, index)
				||	executeNumberConst<UInt64>	(block, arguments, result, index)
				||	executeNumberConst<Int8>		(block, arguments, result, index)
				||	executeNumberConst<Int16>	(block, arguments, result, index)
				||	executeNumberConst<Int32>	(block, arguments, result, index)
				||	executeNumberConst<Int64>	(block, arguments, result, index)
				||	executeNumberConst<Float32>	(block, arguments, result, index)
				||	executeNumberConst<Float64>	(block, arguments, result, index)
				||	executeConstConst			(block, arguments, result, index)
				||	executeStringConst			(block, arguments, result, index)))
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of first argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
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
		const PODArray<T> & data, const ColumnArray::Offsets_t & offsets,
		const T value,
		PODArray<typename IndexConv::ResultType> & result)
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
		const ColumnString::Chars_t & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
		const String & value,
		PODArray<typename IndexConv::ResultType> & result)
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
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionArrayIndex; }

private:
	typedef ColumnVector<typename IndexConv::ResultType> ResultColumnType;

	template <typename T>
	bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result, const Field & value)
	{
		const ColumnArray * col_array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnVector<T> * col_nested = typeid_cast<const ColumnVector<T> *>(&col_array->getData());

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
		const ColumnArray * col_array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnString * col_nested = typeid_cast<const ColumnString *>(&col_array->getData());

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
		const ColumnConstArray * col_array = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[0]).column);

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
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[0]);
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

class FunctionArrayEnumerate : public IFunction
{
public:
	static constexpr auto name = "arrayEnumerate";
	static IFunction * create (const Context & context) { return new FunctionArrayEnumerate; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[0]);
		if (!array_type)
			throw Exception("First argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeArray(new DataTypeUInt32);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnArray * array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column))
		{
			const ColumnArray::Offsets_t & offsets = array->getOffsets();

			ColumnVector<UInt32> * res_nested = new ColumnVector<UInt32>;
			ColumnArray * res_array = new ColumnArray(res_nested, array->getOffsetsColumn());
			block.getByPosition(result).column = res_array;

			ColumnVector<UInt32>::Container_t & res_values = res_nested->getData();
			res_values.resize(array->getData().size());
			size_t prev_off = 0;
			for (size_t i = 0; i < offsets.size(); ++i)
			{
				size_t off = offsets[i];
				for (size_t j = prev_off; j < off; ++j)
				{
					res_values[j] = j - prev_off + 1;
				}
				prev_off = off;
			}
		}
		else if (const ColumnConstArray * array = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[0]).column))
		{
			const Array & values = array->getData();

			Array res_values(values.size());
			for (size_t i = 0; i < values.size(); ++i)
			{
				res_values[i] = i + 1;
			}

			ColumnConstArray * res_array = new ColumnConstArray(array->size(), res_values, new DataTypeArray(new DataTypeUInt32));
			block.getByPosition(result).column = res_array;
		}
		else
		{
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
		}
	}
};

class FunctionArrayEnumerateUniq : public IFunction
{
public:
	static constexpr auto name = "arrayEnumerateUniq";
	static IFunction * create(const Context & context) { return new FunctionArrayEnumerateUniq; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() == 0)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be at least 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		for (size_t i = 0; i < arguments.size(); ++i)
		{
			const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[i]);
			if (!array_type)
				throw Exception("All arguments for function " + getName() + " must be arrays; argument " + toString(i + 1) + " isn't.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}

		return new DataTypeArray(new DataTypeUInt32);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (arguments.size() == 1 && executeConst(block, arguments, result))
			return;

		Columns array_columns(arguments.size());
		const ColumnArray::Offsets_t * offsets = nullptr;
		ConstColumnPlainPtrs data_columns(arguments.size());

		for (size_t i = 0; i < arguments.size(); ++i)
		{
			ColumnPtr array_ptr = block.getByPosition(arguments[i]).column;
			const ColumnArray * array = typeid_cast<const ColumnArray *>(&*array_ptr);
			if (!array)
			{
				const ColumnConstArray * const_array = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[i]).column);
				if (!const_array)
					throw Exception("Illegal column " + block.getByPosition(arguments[i]).column->getName()
						+ " of " + toString(i + 1) + "-th argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
				array_ptr = const_array->convertToFullColumn();
				array = typeid_cast<const ColumnArray *>(&*array_ptr);
			}
			array_columns[i] = array_ptr;
			const ColumnArray::Offsets_t & offsets_i = array->getOffsets();
			if (!i)
				offsets = &offsets_i;
			else if (offsets_i != *offsets)
				throw Exception("Lengths of all arrays passsed to " + getName() + " must be equal.",
					ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
			data_columns[i] = &array->getData();
		}

		const ColumnArray * first_array = typeid_cast<const ColumnArray *>(&*array_columns[0]);
		ColumnVector<UInt32> * res_nested = new ColumnVector<UInt32>;
		ColumnArray * res_array = new ColumnArray(res_nested, first_array->getOffsetsColumn());
		block.getByPosition(result).column = res_array;

		ColumnVector<UInt32>::Container_t & res_values = res_nested->getData();
		if (!offsets->empty())
			res_values.resize(offsets->back());

		if (arguments.size() == 1)
		{
			if (!(	executeNumber<UInt8>	(first_array, res_values)
				||	executeNumber<UInt16>	(first_array, res_values)
				||	executeNumber<UInt32>	(first_array, res_values)
				||	executeNumber<UInt64>	(first_array, res_values)
				||	executeNumber<Int8>		(first_array, res_values)
				||	executeNumber<Int16>	(first_array, res_values)
				||	executeNumber<Int32>	(first_array, res_values)
				||	executeNumber<Int64>	(first_array, res_values)
				||	executeNumber<Float32>	(first_array, res_values)
				||	executeNumber<Float64>	(first_array, res_values)
				||	executeString			(first_array, res_values)))
				throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of first argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else
		{
			if (!execute128bit(*offsets, data_columns, res_values))
				executeHashed(*offsets, data_columns, res_values);
		}
	}

private:
	/// Изначально выделить кусок памяти для 512 элементов.
	static constexpr size_t INITIAL_SIZE_DEGREE = 9;

	template <typename T>
	bool executeNumber(const ColumnArray * array, ColumnVector<UInt32>::Container_t & res_values)
	{
		const ColumnVector<T> * nested = typeid_cast<const ColumnVector<T> *>(&array->getData());
		if (!nested)
			return false;
		const ColumnArray::Offsets_t & offsets = array->getOffsets();
		const typename ColumnVector<T>::Container_t & values = nested->getData();

		typedef ClearableHashMap<T, UInt32, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
			HashTableAllocatorWithStackMemory<(1 << INITIAL_SIZE_DEGREE) * sizeof(T)> > ValuesToIndices;

		ValuesToIndices indices;
		size_t prev_off = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			indices.clear();
			size_t off = offsets[i];
			for (size_t j = prev_off; j < off; ++j)
			{
				res_values[j] = ++indices[values[j]];
			}
			prev_off = off;
		}
		return true;
	}

	bool executeString(const ColumnArray * array, ColumnVector<UInt32>::Container_t & res_values)
	{
		const ColumnString * nested = typeid_cast<const ColumnString *>(&array->getData());
		if (!nested)
			return false;
		const ColumnArray::Offsets_t & offsets = array->getOffsets();

		size_t prev_off = 0;
		typedef ClearableHashMap<StringRef, UInt32, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
			HashTableAllocatorWithStackMemory<(1 << INITIAL_SIZE_DEGREE) * sizeof(StringRef)> > ValuesToIndices;

		ValuesToIndices indices;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			indices.clear();
			size_t off = offsets[i];
			for (size_t j = prev_off; j < off; ++j)
			{
				res_values[j] = ++indices[nested->getDataAt(j)];
			}
			prev_off = off;
		}
		return true;
	}

	bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnConstArray * array = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[0]).column);
		if (!array)
			return false;
		const Array & values = array->getData();

		Array res_values(values.size());
		std::map<Field, UInt32> indices;
		for (size_t i = 0; i < values.size(); ++i)
		{
			res_values[i] = static_cast<UInt64>(++indices[values[i]]);
		}

		ColumnConstArray * res_array = new ColumnConstArray(array->size(), res_values, new DataTypeArray(new DataTypeUInt32));
		block.getByPosition(result).column = res_array;

		return true;
	}

	bool execute128bit(
		const ColumnArray::Offsets_t & offsets,
		const ConstColumnPlainPtrs & columns,
		ColumnVector<UInt32>::Container_t & res_values)
	{
		size_t count = columns.size();
		size_t keys_bytes = 0;
		Sizes key_sizes(count);
		for (size_t j = 0; j < count; ++j)
		{
			if (!columns[j]->isFixed())
				return false;
			key_sizes[j] = columns[j]->sizeOfField();
			keys_bytes += key_sizes[j];
		}
		if (keys_bytes > 16)
			return false;

		typedef ClearableHashMap<UInt128, UInt32, UInt128HashCRC32, HashTableGrower<INITIAL_SIZE_DEGREE>,
			HashTableAllocatorWithStackMemory<(1 << INITIAL_SIZE_DEGREE) * sizeof(UInt128)> > ValuesToIndices;

		ValuesToIndices indices;
		size_t prev_off = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			indices.clear();
			size_t off = offsets[i];
			for (size_t j = prev_off; j < off; ++j)
			{
				res_values[j] = ++indices[packFixed<UInt128>(j, count, columns, key_sizes)];
			}
			prev_off = off;
		}

		return true;
	}

	void executeHashed(
		const ColumnArray::Offsets_t & offsets,
		const ConstColumnPlainPtrs & columns,
		ColumnVector<UInt32>::Container_t & res_values)
	{
		size_t count = columns.size();

		typedef ClearableHashMap<UInt128, UInt32, UInt128TrivialHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
			HashTableAllocatorWithStackMemory<(1 << INITIAL_SIZE_DEGREE) * sizeof(UInt128)> > ValuesToIndices;

		ValuesToIndices indices;
		size_t prev_off = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			indices.clear();
			size_t off = offsets[i];
			for (size_t j = prev_off; j < off; ++j)
			{
				res_values[j] = ++indices[hash128(j, count, columns)];
			}
			prev_off = off;
		}
	}
};


template <typename Type> struct TypeToColumnType { using ColumnType = ColumnVector<Type>; };
template <> struct TypeToColumnType<String> { using ColumnType = ColumnString; };

template <typename DataType> struct DataTypeToName : TypeName<typename DataType::FieldType> { };
template <> struct DataTypeToName<DataTypeDate> { static std::string get() { return "Date"; } };
template <> struct DataTypeToName<DataTypeDateTime> { static std::string get() { return "DateTime"; } };

template <typename DataType>
struct FunctionEmptyArray : public IFunction
{
	static constexpr auto base_name = "emptyArray";
	static const String name;
	static IFunction * create(const Context & context) { return new FunctionEmptyArray; }

private:
	String getName() const
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 0)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeArray{new DataType{}};
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		using UnderlyingColumnType = typename TypeToColumnType<typename DataType::FieldType>::ColumnType;

		block.getByPosition(result).column = new ColumnArray{
			new UnderlyingColumnType,
			new ColumnArray::ColumnOffsets_t{block.rowsInFirstColumn(), 0}
		};
	}
};

template <typename DataType>
const String FunctionEmptyArray<DataType>::name = FunctionEmptyArray::base_name + DataTypeToName<DataType>::get();

class FunctionRange : public IFunction
{
public:
	static constexpr auto max_elements = 100000000;
	static constexpr auto name = "range";
	static IFunction * create(const Context &) { return new FunctionRange; }

private:
	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception{
				"Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		const auto arg = arguments.front().get();

		if (!typeid_cast<const DataTypeUInt8 *>(arg) &&
			!typeid_cast<const DataTypeUInt16 *>(arg) &&
			!typeid_cast<const DataTypeUInt32 *>(arg) &
			!typeid_cast<const DataTypeUInt64 *>(arg))
		{
			throw Exception{
				"Illegal type " + arg->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};
		}

		return new DataTypeArray{arg->clone()};
	}

	template <typename T>
	bool execute(Block & block, const IColumn * const arg, const size_t result)
	{
		if (const auto in = typeid_cast<const ColumnVector<T> *>(arg))
		{
			const auto & in_data = in->getData();
			const auto total_values = std::accumulate(std::begin(in_data), std::end(in_data), std::size_t{},
				std::plus<std::size_t>{});
			if (total_values > max_elements)
				throw Exception{
					"A call to function " + getName() + " would produce " + std::to_string(total_values) +
						" array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
					ErrorCodes::ARGUMENT_OUT_OF_BOUND
				};

			const auto data_col = new ColumnVector<T>{total_values};
			const auto out = new ColumnArray{
				data_col,
				new ColumnArray::ColumnOffsets_t{in->size()}
			};
			block.getByPosition(result).column = out;

			auto & out_data = data_col->getData();
			auto & out_offsets = out->getOffsets();

			IColumn::Offset_t offset{};
			for (const auto i : ext::range(0, in->size()))
			{
				std::copy(ext::make_range_iterator(T{}), ext::make_range_iterator(in_data[i]), &out_data[offset]);
				offset += in_data[i];
				out_offsets[i] = offset;
			}

			return true;
		}
		else if (const auto in = typeid_cast<const ColumnConst<T> *>(arg))
		{
			const auto & in_data = in->getData();
			const std::size_t total_values = in->size() * in_data;
			if (total_values > max_elements)
				throw Exception{
					"A call to function " + getName() + " would produce " + std::to_string(total_values) +
						" array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
					ErrorCodes::ARGUMENT_OUT_OF_BOUND
				};

			const auto data_col = new ColumnVector<T>{total_values};
			const auto out = new ColumnArray{
				data_col,
				new ColumnArray::ColumnOffsets_t{in->size()}
			};
			block.getByPosition(result).column = out;

			auto & out_data = data_col->getData();
			auto & out_offsets = out->getOffsets();

			IColumn::Offset_t offset{};
			for (const auto i : ext::range(0, in->size()))
			{
				std::copy(ext::make_range_iterator(T{}), ext::make_range_iterator(in_data), &out_data[offset]);
				offset += in_data;
				out_offsets[i] = offset;
			}

			return true;
		}

		return false;
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto col = block.getByPosition(arguments[0]).column.get();

		if (!execute<UInt8>(block, col, result) &&
			!execute<UInt16>(block, col, result) &&
			!execute<UInt32>(block, col, result) &&
			!execute<UInt64>(block, col, result))
		{
			throw Exception{
				"Illegal column " + col->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
		}
	}
};


struct NameHas			{ static constexpr auto name = "has"; };
struct NameIndexOf		{ static constexpr auto name = "indexOf"; };
struct NameCountEqual	{ static constexpr auto name = "countEqual"; };

typedef FunctionArrayIndex<IndexToOne, 		NameHas>	FunctionHas;
typedef FunctionArrayIndex<IndexIdentity, 	NameIndexOf>	FunctionIndexOf;
typedef FunctionArrayIndex<IndexCount, 	NameCountEqual>	FunctionCountEqual;

using FunctionEmptyArrayUInt8 = FunctionEmptyArray<DataTypeUInt8>;
using FunctionEmptyArrayUInt16 = FunctionEmptyArray<DataTypeUInt16>;
using FunctionEmptyArrayUInt32 = FunctionEmptyArray<DataTypeUInt32>;
using FunctionEmptyArrayUInt64 = FunctionEmptyArray<DataTypeUInt64>;
using FunctionEmptyArrayInt8 = FunctionEmptyArray<DataTypeInt8>;
using FunctionEmptyArrayInt16 = FunctionEmptyArray<DataTypeInt16>;
using FunctionEmptyArrayInt32 = FunctionEmptyArray<DataTypeInt32>;
using FunctionEmptyArrayInt64 = FunctionEmptyArray<DataTypeInt64>;
using FunctionEmptyArrayFloat32 = FunctionEmptyArray<DataTypeFloat32>;
using FunctionEmptyArrayFloat64 = FunctionEmptyArray<DataTypeFloat64>;
using FunctionEmptyArrayDate = FunctionEmptyArray<DataTypeDate>;
using FunctionEmptyArrayDateTime = FunctionEmptyArray<DataTypeDateTime>;
using FunctionEmptyArrayString = FunctionEmptyArray<DataTypeString>;


}
