#pragma once

#include <DB/Core/FieldVisitors.h>

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
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTLiteral.h>

#include <ext/range.hpp>

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
  *
  * arrayUniq(arr) - считает количество разных элементов в массиве,
  * arrayUniq(arr1, arr2, ...) - считает количество разных кортежей из элементов на соответствующих позициях в нескольких массивах.
  *
  * arrayEnumerateUniq(arr)
  *  - возаращает массив,  параллельный данному, где для каждого элемента указано,
  *  какой он по счету среди элементов с таким значением.
  *  Например: arrayEnumerateUniq([10, 20, 10, 30]) = [1,  1,  2,  1]
  * arrayEnumerateUniq(arr1, arr2...)
  *  - для кортежей из элементов на соответствующих позициях в нескольких массивах.
  *
  * emptyArrayToSingle(arr) - заменить пустые массивы на массивы из одного элемента со значением "по-умолчанию".
  *
  * arrayReduce('agg', arr1, ...) - применить агрегатную функцию agg к массивам arr1...
  */


class FunctionArray : public IFunction
{
public:
	static constexpr auto name = "array";
	static IFunction * create(const Context & context) { return new FunctionArray; }

private:
	/// Получить имя функции.
	String getName() const override
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

	static const DataTypePtr & getScalarType(const DataTypePtr & type)
	{
		const auto array = typeid_cast<const DataTypeArray *>(type.get());

		if (!array)
			return type;

		return getScalarType(array->getNestedType());
	}

public:
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const auto is_const = [&] {
			for (const auto arg_num : arguments)
				if (!block.getByPosition(arg_num).column->isConst())
					return false;

			return true;
		}();

		const auto first_arg = block.getByPosition(arguments[0]);
		DataTypePtr result_type = first_arg.type;
		if (result_type->behavesAsNumber())
		{
			/// Если тип числовой, вычисляем наименьший общий тип
			for (size_t i = 1, size = arguments.size(); i < size; ++i)
				result_type = getLeastCommonType(result_type, block.getByPosition(arguments[i]).type);
		}

		if (is_const)
		{
			Array arr;
			for (const auto arg_num : arguments)
				if (block.getByPosition(arg_num).type->getName() == result_type->getName())
					/// Если элемент такого же типа как результат, просто добавляем его в ответ
					arr.push_back((*block.getByPosition(arg_num).column)[0]);
				else
					/// Иначе необходимо привести его к типу результата
					addField(result_type, (*block.getByPosition(arg_num).column)[0], arr);

			block.getByPosition(result).column = new ColumnConstArray{
				first_arg.column->size(), arr, new DataTypeArray{result_type}
			};
		}
		else
		{
			auto out = new ColumnArray{result_type->createColumn()};
			ColumnPtr out_ptr{out};

			for (const auto row_num : ext::range(0, first_arg.column->size()))
			{
				Array arr;

				for (const auto arg_num : arguments)
					if (block.getByPosition(arg_num).type->getName() == result_type->getName())
						/// Если элемент такого же типа как результат, просто добавляем его в ответ
						arr.push_back((*block.getByPosition(arg_num).column)[row_num]);
					else
						/// Иначе необходимо привести его к типу результата
						addField(result_type, (*block.getByPosition(arg_num).column)[row_num], arr);

				out->insert(arr);
			}

			block.getByPosition(result).column = out_ptr;
		}
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
			ColumnWithTypeAndName array_of_tuple_section;
			array_of_tuple_section.column = new ColumnArray(tuple_block.getByPosition(i).column, col_array->getOffsetsColumn());
			array_of_tuple_section.type = new DataTypeArray(tuple_block.getByPosition(i).type);
			block_of_temporary_results.insert(array_of_tuple_section);

			ColumnWithTypeAndName array_elements_of_tuple_section;
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
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
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
	static bool apply(size_t j, ResultType & current) { current = 1; return false; }
};

/// Для indexOf.
struct IndexIdentity
{
	typedef UInt64 ResultType;
	/// Индекс возвращается начиная с единицы.
	static bool apply(size_t j, ResultType & current) { current = j + 1; return false; }
};

/// Для countEqual.
struct IndexCount
{
	typedef UInt32 ResultType;
	static bool apply(size_t j, ResultType & current) { ++current; return true; }
};


template <typename T, typename U, typename IndexConv>
struct ArrayIndexNumImpl
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

	/// compares `lhs` against `i`-th element of `rhs`
	static bool compare(const T & lhs, const PODArray<U> & rhs, const std::size_t i ) { return lhs == rhs[i]; }
	/// compares `lhs against `rhs`, third argument unused
	static bool compare(const T & lhs, const U & rhs, std::size_t) { return lhs == rhs; }

#pragma GCC diagnostic pop

	template <typename ScalarOrVector>
	static void vector(
		const PODArray<T> & data, const ColumnArray::Offsets_t & offsets,
		const ScalarOrVector & value,
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
				if (compare(data[current_offset + j], value, i))
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
	static void vector_const(
		const ColumnString::Chars_t & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
		const String & value,
		PODArray<typename IndexConv::ResultType> & result)
	{
		const auto size = offsets.size();
		const auto value_size = value.size();
		result.resize(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			const auto array_size = offsets[i] - current_offset;
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

	static void vector_vector(
		const ColumnString::Chars_t & data, const ColumnArray::Offsets_t & offsets, const ColumnString::Offsets_t & string_offsets,
		const ColumnString::Chars_t & item_values, const ColumnString::Offsets_t & item_offsets,
		PODArray<typename IndexConv::ResultType> & result)
	{
		const auto size = offsets.size();
		result.resize(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			const auto array_size = offsets[i] - current_offset;
			typename IndexConv::ResultType current = 0;
			const auto value_pos = 0 == i ? 0 : item_offsets[i - 1];
			const auto value_size = item_offsets[i] - value_pos;

			for (size_t j = 0; j < array_size; ++j)
			{
				ColumnArray::Offset_t string_pos = current_offset == 0 && j == 0
												   ? 0
												   : string_offsets[current_offset + j - 1];

				ColumnArray::Offset_t string_size = string_offsets[current_offset + j] - string_pos;

				if (string_size == value_size && 0 == memcmp(&item_values[value_pos], &data[string_pos], value_size))
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
	bool executeNumber(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		return executeNumberNumber<T, UInt8>(block, arguments, result)
			|| executeNumberNumber<T, UInt16>(block, arguments, result)
			|| executeNumberNumber<T, UInt32>(block, arguments, result)
			|| executeNumberNumber<T, UInt64>(block, arguments, result)
			|| executeNumberNumber<T, Int8>(block, arguments, result)
			|| executeNumberNumber<T, Int16>(block, arguments, result)
			|| executeNumberNumber<T, Int32>(block, arguments, result)
			|| executeNumberNumber<T, Int64>(block, arguments, result)
			|| executeNumberNumber<T, Float32>(block, arguments, result)
			|| executeNumberNumber<T, Float64>(block, arguments, result);
	}

	template <typename T, typename U>
	bool executeNumberNumber(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnArray * col_array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnVector<T> * col_nested = typeid_cast<const ColumnVector<T> *>(&col_array->getData());

		if (!col_nested)
			return false;

		const auto item_arg = block.getByPosition(arguments[1]).column.get();

		if (const auto item_arg_const = typeid_cast<const ColumnConst<U> *>(item_arg))
		{
			const auto col_res = new ResultColumnType;
			ColumnPtr col_ptr{col_res};
			block.getByPosition(result).column = col_ptr;

			ArrayIndexNumImpl<T, U, IndexConv>::vector(col_nested->getData(), col_array->getOffsets(),
				item_arg_const->getData(), col_res->getData());
		}
		else if (const auto item_arg_vector = typeid_cast<const ColumnVector<U> *>(item_arg))
		{
			const auto col_res = new ResultColumnType;
			ColumnPtr col_ptr{col_res};
			block.getByPosition(result).column = col_ptr;

			ArrayIndexNumImpl<T, U, IndexConv>::vector(col_nested->getData(), col_array->getOffsets(),
				item_arg_vector->getData(), col_res->getData());
		}
		else
			return false;

		return true;
	}

	bool executeString(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnArray * col_array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const ColumnString * col_nested = typeid_cast<const ColumnString *>(&col_array->getData());

		if (!col_nested)
			return false;

		const auto item_arg = block.getByPosition(arguments[1]).column.get();

		if (const auto item_arg_const = typeid_cast<const ColumnConst<String> *>(item_arg))
		{
			const auto col_res = new ResultColumnType;
			ColumnPtr col_ptr{col_res};
			block.getByPosition(result).column = col_ptr;

			ArrayIndexStringImpl<IndexConv>::vector_const(col_nested->getChars(), col_array->getOffsets(),
				col_nested->getOffsets(), item_arg_const->getData(), col_res->getData());
		}
		else if (const auto item_arg_vector = typeid_cast<const ColumnString *>(item_arg))
		{
			const auto col_res = new ResultColumnType;
			ColumnPtr col_ptr{col_res};
			block.getByPosition(result).column = col_ptr;

			ArrayIndexStringImpl<IndexConv>::vector_vector(col_nested->getChars(), col_array->getOffsets(),
				col_nested->getOffsets(), item_arg_vector->getChars(), item_arg_vector->getOffsets(),
				col_res->getData());
		}

		return true;
	}

	bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnConstArray * col_array = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[0]).column);

		if (!col_array)
			return false;

		const Array & arr = col_array->getData();

		const auto item_arg = block.getByPosition(arguments[1]).column.get();
		if (item_arg->isConst())
		{
			typename IndexConv::ResultType current{};
			const auto & value = (*item_arg)[0];

			for (size_t i = 0, size = arr.size(); i < size; ++i)
			{
				if (apply_visitor(FieldVisitorAccurateEquals(), arr[i], value))
				{
					if (!IndexConv::apply(i, current))
						break;
				}
			}

			block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(
				item_arg->size(),
				static_cast<typename NearestFieldType<typename IndexConv::ResultType>::Type>(current));
		}
		else
		{
			const auto size = item_arg->size();
			const auto col_res = new ResultColumnType{size, {}};
			ColumnPtr col_ptr{col_res};
			block.getByPosition(result).column = col_ptr;

			auto & data = col_res->getData();

			for (size_t row = 0; row < size; ++row)
			{
				const auto & value = (*item_arg)[row];
				for (size_t i = 0, size = arr.size(); i < size; ++i)
				{
					if (apply_visitor(FieldVisitorAccurateEquals(), arr[i], value))
					{
						if (!IndexConv::apply(i, data[row]))
							break;
					}
				}
			}
		}

		return true;
	}


public:
	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[0]);
		if (!array_type)
			throw Exception("First argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!(array_type->getNestedType()->behavesAsNumber() && arguments[1]->behavesAsNumber())
			&& array_type->getNestedType()->getName() != arguments[1]->getName())
			throw Exception("Type of array elements and second argument for function " + getName() + " must be same."
				" Passed: " + arguments[0]->getName() + " and " + arguments[1]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new typename DataTypeFromFieldType<typename IndexConv::ResultType>::Type;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		if (!(executeNumber<UInt8>(block, arguments, result)
			  || executeNumber<UInt16>(block, arguments, result)
			  || executeNumber<UInt32>(block, arguments, result)
			  || executeNumber<UInt64>(block, arguments, result)
			  || executeNumber<Int8>(block, arguments, result)
			  || executeNumber<Int16>(block, arguments, result)
			  || executeNumber<Int32>(block, arguments, result)
			  || executeNumber<Int64>(block, arguments, result)
			  || executeNumber<Float32>(block, arguments, result)
			  || executeNumber<Float64>(block, arguments, result)
			  || executeConst(block, arguments, result)
			  || executeString(block, arguments, result)))
			throw Exception{
				"Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
	}
};

class FunctionArrayEnumerate : public IFunction
{
public:
	static constexpr auto name = "arrayEnumerate";
	static IFunction * create (const Context & context) { return new FunctionArrayEnumerate; }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		if (const ColumnArray * array = typeid_cast<const ColumnArray *>(&*block.getByPosition(arguments[0]).column))
		{
			const ColumnArray::Offsets_t & offsets = array->getOffsets();

			ColumnUInt32 * res_nested = new ColumnUInt32;
			ColumnArray * res_array = new ColumnArray(res_nested, array->getOffsetsColumn());
			block.getByPosition(result).column = res_array;

			ColumnUInt32::Container_t & res_values = res_nested->getData();
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


/// Считает количество разных элементов в массиве, или количество разных кортежей из элементов на соответствующих позициях в нескольких массивах.
/// NOTE Реализация частично совпадает с arrayEnumerateUniq.
class FunctionArrayUniq : public IFunction
{
public:
	static constexpr auto name = "arrayUniq";
	static IFunction * create(const Context & context) { return new FunctionArrayUniq; }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
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

		return new DataTypeUInt32;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
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
		ColumnUInt32 * res = new ColumnUInt32;
		block.getByPosition(result).column = res;

		ColumnUInt32::Container_t & res_values = res->getData();
		res_values.resize(offsets->size());

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
	bool executeNumber(const ColumnArray * array, ColumnUInt32::Container_t & res_values)
	{
		const ColumnVector<T> * nested = typeid_cast<const ColumnVector<T> *>(&array->getData());
		if (!nested)
			return false;
		const ColumnArray::Offsets_t & offsets = array->getOffsets();
		const typename ColumnVector<T>::Container_t & values = nested->getData();

		typedef ClearableHashSet<T, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
			HashTableAllocatorWithStackMemory<(1 << INITIAL_SIZE_DEGREE) * sizeof(T)> > Set;

		Set set;
		size_t prev_off = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			set.clear();
			size_t off = offsets[i];
			for (size_t j = prev_off; j < off; ++j)
				set.insert(values[j]);

			res_values[i] = set.size();
			prev_off = off;
		}
		return true;
	}

	bool executeString(const ColumnArray * array, ColumnUInt32::Container_t & res_values)
	{
		const ColumnString * nested = typeid_cast<const ColumnString *>(&array->getData());
		if (!nested)
			return false;
		const ColumnArray::Offsets_t & offsets = array->getOffsets();

		typedef ClearableHashSet<StringRef, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
			HashTableAllocatorWithStackMemory<(1 << INITIAL_SIZE_DEGREE) * sizeof(StringRef)> > Set;

		Set set;
		size_t prev_off = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			set.clear();
			size_t off = offsets[i];
			for (size_t j = prev_off; j < off; ++j)
				set.insert(nested->getDataAt(j));

			res_values[i] = set.size();
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

		std::set<Field> set;
		for (size_t i = 0; i < values.size(); ++i)
			set.insert(values[i]);

		block.getByPosition(result).column = new ColumnConstUInt32(array->size(), set.size());
		return true;
	}

	bool execute128bit(
		const ColumnArray::Offsets_t & offsets,
		const ConstColumnPlainPtrs & columns,
		ColumnUInt32::Container_t & res_values)
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

		typedef ClearableHashSet<UInt128, UInt128HashCRC32, HashTableGrower<INITIAL_SIZE_DEGREE>,
			HashTableAllocatorWithStackMemory<(1 << INITIAL_SIZE_DEGREE) * sizeof(UInt128)> > Set;

		Set set;
		size_t prev_off = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			set.clear();
			size_t off = offsets[i];
			for (size_t j = prev_off; j < off; ++j)
				set.insert(packFixed<UInt128>(j, count, columns, key_sizes));

			res_values[i] = set.size();
			prev_off = off;
		}

		return true;
	}

	void executeHashed(
		const ColumnArray::Offsets_t & offsets,
		const ConstColumnPlainPtrs & columns,
		ColumnUInt32::Container_t & res_values)
	{
		size_t count = columns.size();

		typedef ClearableHashSet<UInt128, UInt128TrivialHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
			HashTableAllocatorWithStackMemory<(1 << INITIAL_SIZE_DEGREE) * sizeof(UInt128)> > Set;

		Set set;
		size_t prev_off = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			set.clear();
			size_t off = offsets[i];
			for (size_t j = prev_off; j < off; ++j)
				set.insert(hash128(j, count, columns));

			res_values[i] = set.size();
			prev_off = off;
		}
	}
};


class FunctionArrayEnumerateUniq : public IFunction
{
public:
	static constexpr auto name = "arrayEnumerateUniq";
	static IFunction * create(const Context & context) { return new FunctionArrayEnumerateUniq; }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
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
		ColumnUInt32 * res_nested = new ColumnUInt32;
		ColumnArray * res_array = new ColumnArray(res_nested, first_array->getOffsetsColumn());
		block.getByPosition(result).column = res_array;

		ColumnUInt32::Container_t & res_values = res_nested->getData();
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
	bool executeNumber(const ColumnArray * array, ColumnUInt32::Container_t & res_values)
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

	bool executeString(const ColumnArray * array, ColumnUInt32::Container_t & res_values)
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
		ColumnUInt32::Container_t & res_values)
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
		ColumnUInt32::Container_t & res_values)
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
	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 0)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeArray{new DataType{}};
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
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
				[this] (const std::size_t lhs, const std::size_t rhs) {
					const auto sum = lhs + rhs;
					if (sum < lhs)
						throw Exception{
							"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
							ErrorCodes::ARGUMENT_OUT_OF_BOUND
						};

					return sum;
				});

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
			if (in->size() > std::numeric_limits<std::size_t>::max() / in_data)
				throw Exception{
					"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
					ErrorCodes::ARGUMENT_OUT_OF_BOUND
				};

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


class FunctionEmptyArrayToSingle : public IFunction
{
public:
	static constexpr auto name = "emptyArrayToSingle";
	static IFunction * create(const Context & context) { return new FunctionEmptyArrayToSingle; }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
		if (!array_type)
			throw Exception("Argument for function " + getName() + " must be array.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arguments[0]->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		if (executeConst(block, arguments, result))
			return;

		const ColumnArray * array = typeid_cast<const ColumnArray *>(block.getByPosition(arguments[0]).column.get());
		if (!array)
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		ColumnPtr res_ptr = array->cloneEmpty();
		block.getByPosition(result).column = res_ptr;
		ColumnArray & res = static_cast<ColumnArray &>(*res_ptr);

		const IColumn & src_data = array->getData();
		const ColumnArray::Offsets_t & src_offsets = array->getOffsets();
		IColumn & res_data = res.getData();
		ColumnArray::Offsets_t & res_offsets = res.getOffsets();

		if (!(	executeNumber<UInt8>	(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<UInt16>	(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<UInt32>	(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<UInt64>	(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<Int8>		(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<Int16>	(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<Int32>	(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<Int64>	(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<Float32>	(src_data, src_offsets, res_data, res_offsets)
			||	executeNumber<Float64>	(src_data, src_offsets, res_data, res_offsets)
			||	executeString			(src_data, src_offsets, res_data, res_offsets)
			||	executeFixedString		(src_data, src_offsets, res_data, res_offsets)))
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}

private:
	bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnConstArray * const_array = typeid_cast<const ColumnConstArray *>(block.getByPosition(arguments[0]).column.get()))
		{
			if (const_array->getData().empty())
			{
				auto nested_type = typeid_cast<const DataTypeArray &>(*block.getByPosition(arguments[0]).type).getNestedType();

				block.getByPosition(result).column = new ColumnConstArray(
					block.rowsInFirstColumn(),
					{nested_type->getDefault()},
					nested_type->clone());
			}
			else
				block.getByPosition(result).column = block.getByPosition(arguments[0]).column;

			return true;
		}
		else
			return false;
	}

	template <typename T>
	bool executeNumber(
		const IColumn & src_data, const ColumnArray::Offsets_t & src_offsets,
		IColumn & res_data_col, ColumnArray::Offsets_t & res_offsets)
	{
		if (const ColumnVector<T> * src_data_concrete = typeid_cast<const ColumnVector<T> *>(&src_data))
		{
			const PODArray<T> & src_data = src_data_concrete->getData();
			PODArray<T> & res_data = typeid_cast<ColumnVector<T> &>(res_data_col).getData();
			size_t size = src_offsets.size();
			res_offsets.resize(size);
			res_data.reserve(src_data.size());

			ColumnArray::Offset_t src_prev_offset = 0;
			ColumnArray::Offset_t res_prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				if (src_offsets[i] != src_prev_offset)
				{
					size_t size_to_write = src_offsets[i] - src_prev_offset;
					size_t prev_res_data_size = res_data.size();
					res_data.resize(prev_res_data_size + size_to_write);
					memcpy(&res_data[prev_res_data_size], &src_data[src_prev_offset], size_to_write * sizeof(T));
					res_prev_offset += size_to_write;
					res_offsets[i] = res_prev_offset;
				}
				else
				{
					res_data.push_back(T());
					++res_prev_offset;
					res_offsets[i] = res_prev_offset;
				}

				src_prev_offset = src_offsets[i];
			}

			return true;
		}
		else
			return false;
	}

	bool executeFixedString(
		const IColumn & src_data, const ColumnArray::Offsets_t & src_offsets,
		IColumn & res_data_col, ColumnArray::Offsets_t & res_offsets)
	{
		if (const ColumnFixedString * src_data_concrete = typeid_cast<const ColumnFixedString *>(&src_data))
		{
			const size_t n = src_data_concrete->getN();
			const ColumnFixedString::Chars_t & src_data = src_data_concrete->getChars();
			ColumnFixedString::Chars_t & res_data = typeid_cast<ColumnFixedString &>(res_data_col).getChars();
			size_t size = src_offsets.size();
			res_offsets.resize(size);
			res_data.reserve(src_data.size());

			ColumnArray::Offset_t src_prev_offset = 0;
			ColumnArray::Offset_t res_prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				if (src_offsets[i] != src_prev_offset)
				{
					size_t size_to_write = src_offsets[i] - src_prev_offset;
					size_t prev_res_data_size = res_data.size();
					res_data.resize(prev_res_data_size + size_to_write * n);
					memcpy(&res_data[prev_res_data_size], &src_data[src_prev_offset], size_to_write * n);
					res_prev_offset += size_to_write;
					res_offsets[i] = res_prev_offset;
				}
				else
				{
					size_t prev_res_data_size = res_data.size();
					res_data.resize(prev_res_data_size + n);
					memset(&res_data[prev_res_data_size], 0, n);
					++res_prev_offset;
					res_offsets[i] = res_prev_offset;
				}

				src_prev_offset = src_offsets[i];
			}

			return true;
		}
		else
			return false;
	}

	bool executeString(
		const IColumn & src_data, const ColumnArray::Offsets_t & src_array_offsets,
		IColumn & res_data_col, ColumnArray::Offsets_t & res_array_offsets)
	{
		if (const ColumnString * src_data_concrete = typeid_cast<const ColumnString *>(&src_data))
		{
			const ColumnString::Offsets_t & src_string_offsets = src_data_concrete->getOffsets();
			ColumnString::Offsets_t & res_string_offsets = typeid_cast<ColumnString &>(res_data_col).getOffsets();

			const ColumnString::Chars_t & src_data = src_data_concrete->getChars();
			ColumnString::Chars_t & res_data = typeid_cast<ColumnString &>(res_data_col).getChars();

			size_t size = src_array_offsets.size();
			res_array_offsets.resize(size);
			res_string_offsets.reserve(src_string_offsets.size());
			res_data.reserve(src_data.size());

			ColumnArray::Offset_t src_array_prev_offset = 0;
			ColumnArray::Offset_t res_array_prev_offset = 0;

			ColumnString::Offset_t src_string_prev_offset = 0;
			ColumnString::Offset_t res_string_prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				if (src_array_offsets[i] != src_array_prev_offset)
				{
					size_t array_size = src_array_offsets[i] - src_array_prev_offset;

					size_t bytes_to_copy = 0;
					size_t from_string_prev_offset_local = src_string_prev_offset;
					for (size_t j = 0; j < array_size; ++j)
					{
						size_t string_size = src_string_offsets[src_array_prev_offset + j] - from_string_prev_offset_local;

						res_string_prev_offset += string_size;
						res_string_offsets.push_back(res_string_prev_offset);

						from_string_prev_offset_local += string_size;
						bytes_to_copy += string_size;
					}

					size_t res_data_old_size = res_data.size();
					res_data.resize(res_data_old_size + bytes_to_copy);
					memcpy(&res_data[res_data_old_size], &src_data[src_string_prev_offset], bytes_to_copy);

					res_array_prev_offset += array_size;
					res_array_offsets[i] = res_array_prev_offset;
				}
				else
				{
					res_data.push_back(0);	/// Пустая строка, включая ноль на конце.

					++res_string_prev_offset;
					res_string_offsets.push_back(res_string_prev_offset);

					++res_array_prev_offset;
					res_array_offsets[i] = res_array_prev_offset;
				}

				src_array_prev_offset = src_array_offsets[i];

				if (src_array_prev_offset)
					src_string_prev_offset = src_string_offsets[src_array_prev_offset - 1];
			}

			return true;
		}
		else
			return false;
	}
};


class FunctionArrayReverse : public IFunction
{
public:
	static constexpr auto name = "reverse";
	static IFunction * create(const Context & context) { return new FunctionArrayReverse; }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
		if (!array_type)
			throw Exception("Argument for function " + getName() + " must be array.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arguments[0]->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		if (executeConst(block, arguments, result))
			return;

		const ColumnArray * array = typeid_cast<const ColumnArray *>(block.getByPosition(arguments[0]).column.get());
		if (!array)
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		ColumnPtr res_ptr = array->cloneEmpty();
		block.getByPosition(result).column = res_ptr;
		ColumnArray & res = static_cast<ColumnArray &>(*res_ptr);

		const IColumn & src_data = array->getData();
		const ColumnArray::Offsets_t & offsets = array->getOffsets();
		IColumn & res_data = res.getData();
		res.getOffsetsColumn() = array->getOffsetsColumn();

		if (!(	executeNumber<UInt8>	(src_data, offsets, res_data)
			||	executeNumber<UInt16>	(src_data, offsets, res_data)
			||	executeNumber<UInt32>	(src_data, offsets, res_data)
			||	executeNumber<UInt64>	(src_data, offsets, res_data)
			||	executeNumber<Int8>		(src_data, offsets, res_data)
			||	executeNumber<Int16>	(src_data, offsets, res_data)
			||	executeNumber<Int32>	(src_data, offsets, res_data)
			||	executeNumber<Int64>	(src_data, offsets, res_data)
			||	executeNumber<Float32>	(src_data, offsets, res_data)
			||	executeNumber<Float64>	(src_data, offsets, res_data)
			||	executeString			(src_data, offsets, res_data)
			||	executeFixedString		(src_data, offsets, res_data)))
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}

private:
	bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnConstArray * const_array = typeid_cast<const ColumnConstArray *>(block.getByPosition(arguments[0]).column.get()))
		{
			const Array & arr = const_array->getData();

			size_t size = arr.size();
			Array res(size);

			for (size_t i = 0; i < size; ++i)
				res[i] = arr[size - i - 1];

			block.getByPosition(result).column = new ColumnConstArray(
				block.rowsInFirstColumn(),
				res,
				block.getByPosition(arguments[0]).type->clone());

			return true;
		}
		else
			return false;
	}

	template <typename T>
	bool executeNumber(
		const IColumn & src_data, const ColumnArray::Offsets_t & src_offsets,
		IColumn & res_data_col)
	{
		if (const ColumnVector<T> * src_data_concrete = typeid_cast<const ColumnVector<T> *>(&src_data))
		{
			const PODArray<T> & src_data = src_data_concrete->getData();
			PODArray<T> & res_data = typeid_cast<ColumnVector<T> &>(res_data_col).getData();
			size_t size = src_offsets.size();
			res_data.resize(src_data.size());

			ColumnArray::Offset_t src_prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				const T * src = &src_data[src_prev_offset];
				const T * src_end = &src_data[src_offsets[i]];

				if (src == src_end)
					continue;

				T * dst = &res_data[src_offsets[i] - 1];

				while (src < src_end)
				{
					*dst = *src;
					++src;
					--dst;
				}

				src_prev_offset = src_offsets[i];
			}

			return true;
		}
		else
			return false;
	}

	bool executeFixedString(
		const IColumn & src_data, const ColumnArray::Offsets_t & src_offsets,
		IColumn & res_data_col)
	{
		if (const ColumnFixedString * src_data_concrete = typeid_cast<const ColumnFixedString *>(&src_data))
		{
			const size_t n = src_data_concrete->getN();
			const ColumnFixedString::Chars_t & src_data = src_data_concrete->getChars();
			ColumnFixedString::Chars_t & res_data = typeid_cast<ColumnFixedString &>(res_data_col).getChars();
			size_t size = src_offsets.size();
			res_data.resize(src_data.size());

			ColumnArray::Offset_t src_prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				const UInt8 * src = &src_data[src_prev_offset * n];
				const UInt8 * src_end = &src_data[src_offsets[i] * n];

				if (src == src_end)
					continue;

				UInt8 * dst = &res_data[src_offsets[i] * n - n];

				while (src < src_end)
				{
					memcpy(dst, src, n);
					src += n;
					dst -= n;
				}

				src_prev_offset = src_offsets[i];
			}

			return true;
		}
		else
			return false;
	}

	bool executeString(
		const IColumn & src_data, const ColumnArray::Offsets_t & src_array_offsets,
		IColumn & res_data_col)
	{
		if (const ColumnString * src_data_concrete = typeid_cast<const ColumnString *>(&src_data))
		{
			const ColumnString::Offsets_t & src_string_offsets = src_data_concrete->getOffsets();
			ColumnString::Offsets_t & res_string_offsets = typeid_cast<ColumnString &>(res_data_col).getOffsets();

			const ColumnString::Chars_t & src_data = src_data_concrete->getChars();
			ColumnString::Chars_t & res_data = typeid_cast<ColumnString &>(res_data_col).getChars();

			size_t size = src_array_offsets.size();
			res_string_offsets.resize(src_string_offsets.size());
			res_data.resize(src_data.size());

			ColumnArray::Offset_t src_array_prev_offset = 0;
			ColumnString::Offset_t res_string_prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				if (src_array_offsets[i] != src_array_prev_offset)
				{
					size_t array_size = src_array_offsets[i] - src_array_prev_offset;

					for (size_t j = 0; j < array_size; ++j)
					{
						size_t j_reversed = array_size - j - 1;

						auto src_pos = src_array_prev_offset + j_reversed == 0 ? 0 : src_string_offsets[src_array_prev_offset + j_reversed - 1];
						size_t string_size = src_string_offsets[src_array_prev_offset + j_reversed] - src_pos;

						memcpy(&res_data[res_string_prev_offset], &src_data[src_pos], string_size);

						res_string_prev_offset += string_size;
						res_string_offsets[src_array_prev_offset + j] = res_string_prev_offset;
					}
				}

				src_array_prev_offset = src_array_offsets[i];
			}

			return true;
		}
		else
			return false;
	}
};


/** Применяет к массиву агрегатную функцию и возвращает её результат.
  * Также может быть применена к нескольким массивам одинаковых размеров, если агрегатная функция принимает несколько аргументов.
  */
class FunctionArrayReduce : public IFunction
{
public:
	static constexpr auto name = "arrayReduce";
	static IFunction * create(const Context & context) { return new FunctionArrayReduce; }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	void getReturnTypeAndPrerequisites(
		const ColumnsWithTypeAndName & arguments,
		DataTypePtr & out_return_type,
		std::vector<ExpressionAction> & out_prerequisites) override
	{
		/// Первый аргумент - константная строка с именем агрегатной функции (возможно, с параметрами в скобках, например: "quantile(0.99)").

		if (arguments.size() < 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be at least 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const ColumnConstString * aggregate_function_name_column = typeid_cast<const ColumnConstString *>(arguments[0].column.get());
		if (!aggregate_function_name_column)
			throw Exception("First argument for function " + getName() + " must be constant string: name of aggregate function.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		DataTypes argument_types(arguments.size() - 1);
		for (size_t i = 1, size = arguments.size(); i < size; ++i)
		{
			const DataTypeArray * arg = typeid_cast<const DataTypeArray *>(arguments[i].type.get());
			if (!arg)
				throw Exception("Argument " + toString(i) + " for function " + getName() + " must be array.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			argument_types[i - 1] = arg->getNestedType()->clone();
		}

		if (!aggregate_function)
		{
			const String & aggregate_function_name_with_params = aggregate_function_name_column->getData();

			if (aggregate_function_name_with_params.empty())
				throw Exception("First argument for function " + getName() + " (name of aggregate function) cannot be empty.",
					ErrorCodes::BAD_ARGUMENTS);

			bool has_parameters = ')' == aggregate_function_name_with_params.back();

			String aggregate_function_name = aggregate_function_name_with_params;
			String parameters;
			Array params_row;

			if (has_parameters)
			{
				size_t pos = aggregate_function_name_with_params.find('(');
				if (pos == std::string::npos || pos + 2 >= aggregate_function_name_with_params.size())
					throw Exception("First argument for function " + getName() + " doesn't look like aggregate function name.",
						ErrorCodes::BAD_ARGUMENTS);

				aggregate_function_name = aggregate_function_name_with_params.substr(0, pos);
				parameters = aggregate_function_name_with_params.substr(pos + 1, aggregate_function_name_with_params.size() - pos - 2);

				if (aggregate_function_name.empty())
					throw Exception("First argument for function " + getName() + " doesn't look like aggregate function name.",
						ErrorCodes::BAD_ARGUMENTS);

				ParserExpressionList params_parser(false);
				ASTPtr args_ast = parseQuery(params_parser,
					parameters.data(), parameters.data() + parameters.size(),
					"parameters of aggregate function");

				ASTExpressionList & args_list = typeid_cast<ASTExpressionList &>(*args_ast);

				if (args_list.children.empty())
					throw Exception("Incorrect list of parameters to aggregate function "
						+ aggregate_function_name, ErrorCodes::BAD_ARGUMENTS);

				params_row.reserve(args_list.children.size());
				for (const auto & child : args_list.children)
				{
					const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(child.get());
					if (!lit)
						throw Exception("Parameters to aggregate functions must be literals",
							ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS);

					params_row.push_back(lit->value);
				}
			}

			aggregate_function = AggregateFunctionFactory().get(aggregate_function_name, argument_types);

			/// Потому что владение состояниями агрегатных функций никуда не отдаётся.
			if (aggregate_function->isState())
				throw Exception("Using aggregate function with -State modifier in function arrayReduce is not supported", ErrorCodes::BAD_ARGUMENTS);

			if (has_parameters)
				aggregate_function->setParameters(params_row);
			aggregate_function->setArguments(argument_types);
		}

		out_return_type = aggregate_function->getReturnType();
	}


	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		IAggregateFunction & agg_func = *aggregate_function.get();
		std::unique_ptr<char[]> place_holder { new char[agg_func.sizeOfData()] };
		AggregateDataPtr place = place_holder.get();

		size_t rows = block.rowsInFirstColumn();

		/// Агрегатные функции не поддерживают константные столбцы. Поэтому, материализуем их.
		std::vector<ColumnPtr> materialized_columns;

		std::vector<const IColumn *> aggregate_arguments_vec(arguments.size() - 1);

		for (size_t i = 0, size = arguments.size() - 1; i < size; ++i)
		{
			const IColumn * col = block.unsafeGetByPosition(arguments[i + 1]).column.get();
			if (const ColumnArray * arr = typeid_cast<const ColumnArray *>(col))
			{
				aggregate_arguments_vec[i] = arr->getDataPtr().get();
			}
			else if (const ColumnConstArray * arr = typeid_cast<const ColumnConstArray *>(col))
			{
				materialized_columns.emplace_back(arr->convertToFullColumn());
				aggregate_arguments_vec[i] = typeid_cast<const ColumnArray &>(*materialized_columns.back().get()).getDataPtr().get();
			}
			else
				throw Exception("Illegal column " + col->getName() + " as argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

		}
		const IColumn ** aggregate_arguments = aggregate_arguments_vec.data();

		const ColumnArray::Offsets_t & offsets = typeid_cast<const ColumnArray &>(!materialized_columns.empty()
			? *materialized_columns.front().get()
			: *block.unsafeGetByPosition(arguments[1]).column.get()).getOffsets();

		ColumnPtr result_holder = block.getByPosition(result).type->createColumn();
		block.getByPosition(result).column = result_holder;
		IColumn & res_col = *result_holder.get();

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < rows; ++i)
		{
			agg_func.create(place);
			ColumnArray::Offset_t next_offset = offsets[i];

			try
			{
				for (size_t j = current_offset; j < next_offset; ++j)
					agg_func.add(place, aggregate_arguments, j);

				agg_func.insertResultInto(place, res_col);
			}
			catch (...)
			{
				agg_func.destroy(place);
				throw;
			}

			agg_func.destroy(place);
			current_offset = next_offset;
		}
	}

private:
	AggregateFunctionPtr aggregate_function;
};


struct NameHas			{ static constexpr auto name = "has"; };
struct NameIndexOf		{ static constexpr auto name = "indexOf"; };
struct NameCountEqual	{ static constexpr auto name = "countEqual"; };

typedef FunctionArrayIndex<IndexToOne, 		NameHas>		FunctionHas;
typedef FunctionArrayIndex<IndexIdentity, 	NameIndexOf>	FunctionIndexOf;
typedef FunctionArrayIndex<IndexCount, 	NameCountEqual>		FunctionCountEqual;

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
