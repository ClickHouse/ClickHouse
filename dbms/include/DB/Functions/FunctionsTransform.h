#pragma once

#include <mutex>

#include <DB/Core/FieldVisitors.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Common/Arena.h>
#include <DB/Core/StringRef.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** transform(x, from_array, to_array[, default]) - преобразовать x согласно переданному явным образом соответствию.
  */

DataTypePtr getSmallestCommonNumericType(const IDataType & t1, const IDataType & t2);

/** transform(x, [from...], [to...], default)
  * - преобразует значения согласно явно указанному отображению.
  *
  * x - что преобразовывать.
  * from - константный массив значений для преобразования.
  * to - константный массив значений, в которые должны быть преобразованы значения из from.
  * default - какое значение использовать, если x не равен ни одному из значений во from.
  * from и to - массивы одинаковых размеров.
  *
  * Типы:
  * transform(T, Array(T), Array(U), U) -> U
  *
  * transform(x, [from...], [to...])
  * - eсли default не указан, то для значений x, для которых нет соответствующего элемента во from, возвращается не изменённое значение x.
  *
  * Типы:
  * transform(T, Array(T), Array(T)) -> T
  *
  * Замечание: реализация довольно громоздкая.
  */
class FunctionTransform : public IFunction
{
public:
	static constexpr auto name = "transform";
	static IFunction * create(const Context &) { return new FunctionTransform; }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		const auto args_size = arguments.size();
		if (args_size != 3 && args_size != 4)
			throw Exception{
				"Number of arguments for function " + getName() + " doesn't match: passed " +
					toString(args_size) + ", should be 3 or 4",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

		const IDataType * type_x = arguments[0].get();

		if (!type_x->isNumeric() && !typeid_cast<const DataTypeString *>(type_x))
			throw Exception("Unsupported type " + type_x->getName()
				+ " of first argument of function " + getName()
				+ ", must be numeric type or Date/DateTime or String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const DataTypeArray * type_arr_from = typeid_cast<const DataTypeArray *>(arguments[1].get());

		if (!type_arr_from)
			throw Exception("Second argument of function " + getName()
				+ ", must be array of source values to transform from.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const auto type_arr_from_nested = type_arr_from->getNestedType();

		if ((type_x->isNumeric() != type_arr_from_nested->isNumeric())
			|| (!!typeid_cast<const DataTypeString *>(type_x) != !!typeid_cast<const DataTypeString *>(type_arr_from_nested.get())))
			throw Exception("First argument and elements of array of second argument of function " + getName()
				+ " must have compatible types: both numeric or both strings.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const DataTypeArray * type_arr_to = typeid_cast<const DataTypeArray *>(arguments[2].get());

		if (!type_arr_to)
			throw Exception("Third argument of function " + getName()
				+ ", must be array of destination values to transform to.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const auto type_arr_to_nested = type_arr_to->getNestedType();

		if (args_size == 3)
		{
			if ((type_x->isNumeric() != type_arr_to_nested->isNumeric())
				|| (!!typeid_cast<const DataTypeString *>(type_x) != !!typeid_cast<const DataTypeString *>(type_arr_to_nested.get())))
				throw Exception("Function " + getName()
					+ " have signature: transform(T, Array(T), Array(U), U) -> U; or transform(T, Array(T), Array(T)) -> T; where T and U are types.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			return type_x->clone();
		}
		else
		{
			const IDataType * type_default = arguments[3].get();

			if (!type_default->isNumeric() && !typeid_cast<const DataTypeString *>(type_default))
				throw Exception("Unsupported type " + type_default->getName()
					+ " of fourth argument (default value) of function " + getName()
					+ ", must be numeric type or Date/DateTime or String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			if ((type_default->isNumeric() != type_arr_to_nested->isNumeric())
				|| (!!typeid_cast<const DataTypeString *>(type_default) != !!typeid_cast<const DataTypeString *>(type_arr_to_nested.get())))
				throw Exception("Function " + getName()
					+ " have signature: transform(T, Array(T), Array(U), U) -> U; or transform(T, Array(T), Array(T)) -> T; where T and U are types.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			if (type_arr_to_nested->behavesAsNumber() && type_default->behavesAsNumber())
			{
				/// Берём наименьший общий тип для элементов массива значений to и для default-а.
				return getSmallestCommonNumericType(*type_arr_to_nested, *type_default);
			}

			/// TODO Больше проверок.
			return type_arr_to_nested->clone();
		}
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const ColumnConstArray * array_from = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[1]).column);
		const ColumnConstArray * array_to = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[2]).column);

		if (!array_from || !array_to)
			throw Exception("Second and third arguments of function " + getName() + " must be constant arrays.", ErrorCodes::ILLEGAL_COLUMN);

		prepare(array_from->getData(), array_to->getData(), block, arguments);

		const auto in = block.getByPosition(arguments.front()).column.get();

		if (in->isConst())
		{
			executeConst(block, arguments, result);
			return;
		}

		const IColumn * default_column = nullptr;
		if (arguments.size() == 4)
			default_column = block.getByPosition(arguments[3]).column.get();

		auto column_result = block.getByPosition(result).type->createColumn();
		auto out = column_result.get();

		if (!executeNum<UInt8>(in, out, default_column)
			&& !executeNum<UInt16>(in, out, default_column)
			&& !executeNum<UInt32>(in, out, default_column)
			&& !executeNum<UInt64>(in, out, default_column)
			&& !executeNum<Int8>(in, out, default_column)
			&& !executeNum<Int16>(in, out, default_column)
			&& !executeNum<Int32>(in, out, default_column)
			&& !executeNum<Int64>(in, out, default_column)
			&& !executeNum<Float32>(in, out, default_column)
			&& !executeNum<Float64>(in, out, default_column)
			&& !executeString(in, out, default_column))
			throw Exception(
				"Illegal column " + in->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		block.getByPosition(result).column = column_result;
	}

private:
	void executeConst(Block & block, const ColumnNumbers & arguments, const size_t result)
	{
		/// Составим блок из полноценных столбцов размера 1 и вычислим функцию как обычно.

		Block tmp_block;
		ColumnNumbers tmp_arguments;

		tmp_block.insert(block.getByPosition(arguments[0]));
		tmp_block.getByPosition(0).column = static_cast<IColumnConst *>(tmp_block.getByPosition(0).column->cloneResized(1).get())->convertToFullColumn();
		tmp_arguments.push_back(0);

		for (size_t i = 1; i < arguments.size(); ++i)
		{
			tmp_block.insert(block.getByPosition(arguments[i]));
			tmp_arguments.push_back(i);
		}

		tmp_block.insert(block.getByPosition(result));
		size_t tmp_result = arguments.size();

		execute(tmp_block, tmp_arguments, tmp_result);

		block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(
			block.rowsInFirstColumn(),
			(*tmp_block.getByPosition(tmp_result).column)[0]);
	}

	template <typename T>
	bool executeNum(const IColumn * in_untyped, IColumn * out_untyped, const IColumn * default_untyped)
	{
		if (const auto in = typeid_cast<const ColumnVector<T> *>(in_untyped))
		{
			if (!default_untyped)
			{
				auto out = typeid_cast<ColumnVector<T> *>(out_untyped);
				if (!out)
					throw Exception(
						"Illegal column " + out_untyped->getName() + " of elements of array of third argument of function " + getName()
						+ ", must be " + in->getName(),
						ErrorCodes::ILLEGAL_COLUMN);

				executeImplNumToNum<T>(in->getData(), out->getData());
			}
			else if (default_untyped->isConst())
			{
				if (!executeNumToNumWithConstDefault<T, UInt8>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, UInt16>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, UInt32>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, UInt64>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, Int8>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, Int16>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, Int32>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, Int64>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, Float32>(in, out_untyped)
					&& !executeNumToNumWithConstDefault<T, Float64>(in, out_untyped)
					&& !executeNumToStringWithConstDefault<T>(in, out_untyped))
					throw Exception(
						"Illegal column " + in->getName() + " of elements of array of second argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
			}
			else
			{
				if (!executeNumToNumWithNonConstDefault<T, UInt8>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, UInt16>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, UInt32>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, UInt64>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, Int8>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, Int16>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, Int32>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, Int64>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, Float32>(in, out_untyped, default_untyped)
					&& !executeNumToNumWithNonConstDefault<T, Float64>(in, out_untyped, default_untyped)
					&& !executeNumToStringWithNonConstDefault<T>(in, out_untyped, default_untyped))
					throw Exception(
						"Illegal column " + in->getName() + " of elements of array of second argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
			}

			return true;
		}

		return false;
	}

	bool executeString(const IColumn * in_untyped, IColumn * out_untyped, const IColumn * default_untyped)
	{
		if (const auto in = typeid_cast<const ColumnString *>(in_untyped))
		{
			if (!default_untyped)
			{
				if (!executeStringToString(in, out_untyped))
					throw Exception(
						"Illegal column " + in->getName() + " of elements of array of second argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
			}
			else if (default_untyped->isConst())
			{
				if (!executeStringToNumWithConstDefault<UInt8>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<UInt16>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<UInt32>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<UInt64>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<Int8>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<Int16>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<Int32>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<Int64>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<Float32>(in, out_untyped)
					&& !executeStringToNumWithConstDefault<Float64>(in, out_untyped)
					&& !executeStringToStringWithConstDefault(in, out_untyped))
					throw Exception(
						"Illegal column " + in->getName() + " of elements of array of second argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
			}
			else
			{
				if (!executeStringToNumWithNonConstDefault<UInt8>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<UInt16>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<UInt32>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<UInt64>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<Int8>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<Int16>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<Int32>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<Int64>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<Float32>(in, out_untyped, default_untyped)
					&& !executeStringToNumWithNonConstDefault<Float64>(in, out_untyped, default_untyped)
					&& !executeStringToStringWithNonConstDefault(in, out_untyped, default_untyped))
					throw Exception(
						"Illegal column " + in->getName() + " of elements of array of second argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
			}

			return true;
		}

		return false;
	}

	template <typename T, typename U>
	bool executeNumToNumWithConstDefault(const ColumnVector<T> * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnVector<U> *>(out_untyped);
		if (!out)
			return false;

		executeImplNumToNumWithConstDefault<T, U>(in->getData(), out->getData(), const_default_value.get<U>());
		return true;
	}

	template <typename T, typename U>
	bool executeNumToNumWithNonConstDefault(const ColumnVector<T> * in, IColumn * out_untyped, const IColumn * default_untyped)
	{
		auto out = typeid_cast<ColumnVector<U> *>(out_untyped);
		if (!out)
			return false;

		if (!executeNumToNumWithNonConstDefault2<T, U, UInt8>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, UInt16>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, UInt32>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, UInt64>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, Int8>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, Int16>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, Int32>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, Int64>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, Float32>(in, out, default_untyped)
			&& !executeNumToNumWithNonConstDefault2<T, U, Float64>(in, out, default_untyped))
			throw Exception(
				"Illegal column " + default_untyped->getName() + " of fourth argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		return true;
	}

	template <typename T, typename U, typename V>
	bool executeNumToNumWithNonConstDefault2(const ColumnVector<T> * in, ColumnVector<U> * out, const IColumn * default_untyped)
	{
		auto col_default = typeid_cast<const ColumnVector<V> *>(default_untyped);
		if (!col_default)
			return false;

		executeImplNumToNumWithNonConstDefault<T, U, V>(in->getData(), out->getData(), col_default->getData());
		return true;
	}

	template <typename T>
	bool executeNumToStringWithConstDefault(const ColumnVector<T> * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnString *>(out_untyped);
		if (!out)
			return false;

		const String & default_str = const_default_value.get<const String &>();
		StringRef default_string_ref{default_str.data(), default_str.size() + 1};
		executeImplNumToStringWithConstDefault<T>(in->getData(), out->getChars(), out->getOffsets(), default_string_ref);
		return true;
	}

	template <typename T>
	bool executeNumToStringWithNonConstDefault(const ColumnVector<T> * in, IColumn * out_untyped, const IColumn * default_untyped)
	{
		auto out = typeid_cast<ColumnString *>(out_untyped);
		if (!out)
			return false;

		auto default_col = typeid_cast<const ColumnString *>(default_untyped);
		if (!default_col)
			throw Exception("Illegal column " + default_untyped->getName() + " of fourth argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		executeImplNumToStringWithNonConstDefault<T>(
			in->getData(),
			out->getChars(), out->getOffsets(),
			default_col->getChars(), default_col->getOffsets());

		return true;
	}

	template <typename U>
	bool executeStringToNumWithConstDefault(const ColumnString * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnVector<U> *>(out_untyped);
		if (!out)
			return false;

		executeImplStringToNumWithConstDefault<U>(in->getChars(), in->getOffsets(), out->getData(), const_default_value.get<U>());
		return true;
	}

	template <typename U>
	bool executeStringToNumWithNonConstDefault(const ColumnString * in, IColumn * out_untyped, const IColumn * default_untyped)
	{
		auto out = typeid_cast<ColumnVector<U> *>(out_untyped);
		if (!out)
			return false;

		if (!executeStringToNumWithNonConstDefault2<U, UInt8>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, UInt16>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, UInt32>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, UInt64>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, Int8>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, Int16>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, Int32>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, Int64>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, Float32>(in, out, default_untyped)
			&& !executeStringToNumWithNonConstDefault2<U, Float64>(in, out, default_untyped))
			throw Exception(
				"Illegal column " + default_untyped->getName() + " of fourth argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		return true;
	}

	template <typename U, typename V>
	bool executeStringToNumWithNonConstDefault2(const ColumnString * in, ColumnVector<U> * out, const IColumn * default_untyped)
	{
		auto col_default = typeid_cast<const ColumnVector<V> *>(default_untyped);
		if (!col_default)
			return false;

		executeImplStringToNumWithNonConstDefault<U, V>(in->getChars(), in->getOffsets(), out->getData(), col_default->getData());
		return true;
	}

	bool executeStringToString(const ColumnString * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnString *>(out_untyped);
		if (!out)
			return false;

		executeImplStringToString(in->getChars(), in->getOffsets(), out->getChars(), out->getOffsets());
		return true;
	}

	bool executeStringToStringWithConstDefault(const ColumnString * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnString *>(out_untyped);
		if (!out)
			return false;

		const String & default_str = const_default_value.get<const String &>();
		StringRef default_string_ref{default_str.data(), default_str.size() + 1};
		executeImplStringToStringWithConstDefault(in->getChars(), in->getOffsets(), out->getChars(), out->getOffsets(), default_string_ref);
		return true;
	}

	bool executeStringToStringWithNonConstDefault(const ColumnString * in, IColumn * out_untyped, const IColumn * default_untyped)
	{
		auto out = typeid_cast<ColumnString *>(out_untyped);
		if (!out)
			return false;

		auto default_col = typeid_cast<const ColumnString *>(default_untyped);
		if (!default_col)
			throw Exception("Illegal column " + default_untyped->getName() + " of fourth argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		executeImplStringToStringWithNonConstDefault(
			in->getChars(), in->getOffsets(),
			out->getChars(), out->getOffsets(),
			default_col->getChars(), default_col->getOffsets());

		return true;
	}


	template <typename T, typename U>
	void executeImplNumToNumWithConstDefault(const PaddedPODArray<T> & src, PaddedPODArray<U> & dst, U dst_default)
	{
		const auto & table = *table_num_to_num;
		size_t size = src.size();
		dst.resize(size);
		for (size_t i = 0; i < size; ++i)
		{
			auto it = table.find(src[i]);
			if (it != table.end())
				memcpy(&dst[i], &it->second, sizeof(dst[i]));	/// little endian.
			else
				dst[i] = dst_default;
		}
	}

	template <typename T, typename U, typename V>
	void executeImplNumToNumWithNonConstDefault(const PaddedPODArray<T> & src, PaddedPODArray<U> & dst, const PaddedPODArray<V> & dst_default)
	{
		const auto & table = *table_num_to_num;
		size_t size = src.size();
		dst.resize(size);
		for (size_t i = 0; i < size; ++i)
		{
			auto it = table.find(src[i]);
			if (it != table.end())
				memcpy(&dst[i], &it->second, sizeof(dst[i]));	/// little endian.
			else
				dst[i] = dst_default[i];
		}
	}

	template <typename T>
	void executeImplNumToNum(const PaddedPODArray<T> & src, PaddedPODArray<T> & dst)
	{
		const auto & table = *table_num_to_num;
		size_t size = src.size();
		dst.resize(size);
		for (size_t i = 0; i < size; ++i)
		{
			auto it = table.find(src[i]);
			if (it != table.end())
				memcpy(&dst[i], &it->second, sizeof(dst[i]));
			else
				dst[i] = src[i];
		}
	}

	template <typename T>
	void executeImplNumToStringWithConstDefault(const PaddedPODArray<T> & src,
		ColumnString::Chars_t & dst_data, ColumnString::Offsets_t & dst_offsets, StringRef dst_default)
	{
		const auto & table = *table_num_to_string;
		size_t size = src.size();
		dst_offsets.resize(size);
		ColumnString::Offset_t current_dst_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			auto it = table.find(src[i]);
			StringRef ref = it != table.end() ? it->second : dst_default;
			dst_data.resize(current_dst_offset + ref.size);
			memcpy(&dst_data[current_dst_offset], ref.data, ref.size);
			current_dst_offset += ref.size;
			dst_offsets[i] = current_dst_offset;
		}
	}

	template <typename T>
	void executeImplNumToStringWithNonConstDefault(const PaddedPODArray<T> & src,
		ColumnString::Chars_t & dst_data, ColumnString::Offsets_t & dst_offsets,
		const ColumnString::Chars_t & dst_default_data, const ColumnString::Offsets_t & dst_default_offsets)
	{
		const auto & table = *table_num_to_string;
		size_t size = src.size();
		dst_offsets.resize(size);
		ColumnString::Offset_t current_dst_offset = 0;
		ColumnString::Offset_t current_dst_default_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			auto it = table.find(src[i]);
			StringRef ref;

			if (it != table.end())
				ref = it->second;
			else
			{
				ref.data = reinterpret_cast<const char *>(&dst_default_data[current_dst_default_offset]);
				ref.size = dst_default_offsets[i] - current_dst_default_offset;
			}

			dst_data.resize(current_dst_offset + ref.size);
			memcpy(&dst_data[current_dst_offset], ref.data, ref.size);
			current_dst_offset += ref.size;
			current_dst_default_offset = dst_default_offsets[i];
			dst_offsets[i] = current_dst_offset;
		}
	}

	template <typename U>
	void executeImplStringToNumWithConstDefault(
		const ColumnString::Chars_t & src_data, const ColumnString::Offsets_t & src_offsets,
		PaddedPODArray<U> & dst, U dst_default)
	{
		const auto & table = *table_string_to_num;
		size_t size = src_offsets.size();
		dst.resize(size);
		ColumnString::Offset_t current_src_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			StringRef ref{&src_data[current_src_offset], src_offsets[i] - current_src_offset};
			current_src_offset = src_offsets[i];
			auto it = table.find(ref);
			if (it != table.end())
				memcpy(&dst[i], &it->second, sizeof(dst[i]));
			else
				dst[i] = dst_default;
		}
	}

	template <typename U, typename V>
	void executeImplStringToNumWithNonConstDefault(
		const ColumnString::Chars_t & src_data, const ColumnString::Offsets_t & src_offsets,
		PaddedPODArray<U> & dst, const PaddedPODArray<V> & dst_default)
	{
		const auto & table = *table_string_to_num;
		size_t size = src_offsets.size();
		dst.resize(size);
		ColumnString::Offset_t current_src_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			StringRef ref{&src_data[current_src_offset], src_offsets[i] - current_src_offset};
			current_src_offset = src_offsets[i];
			auto it = table.find(ref);
			if (it != table.end())
				memcpy(&dst[i], &it->second, sizeof(dst[i]));
			else
				dst[i] = dst_default[i];
		}
	}

	template <bool with_default>
	void executeImplStringToStringWithOrWithoutConstDefault(
		const ColumnString::Chars_t & src_data, const ColumnString::Offsets_t & src_offsets,
		ColumnString::Chars_t & dst_data, ColumnString::Offsets_t & dst_offsets, StringRef dst_default)
	{
		const auto & table = *table_string_to_string;
		size_t size = src_offsets.size();
		dst_offsets.resize(size);
		ColumnString::Offset_t current_src_offset = 0;
		ColumnString::Offset_t current_dst_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			StringRef src_ref{&src_data[current_src_offset], src_offsets[i] - current_src_offset};
			current_src_offset = src_offsets[i];

			auto it = table.find(src_ref);

			StringRef dst_ref = it != table.end() ? it->second : (with_default ? dst_default : src_ref);
			dst_data.resize(current_dst_offset + dst_ref.size);
			memcpy(&dst_data[current_dst_offset], dst_ref.data, dst_ref.size);
			current_dst_offset += dst_ref.size;
			dst_offsets[i] = current_dst_offset;
		}
	}

	void executeImplStringToString(
		const ColumnString::Chars_t & src_data, const ColumnString::Offsets_t & src_offsets,
		ColumnString::Chars_t & dst_data, ColumnString::Offsets_t & dst_offsets)
	{
		executeImplStringToStringWithOrWithoutConstDefault<false>(src_data, src_offsets, dst_data, dst_offsets, {});
	}

	void executeImplStringToStringWithConstDefault(
		const ColumnString::Chars_t & src_data, const ColumnString::Offsets_t & src_offsets,
		ColumnString::Chars_t & dst_data, ColumnString::Offsets_t & dst_offsets, StringRef dst_default)
	{
		executeImplStringToStringWithOrWithoutConstDefault<true>(src_data, src_offsets, dst_data, dst_offsets, dst_default);
	}

	void executeImplStringToStringWithNonConstDefault(
		const ColumnString::Chars_t & src_data, const ColumnString::Offsets_t & src_offsets,
		ColumnString::Chars_t & dst_data, ColumnString::Offsets_t & dst_offsets,
		const ColumnString::Chars_t & dst_default_data, const ColumnString::Offsets_t & dst_default_offsets)
	{
		const auto & table = *table_string_to_string;
		size_t size = src_offsets.size();
		dst_offsets.resize(size);
		ColumnString::Offset_t current_src_offset = 0;
		ColumnString::Offset_t current_dst_offset = 0;
		ColumnString::Offset_t current_dst_default_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			StringRef src_ref{&src_data[current_src_offset], src_offsets[i] - current_src_offset};
			current_src_offset = src_offsets[i];

			auto it = table.find(src_ref);
			StringRef dst_ref;

			if (it != table.end())
				dst_ref = it->second;
			else
			{
				dst_ref.data = reinterpret_cast<const char *>(&dst_default_data[current_dst_default_offset]);
				dst_ref.size = dst_default_offsets[i] - current_dst_default_offset;
			}

			dst_data.resize(current_dst_offset + dst_ref.size);
			memcpy(&dst_data[current_dst_offset], dst_ref.data, dst_ref.size);
			current_dst_offset += dst_ref.size;
			current_dst_default_offset = dst_default_offsets[i];
			dst_offsets[i] = current_dst_offset;
		}
	}


	/// Разные варианты хэш-таблиц для реализации отображения.

	using NumToNum = HashMap<UInt64, UInt64, HashCRC32<UInt64>>;
	using NumToString = HashMap<UInt64, StringRef, HashCRC32<UInt64>>;		/// Везде StringRef-ы с завершающим нулём.
	using StringToNum = HashMap<StringRef, UInt64, StringRefHash>;
	using StringToString = HashMap<StringRef, StringRef, StringRefHash>;

	std::unique_ptr<NumToNum> table_num_to_num;
	std::unique_ptr<NumToString> table_num_to_string;
	std::unique_ptr<StringToNum> table_string_to_num;
	std::unique_ptr<StringToString> table_string_to_string;

	Arena string_pool;

	Field const_default_value;	/// Null, если не задано.

	bool prepared = false;
	std::mutex mutex;

	/// Может вызываться из разных потоков. Срабатывает только при первом вызове.
	void prepare(const Array & from, const Array & to, Block & block, const ColumnNumbers & arguments)
	{
		if (prepared)
			return;

		const size_t size = from.size();
		if (0 == size)
			throw Exception("Empty arrays are illegal in function " + getName(), ErrorCodes::BAD_ARGUMENTS);

		std::lock_guard<std::mutex> lock(mutex);

		if (prepared)
			return;

		if (from.size() != to.size())
			throw Exception("Second and third arguments of function " + getName() + " must be arrays of same size", ErrorCodes::BAD_ARGUMENTS);

		Array converted_to;
		const Array * used_to = &to;

		/// Задано ли значение по-умолчанию.

		if (arguments.size() == 4)
		{
			const IColumn * default_col = block.getByPosition(arguments[3]).column.get();
			const IColumnConst * const_default_col = dynamic_cast<const IColumnConst *>(default_col);

			if (const_default_col)
				const_default_value = (*const_default_col)[0];

			/// Нужно ли преобразовать элементы to и default_value к наименьшему общему типу, который является Float64?
			bool default_col_is_float =
				   typeid_cast<const ColumnFloat32 *>(default_col)
				|| typeid_cast<const ColumnFloat64 *>(default_col)
				|| typeid_cast<const ColumnConstFloat32 *>(default_col)
				|| typeid_cast<const ColumnConstFloat64 *>(default_col);

			bool to_is_float = to[0].getType() == Field::Types::Float64;

			if (default_col_is_float && !to_is_float)
			{
				converted_to.resize(to.size());
				for (size_t i = 0, size = to.size(); i < size; ++i)
					converted_to[i] = apply_visitor(FieldVisitorConvertToNumber<Float64>(), to[i]);
				used_to = &converted_to;
			}
			else if (!default_col_is_float && to_is_float)
			{
				if (const_default_col)
					const_default_value = apply_visitor(FieldVisitorConvertToNumber<Float64>(), const_default_value);
			}
		}

		/// Замечание: не делается проверка дубликатов в массиве from.

		if (from[0].getType() != Field::Types::String && to[0].getType() != Field::Types::String)
		{
			table_num_to_num.reset(new NumToNum);
			auto & table = *table_num_to_num;
			for (size_t i = 0; i < size; ++i)
				table[from[i].get<UInt64>()] = (*used_to)[i].get<UInt64>();
		}
		else if (from[0].getType() != Field::Types::String && to[0].getType() == Field::Types::String)
		{
			table_num_to_string.reset(new NumToString);
			auto & table = *table_num_to_string;
			for (size_t i = 0; i < size; ++i)
			{
				const String & str_to = to[i].get<const String &>();
				StringRef ref{string_pool.insert(str_to.data(), str_to.size() + 1), str_to.size() + 1};
				table[from[i].get<UInt64>()] = ref;
			}
		}
		else if (from[0].getType() == Field::Types::String && to[0].getType() != Field::Types::String)
		{
			table_string_to_num.reset(new StringToNum);
			auto & table = *table_string_to_num;
			for (size_t i = 0; i < size; ++i)
			{
				const String & str_from = from[i].get<const String &>();
				StringRef ref{string_pool.insert(str_from.data(), str_from.size() + 1), str_from.size() + 1};
				table[ref] = (*used_to)[i].get<UInt64>();
			}
		}
		else if (from[0].getType() == Field::Types::String && to[0].getType() == Field::Types::String)
		{
			table_string_to_string.reset(new StringToString);
			auto & table = *table_string_to_string;
			for (size_t i = 0; i < size; ++i)
			{
				const String & str_from = from[i].get<const String &>();
				const String & str_to = to[i].get<const String &>();
				StringRef ref_from{string_pool.insert(str_from.data(), str_from.size() + 1), str_from.size() + 1};
				StringRef ref_to{string_pool.insert(str_to.data(), str_to.size() + 1), str_to.size() + 1};
				table[ref_from] = ref_to;
			}
		}

		prepared = true;
	}
};

}
