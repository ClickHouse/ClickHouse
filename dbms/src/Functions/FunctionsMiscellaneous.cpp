#include <math.h>

#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArithmetic.h>
#include <DB/Functions/FunctionsMiscellaneous.h>


namespace DB
{


template <typename T>
static void numWidthVector(const PODArray<T> & a, PODArray<UInt64> & c)
{
	size_t size = a.size();
	for (size_t i = 0; i < size; ++i)
		if (a[i] >= 0)
			c[i] = a[i] ? 1 + log10(a[i]) : 1;
		else if (std::is_signed<T>::value && a[i] == std::numeric_limits<T>::min())
			c[i] = 2 + log10(std::numeric_limits<T>::max());
		else
			c[i] = 2 + log10(-a[i]);
}

template <typename T>
static void numWidthConstant(T a, UInt64 & c)
{
	if (a >= 0)
		c = a ? 1 + log10(a) : 1;
	else if (std::is_signed<T>::value && a == std::numeric_limits<T>::min())
		c = 2 + log10(std::numeric_limits<T>::max());
	else
		c = 2 + log10(-a);
}

inline UInt64 floatWidth(const double x)
{
	/// Не быстро.
	char tmp[25];
	double_conversion::StringBuilder builder{tmp, sizeof(tmp)};

	const auto result = getDoubleToStringConverter<false>().ToShortest(x, &builder);

	if (!result)
		throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	return builder.position();
}

template <typename T>
static void floatWidthVector(const PODArray<T> & a, PODArray<UInt64> & c)
{
	size_t size = a.size();
	for (size_t i = 0; i < size; ++i)
		c[i] = floatWidth(a[i]);
}

template <typename T>
static void floatWidthConstant(T a, UInt64 & c)
{
	c = floatWidth(a);
}

template <> inline void numWidthVector<Float64>(const PODArray<Float64> & a, PODArray<UInt64> & c) { floatWidthVector(a, c); }
template <> inline void numWidthVector<Float32>(const PODArray<Float32> & a, PODArray<UInt64> & c) { floatWidthVector(a, c); }
template <> inline void numWidthConstant<Float64>(Float64 a, UInt64 & c) { floatWidthConstant(a, c); }
template <> inline void numWidthConstant<Float32>(Float32 a, UInt64 & c) { floatWidthConstant(a, c); }


static inline void stringWidthVector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets, PODArray<UInt64> & res)
{
	size_t size = offsets.size();

	size_t prev_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		res[i] = stringWidth(&data[prev_offset], &data[offsets[i] - 1]);
		prev_offset = offsets[i];
	}
}

static inline void stringWidthFixedVector(const ColumnString::Chars_t & data, size_t n, PODArray<UInt64> & res)
{
	size_t size = data.size() / n;
	for (size_t i = 0; i < size; ++i)
		res[i] = stringWidth(&data[i * n], &data[(i + 1) * n]);
}


namespace VisibleWidth
{
	template <typename T>
	static bool executeConstNumber(Block & block, const ColumnPtr & column, size_t result)
	{
		if (const ColumnConst<T> * col = typeid_cast<const ColumnConst<T> *>(&*column))
		{
			UInt64 res = 0;
			numWidthConstant(col->getData(), res);
			block.getByPosition(result).column = new ColumnConstUInt64(column->size(), res);
			return true;
		}
		else
			return false;
	}

	template <typename T>
	static bool executeNumber(Block & block, const ColumnPtr & column, size_t result)
	{
		if (const ColumnVector<T> * col = typeid_cast<const ColumnVector<T> *>(&*column))
		{
			ColumnUInt64 * res = new ColumnUInt64(column->size());
			block.getByPosition(result).column = res;
			numWidthVector(col->getData(), res->getData());
			return true;
		}
		else
			return false;
	}
}


void FunctionVisibleWidth::execute(Block & block, const ColumnNumbers & arguments, size_t result)
{
	const ColumnPtr column = block.getByPosition(arguments[0]).column;
	const DataTypePtr type = block.getByPosition(arguments[0]).type;
	size_t rows = column->size();

	if (typeid_cast<const DataTypeDate *>(&*type))
	{
		block.getByPosition(result).column = new ColumnConstUInt64(rows, strlen("0000-00-00"));
	}
	else if (typeid_cast<const DataTypeDateTime *>(&*type))
	{
		block.getByPosition(result).column = new ColumnConstUInt64(rows, strlen("0000-00-00 00:00:00"));
	}
	else if (VisibleWidth::executeConstNumber<UInt8>(block, column, result)
		|| VisibleWidth::executeConstNumber<UInt16>(block, column, result)
		|| VisibleWidth::executeConstNumber<UInt32>(block, column, result)
		|| VisibleWidth::executeConstNumber<UInt64>(block, column, result)
		|| VisibleWidth::executeConstNumber<Int8>(block, column, result)
		|| VisibleWidth::executeConstNumber<Int16>(block, column, result)
		|| VisibleWidth::executeConstNumber<Int32>(block, column, result)
		|| VisibleWidth::executeConstNumber<Int64>(block, column, result)
		|| VisibleWidth::executeConstNumber<Float32>(block, column, result)	/// TODO: правильная работа с float
		|| VisibleWidth::executeConstNumber<Float64>(block, column, result)
		|| VisibleWidth::executeNumber<UInt8>(block, column, result)
		|| VisibleWidth::executeNumber<UInt16>(block, column, result)
		|| VisibleWidth::executeNumber<UInt32>(block, column, result)
		|| VisibleWidth::executeNumber<UInt64>(block, column, result)
		|| VisibleWidth::executeNumber<Int8>(block, column, result)
		|| VisibleWidth::executeNumber<Int16>(block, column, result)
		|| VisibleWidth::executeNumber<Int32>(block, column, result)
		|| VisibleWidth::executeNumber<Int64>(block, column, result)
		|| VisibleWidth::executeNumber<Float32>(block, column, result)
		|| VisibleWidth::executeNumber<Float64>(block, column, result))
	{
	}
	else if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
	{
		ColumnUInt64 * res = new ColumnUInt64(rows);
		block.getByPosition(result).column = res;
		stringWidthVector(col->getChars(), col->getOffsets(), res->getData());
	}
	else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(&*column))
	{
		ColumnUInt64 * res = new ColumnUInt64(rows);
		block.getByPosition(result).column = res;
		stringWidthFixedVector(col->getChars(), col->getN(), res->getData());
	}
	else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
	{
		UInt64 res = 0;
		block.getByPosition(result).column = new ColumnConstUInt64(rows, res);
		stringWidthConstant(col->getData(), res);
	}
	else if (const ColumnArray * col = typeid_cast<const ColumnArray *>(&*column))
	{
		/// Вычисляем видимую ширину для значений массива.
		Block nested_block;
		ColumnWithNameAndType nested_values;
		nested_values.type = typeid_cast<const DataTypeArray &>(*type).getNestedType();
		nested_values.column = col->getDataPtr();
		nested_block.insert(nested_values);

		ColumnWithNameAndType nested_result;
		nested_result.type = new DataTypeUInt64;
		nested_block.insert(nested_result);

		ColumnNumbers nested_argument_numbers(1, 0);
		execute(nested_block, nested_argument_numbers, 1);

		/// Теперь суммируем и кладём в результат.
		ColumnUInt64 * res = new ColumnUInt64(rows);
		block.getByPosition(result).column = res;
		ColumnUInt64::Container_t & vec = res->getData();

		size_t additional_symbols = 0;	/// Кавычки.
		if (typeid_cast<const DataTypeDate *>(&*nested_values.type)
			|| typeid_cast<const DataTypeDateTime *>(&*nested_values.type)
			|| typeid_cast<const DataTypeString *>(&*nested_values.type)
			|| typeid_cast<const DataTypeFixedString *>(&*nested_values.type))
			additional_symbols = 2;

		if (ColumnUInt64 * nested_result_column = typeid_cast<ColumnUInt64 *>(&*nested_block.getByPosition(1).column))
		{
			ColumnUInt64::Container_t & nested_res = nested_result_column->getData();

			size_t j = 0;
			for (size_t i = 0; i < rows; ++i)
			{
				/** Если пустой массив - то два символа: [];
				  * если непустой - то сначала один символ [, и по одному лишнему символу на значение: , или ].
				  */
				vec[i] = j == col->getOffsets()[i] ? 2 : 1;

				for (; j < col->getOffsets()[i]; ++j)
					vec[i] += 1 + additional_symbols + nested_res[j];
			}
		}
		else if (ColumnConstUInt64 * nested_result_column = typeid_cast<ColumnConstUInt64 *>(&*nested_block.getByPosition(1).column))
		{
			size_t nested_length = nested_result_column->getData() + additional_symbols + 1;
			for (size_t i = 0; i < rows; ++i)
				vec[i] = 1 + std::max(static_cast<size_t>(1),
					(i == 0 ? col->getOffsets()[0] : (col->getOffsets()[i] - col->getOffsets()[i - 1])) * nested_length);
		}
	}
	else if (const ColumnTuple * col = typeid_cast<const ColumnTuple *>(&*column))
	{
		/// Посчитаем видимую ширину для каждого вложенного столбца по отдельности, и просуммируем.
		Block nested_block = col->getData();
		size_t columns = nested_block.columns();

		FunctionPlus func_plus;

		for (size_t i = 0; i < columns; ++i)
		{
			nested_block.getByPosition(i).type = static_cast<const DataTypeTuple &>(*type).getElements()[i];

			/** nested_block будет состоять из следующих столбцов:
			  * x1, x2, x3... , width1, width2, width1 + width2, width3, width1 + width2 + width3, ...
			  */

			ColumnWithNameAndType nested_result;
			nested_result.type = new DataTypeUInt64;
			nested_block.insert(nested_result);

			ColumnNumbers nested_argument_numbers(1, i);
			execute(nested_block, nested_argument_numbers, nested_block.columns() - 1);

			if (i != 0)
			{
				ColumnWithNameAndType plus_result;
				plus_result.type = new DataTypeUInt64;
				nested_block.insert(plus_result);

				ColumnNumbers plus_argument_numbers(2);
				plus_argument_numbers[0] = nested_block.columns() - 3;
				plus_argument_numbers[1] = nested_block.columns() - 2;
				func_plus.execute(nested_block, plus_argument_numbers, nested_block.columns() - 1);
			}
		}

		/// Прибавим ещё количество символов на кавычки и запятые.

		size_t additional_symbols = columns - 1;	/// Запятые.
		for (size_t i = 0; i < columns; ++i)
		{
			if (typeid_cast<const DataTypeDate *>(&*nested_block.getByPosition(i).type)
				|| typeid_cast<const DataTypeDateTime *>(&*nested_block.getByPosition(i).type)
				|| typeid_cast<const DataTypeString *>(&*nested_block.getByPosition(i).type)
				|| typeid_cast<const DataTypeFixedString *>(&*nested_block.getByPosition(i).type))
				additional_symbols += 2;			/// Кавычки.
		}

		ColumnPtr & nested_result_column = nested_block.getByPosition(nested_block.columns() - 1).column;

		if (nested_result_column->isConst())
		{
			ColumnConstUInt64 & nested_result_column_const = typeid_cast<ColumnConstUInt64 &>(*nested_result_column);
			if (nested_result_column_const.size())
				nested_result_column_const.getData() += 2 + additional_symbols;
		}
		else
		{
			ColumnUInt64 & nested_result_column_vec = typeid_cast<ColumnUInt64 &>(*nested_result_column);
			ColumnUInt64::Container_t & nested_res = nested_result_column_vec.getData();

			for (size_t i = 0; i < rows; ++i)
				nested_res[i] += 2 + additional_symbols;
		}

		block.getByPosition(result).column = nested_result_column;
	}
	else if (const ColumnConstArray * col = typeid_cast<const ColumnConstArray *>(&*column))
	{
		String s;
		{
			WriteBufferFromString wb(s);
			type->serializeTextEscaped(col->getData(), wb);
		}

		block.getByPosition(result).column = new ColumnConstUInt64(rows, s.size());
	}
	else
	   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
}


/// TODO: Убрать copy-paste из FunctionsConditional.h
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

template <typename T1, typename T2>
DataTypePtr getSmallestCommonNumericTypeImpl()
{
	using ResultType = typename NumberTraits::ResultOfIf<T1, T2>::Type;
	auto type_res = DataTypeFromFieldTypeOrError<ResultType>::getDataType();
	if (!type_res)
		throw Exception("Types " + TypeName<T1>::get() + " and " + TypeName<T2>::get()
			+ " are not upscalable to a common type without loss of precision", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	return type_res;
}

template <typename T1>
DataTypePtr getSmallestCommonNumericTypeLeft(const IDataType & t2)
{
	if (typeid_cast<const DataTypeUInt8 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, UInt8>();
	if (typeid_cast<const DataTypeUInt16 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, UInt16>();
	if (typeid_cast<const DataTypeUInt32 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, UInt32>();
	if (typeid_cast<const DataTypeUInt64 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, UInt64>();
	if (typeid_cast<const DataTypeInt8 *>(&t2))		return getSmallestCommonNumericTypeImpl<T1, Int8>();
	if (typeid_cast<const DataTypeInt16 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Int16>();
	if (typeid_cast<const DataTypeInt32 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Int32>();
	if (typeid_cast<const DataTypeInt64 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Int64>();
	if (typeid_cast<const DataTypeFloat32 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Float32>();
	if (typeid_cast<const DataTypeFloat64 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Float64>();

	throw Exception("Logical error: not a numeric type passed to function getSmallestCommonNumericType", ErrorCodes::LOGICAL_ERROR);
}

DataTypePtr getSmallestCommonNumericType(const IDataType & t1, const IDataType & t2)
{
	if (typeid_cast<const DataTypeUInt8 *>(&t1))	return getSmallestCommonNumericTypeLeft<UInt8>(t2);
	if (typeid_cast<const DataTypeUInt16 *>(&t1))	return getSmallestCommonNumericTypeLeft<UInt16>(t2);
	if (typeid_cast<const DataTypeUInt32 *>(&t1))	return getSmallestCommonNumericTypeLeft<UInt32>(t2);
	if (typeid_cast<const DataTypeUInt64 *>(&t1))	return getSmallestCommonNumericTypeLeft<UInt64>(t2);
	if (typeid_cast<const DataTypeInt8 *>(&t1))		return getSmallestCommonNumericTypeLeft<Int8>(t2);
	if (typeid_cast<const DataTypeInt16 *>(&t1))	return getSmallestCommonNumericTypeLeft<Int16>(t2);
	if (typeid_cast<const DataTypeInt32 *>(&t1))	return getSmallestCommonNumericTypeLeft<Int32>(t2);
	if (typeid_cast<const DataTypeInt64 *>(&t1))	return getSmallestCommonNumericTypeLeft<Int64>(t2);
	if (typeid_cast<const DataTypeFloat32 *>(&t1))	return getSmallestCommonNumericTypeLeft<Float32>(t2);
	if (typeid_cast<const DataTypeFloat64 *>(&t1))	return getSmallestCommonNumericTypeLeft<Float64>(t2);

	throw Exception("Logical error: not a numeric type passed to function getSmallestCommonNumericType", ErrorCodes::LOGICAL_ERROR);
}

}


namespace DB
{

void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
	factory.registerFunction<FunctionCurrentDatabase>();
	factory.registerFunction<FunctionHostName>();
	factory.registerFunction<FunctionVisibleWidth>();
	factory.registerFunction<FunctionToTypeName>();
	factory.registerFunction<FunctionBlockSize>();
	factory.registerFunction<FunctionSleep>();
	factory.registerFunction<FunctionMaterialize>();
	factory.registerFunction<FunctionIgnore>();
	factory.registerFunction<FunctionArrayJoin>();
	factory.registerFunction<FunctionBar>();

	factory.registerFunction<FunctionTuple>();
	factory.registerFunction<FunctionTupleElement>();
	factory.registerFunction<FunctionIn<false, false>>();
	factory.registerFunction<FunctionIn<false, true>>();
	factory.registerFunction<FunctionIn<true, false>>();
	factory.registerFunction<FunctionIn<true, true>>();

	factory.registerFunction<FunctionIsFinite>();
	factory.registerFunction<FunctionIsInfinite>();
	factory.registerFunction<FunctionIsNaN>();

	factory.registerFunction<FunctionTransform>();
}

}
