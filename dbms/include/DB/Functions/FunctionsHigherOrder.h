#pragma once

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeExpression.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnExpression.h>

#include <DB/Functions/IFunction.h>
#include <DB/Functions/FunctionsMiscellaneous.h>


namespace DB
{

/** Функции высшего порядка для массивов:
  *
  * arrayMap(x1,...,xn -> expression, array1,...,arrayn) - применить выражение к каждому элементу массива (или набора параллельных массивов).
  * arrayFilter(x -> predicate, array) - оставить в массиве только элементы, для которых выражение истинно.
  * arrayCount(x1,...,xn -> expression, array1,...,arrayn) - для скольки элементов массива выражение истинно.
  * arrayExists(x1,...,xn -> expression, array1,...,arrayn) - истинно ли выражение для хотя бы одного элемента массива.
  * arrayAll(x1,...,xn -> expression, array1,...,arrayn) - истинно ли выражение для всех элементов массива.
  *
  * Для функций arrayCount, arrayExists, arrayAll доступна еще перегрузка вида f(array), которая работает так же, как f(x -> x, array).
  */

struct ArrayMapImpl
{
	/// true, если выражение (для перегрузки f(expression, arrays)) или массив (для f(array)) должно быть булевым.
	static bool needBoolean() { return false; }
	/// true, если перегрузка f(array) недоступна.
	static bool needExpression() { return true; }
	/// true, если массив должен быть ровно один.
	static bool needOneArray() { return false; }

	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeArray(expression_return);
	}

	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		return mapped->isConst()
			? new ColumnArray(dynamic_cast<const IColumnConst &>(*mapped).convertToFullColumn(), array->getOffsetsColumn())
			: new ColumnArray(mapped, array->getOffsetsColumn());
	}
};

struct ArrayFilterImpl
{
	static bool needBoolean() { return true; }
	static bool needExpression() { return true; }
	static bool needOneArray() { return false; }

	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeArray(array_element);
	}

	/// Если массивов несколько, сюда передается первый.
	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		const ColumnVector<UInt8> * column_filter = typeid_cast<const ColumnVector<UInt8> *>(&*mapped);

		if (!column_filter)
		{
			const ColumnConstUInt8 * column_filter_const = typeid_cast<const ColumnConstUInt8 *>(&*mapped);

			if (!column_filter_const)
				throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

			if (column_filter_const->getData())
				return array->clone();
			else
				return new ColumnArray(array->getDataPtr()->cloneEmpty(), new ColumnArray::ColumnOffsets_t(array->size(), 0));
		}

		const IColumn::Filter & filter = column_filter->getData();
		ColumnPtr filtered = array->getData().filter(filter, -1);

		const IColumn::Offsets_t & in_offsets = array->getOffsets();
		ColumnArray::ColumnOffsets_t * column_offsets = new ColumnArray::ColumnOffsets_t(in_offsets.size());
		ColumnPtr column_offsets_ptr = column_offsets;
		IColumn::Offsets_t & out_offsets = column_offsets->getData();

		size_t in_pos = 0;
		size_t out_pos = 0;
		for (size_t i = 0; i < in_offsets.size(); ++i)
		{
			for (; in_pos < in_offsets[i]; ++in_pos)
			{
				if (filter[in_pos])
					++out_pos;
			}
			out_offsets[i] = out_pos;
		}

		return new ColumnArray(filtered, column_offsets_ptr);
	}
};

struct ArrayCountImpl
{
	static bool needBoolean() { return true; }
	static bool needExpression() { return false; }
	static bool needOneArray() { return false; }

	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeUInt32;
	}

	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		const ColumnVector<UInt8> * column_filter = typeid_cast<const ColumnVector<UInt8> *>(&*mapped);

		if (!column_filter)
		{
			const ColumnConstUInt8 * column_filter_const = typeid_cast<const ColumnConstUInt8 *>(&*mapped);

			if (!column_filter_const)
				throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

			if (column_filter_const->getData())
			{
				const IColumn::Offsets_t & offsets = array->getOffsets();
				ColumnVector<UInt32> * out_column = new ColumnVector<UInt32>(offsets.size());
				ColumnPtr out_column_ptr = out_column;
				ColumnVector<UInt32>::Container_t & out_counts = out_column->getData();

				size_t pos = 0;
				for (size_t i = 0; i < offsets.size(); ++i)
				{
					out_counts[i] = offsets[i] - pos;
					pos = offsets[i];
				}

				return out_column_ptr;
			}
			else
				return new ColumnConstUInt32(array->size(), 0);
		}

		const IColumn::Filter & filter = column_filter->getData();
		const IColumn::Offsets_t & offsets = array->getOffsets();
		ColumnVector<UInt32> * out_column = new ColumnVector<UInt32>(offsets.size());
		ColumnPtr out_column_ptr = out_column;
		ColumnVector<UInt32>::Container_t & out_counts = out_column->getData();

		size_t pos = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			size_t count = 0;
			for (; pos < offsets[i]; ++pos)
			{
				if (filter[pos])
					++count;
			}
			out_counts[i] = count;
		}

		return out_column_ptr;
	}
};

struct ArrayExistsImpl
{
	static bool needBoolean() { return true; }
	static bool needExpression() { return false; }
	static bool needOneArray() { return false; }

	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeUInt8;
	}

	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		const ColumnVector<UInt8> * column_filter = typeid_cast<const ColumnVector<UInt8> *>(&*mapped);

		if (!column_filter)
		{
			const ColumnConstUInt8 * column_filter_const = typeid_cast<const ColumnConstUInt8 *>(&*mapped);

			if (!column_filter_const)
				throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

			if (column_filter_const->getData())
			{
				const IColumn::Offsets_t & offsets = array->getOffsets();
				ColumnVector<UInt8> * out_column = new ColumnVector<UInt8>(offsets.size());
				ColumnPtr out_column_ptr = out_column;
				ColumnVector<UInt8>::Container_t & out_exists = out_column->getData();

				size_t pos = 0;
				for (size_t i = 0; i < offsets.size(); ++i)
				{
					out_exists[i] = offsets[i] - pos > 0;
					pos = offsets[i];
				}

				return out_column_ptr;
			}
			else
				return new ColumnConstUInt8(array->size(), 0);
		}

		const IColumn::Filter & filter = column_filter->getData();
		const IColumn::Offsets_t & offsets = array->getOffsets();
		ColumnVector<UInt8> * out_column = new ColumnVector<UInt8>(offsets.size());
		ColumnPtr out_column_ptr = out_column;
		ColumnVector<UInt8>::Container_t & out_exists = out_column->getData();

		size_t pos = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			UInt8 exists = 0;
			for (; pos < offsets[i]; ++pos)
			{
				if (filter[pos])
				{
					exists = 1;
					pos = offsets[i];
					break;
				}
			}
			out_exists[i] = exists;
		}

		return out_column_ptr;
	}
};

struct ArrayAllImpl
{
	static bool needBoolean() { return true; }
	static bool needExpression() { return false; }
	static bool needOneArray() { return false; }

	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeUInt8;
	}

	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		const ColumnVector<UInt8> * column_filter = typeid_cast<const ColumnVector<UInt8> *>(&*mapped);

		if (!column_filter)
		{
			const ColumnConstUInt8 * column_filter_const = typeid_cast<const ColumnConstUInt8 *>(&*mapped);

			if (!column_filter_const)
				throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

			if (column_filter_const->getData())
				return new ColumnConstUInt8(array->size(), 1);
			else
			{
				const IColumn::Offsets_t & offsets = array->getOffsets();
				ColumnVector<UInt8> * out_column = new ColumnVector<UInt8>(offsets.size());
				ColumnPtr out_column_ptr = out_column;
				ColumnVector<UInt8>::Container_t & out_all = out_column->getData();

				size_t pos = 0;
				for (size_t i = 0; i < offsets.size(); ++i)
				{
					out_all[i] = offsets[i] == pos;
					pos = offsets[i];
				}

				return out_column_ptr;
			}
		}

		const IColumn::Filter & filter = column_filter->getData();
		const IColumn::Offsets_t & offsets = array->getOffsets();
		ColumnVector<UInt8> * out_column = new ColumnVector<UInt8>(offsets.size());
		ColumnPtr out_column_ptr = out_column;
		ColumnVector<UInt8>::Container_t & out_all = out_column->getData();

		size_t pos = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			UInt8 all = 1;
			for (; pos < offsets[i]; ++pos)
			{
				if (!filter[pos])
				{
					all = 0;
					pos = offsets[i];
					break;
				}
			}
			out_all[i] = all;
		}

		return out_column_ptr;
	}
};

struct ArraySumImpl
{
	static bool needBoolean() { return false; }
	static bool needExpression() { return false; }
	static bool needOneArray() { return false; }

	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		if (typeid_cast<const DataTypeUInt8 *>(&*expression_return) ||
			typeid_cast<const DataTypeUInt16 *>(&*expression_return) ||
			typeid_cast<const DataTypeUInt32 *>(&*expression_return) ||
			typeid_cast<const DataTypeUInt64 *>(&*expression_return))
			return new DataTypeUInt64;

		if (typeid_cast<const DataTypeInt8 *>(&*expression_return) ||
			typeid_cast<const DataTypeInt16 *>(&*expression_return) ||
			typeid_cast<const DataTypeInt32 *>(&*expression_return) ||
			typeid_cast<const DataTypeInt64 *>(&*expression_return))
			return new DataTypeInt64;

		if (typeid_cast<const DataTypeFloat32 *>(&*expression_return) ||
			typeid_cast<const DataTypeFloat64 *>(&*expression_return))
			return new DataTypeFloat64;

		throw Exception("arraySum cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	template <class Element, class Result>
	static bool executeType(const ColumnPtr & mapped, const ColumnArray::Offsets_t & offsets, ColumnPtr & res_ptr)
	{
		const ColumnVector<Element> * column = typeid_cast<const ColumnVector<Element> *>(&*mapped);

		if (!column)
		{
			const ColumnConst<Element> * column_const = typeid_cast<const ColumnConst<Element> *>(&*mapped);

			if (!column_const)
				return false;

			const Element x = column_const->getData();

			ColumnVector<Result> * res_column = new ColumnVector<Result>(offsets.size());
			res_ptr = res_column;
			typename ColumnVector<Result>::Container_t & res = res_column->getData();

			size_t pos = 0;
			for (size_t i = 0; i < offsets.size(); ++i)
			{
				res[i] = x * (offsets[i] - pos);
				pos = offsets[i];
			}

			return true;
		}

		const typename ColumnVector<Element>::Container_t & data = column->getData();
		ColumnVector<Result> * res_column = new ColumnVector<Result>(offsets.size());
		res_ptr = res_column;
		typename ColumnVector<Result>::Container_t & res = res_column->getData();

		size_t pos = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			Result s = 0;
			for (; pos < offsets[i]; ++pos)
			{
				s += data[pos];
			}
			res[i] = s;
		}

		return true;
	}

	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		const IColumn::Offsets_t & offsets = array->getOffsets();
		ColumnPtr res;

		if (executeType< UInt8 , UInt64>(mapped, offsets, res) ||
			executeType< UInt16, UInt64>(mapped, offsets, res) ||
			executeType< UInt32, UInt64>(mapped, offsets, res) ||
			executeType< UInt64, UInt64>(mapped, offsets, res) ||
			executeType<  Int8 ,  Int64>(mapped, offsets, res) ||
			executeType<  Int16,  Int64>(mapped, offsets, res) ||
			executeType<  Int32,  Int64>(mapped, offsets, res) ||
			executeType<  Int64,  Int64>(mapped, offsets, res) ||
			executeType<Float32,Float64>(mapped, offsets, res) ||
			executeType<Float64,Float64>(mapped, offsets, res))
			return res;
		else
			throw Exception("Unexpected column for arraySum: " + mapped->getName());
	}
};

struct ArrayFirstImpl
{
	static bool needBoolean() { return false; }
	static bool needExpression() { return true; }
	static bool needOneArray() { return false; }

	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return array_element;
	}

	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		auto column_filter = typeid_cast<const ColumnVector<UInt8> *>(&*mapped);

		if (!column_filter)
		{
			const ColumnConstUInt8 * column_filter_const = typeid_cast<const ColumnConstUInt8 *>(&*mapped);

			if (!column_filter_const)
				throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

			if (column_filter_const->getData())
			{
				const auto & offsets = array->getOffsets();
				const auto & data = array->getData();
				ColumnPtr out{data.cloneEmpty()};

				size_t pos{};
				for (size_t i = 0; i < offsets.size(); ++i)
				{
					if (offsets[i] - pos > 0)
						out->insert(data[pos]);
					else
						out->insertDefault();

					pos = offsets[i];
				}

				return out;
			}
			else
			{
				ColumnPtr out{array->getData().cloneEmpty()};
				out->insertDefault();
				return out->replicate(IColumn::Offsets_t(1, array->size()));
			}
		}

		const auto & filter = column_filter->getData();
		const auto & offsets = array->getOffsets();
		const auto & data = array->getData();
		ColumnPtr out{data.cloneEmpty()};

		size_t pos{};
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			auto exists = false;
			for (; pos < offsets[i]; ++pos)
			{
				if (filter[pos])
				{
					out->insert(data[pos]);
					exists = true;
					pos = offsets[i];
					break;
				}
			}

			if (!exists)
				out->insertDefault();
		}

		return out;
	}
};

struct ArrayFirstIndexImpl
{
	static bool needBoolean() { return false; }
	static bool needExpression() { return true; }
	static bool needOneArray() { return false; }

	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeUInt32;
	}

	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		auto column_filter = typeid_cast<const ColumnVector<UInt8> *>(&*mapped);

		if (!column_filter)
		{
			const ColumnConstUInt8 * column_filter_const = typeid_cast<const ColumnConstUInt8 *>(&*mapped);

			if (!column_filter_const)
				throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

			if (column_filter_const->getData())
			{
				const auto & offsets = array->getOffsets();
				auto out_column = new ColumnVector<UInt32>{offsets.size()};
				ColumnPtr out_column_ptr{out_column};
				auto & out_index = out_column->getData();

				size_t pos{};
				for (size_t i = 0; i < offsets.size(); ++i)
				{
					out_index[i] = offsets[i] - pos > 0;
					pos = offsets[i];
				}

				return out_column_ptr;
			}
			else
				return new ColumnConstUInt32(array->size(), 0);
		}

		const auto & filter = column_filter->getData();
		const auto & offsets = array->getOffsets();
		auto out_column = new ColumnVector<UInt32>{offsets.size()};
		ColumnPtr out_column_ptr{out_column};
		auto & out_index = out_column->getData();

		size_t pos{};
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			UInt32 index{};
			for (size_t idx{1}; pos < offsets[i]; ++pos, ++idx)
			{
				if (filter[pos])
				{
					index = idx;
					pos = offsets[i];
					break;
				}
			}

			out_index[i] = index;
		}

		return out_column_ptr;
	}
};

template <typename Impl, typename Name>
class FunctionArrayMapped : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionArrayMapped; };

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Вызывается, если хоть один агрумент функции - лямбда-выражение.
	/// Для аргументов-лямбда-выражений определяет типы аргументов этих выражений.
	void getLambdaArgumentTypes(DataTypes & arguments) const
	{
		if (arguments.size() < 1)
			throw Exception("Function " + getName() + " needs at least one argument; passed "
							+ toString(arguments.size()) + ".",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (arguments.size() == 1)
			throw Exception("Function " + getName() + " needs at least one array argument.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		DataTypes nested_types(arguments.size() - 1);
		for (size_t i = 0; i < nested_types.size(); ++i)
		{
			const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[i + 1]);
			if (!array_type)
				throw Exception("Argument " + toString(i + 2) + " of function " + getName() + " must be array. Found "
								+ arguments[i + 1]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
			nested_types[i] = array_type->getNestedType();
		}

		const DataTypeExpression * expression_type = typeid_cast<const DataTypeExpression *>(&*arguments[0]);
		if (!expression_type || expression_type->getArgumentTypes().size() != nested_types.size())
			throw Exception("First argument for this overload of " + getName() + " must be an expression with "
							+ toString(nested_types.size()) + " arguments. Found "
							+ arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		arguments[0] = new DataTypeExpression(nested_types);
	}

	void getReturnTypeAndPrerequisites(const ColumnsWithTypeAndName & arguments,
										DataTypePtr & out_return_type,
										ExpressionActions::Actions & out_prerequisites) override
	{
		size_t min_args = Impl::needExpression() ? 2 : 1;
		if (arguments.size() < min_args)
			throw Exception("Function " + getName() + " needs at least "
							+ toString(min_args) + " argument; passed "
							+ toString(arguments.size()) + ".",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (arguments.size() == 1)
		{
			const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[0].type);

			if (!array_type)
				throw Exception("The only argument for function " + getName() + " must be array. Found "
								+ arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			DataTypePtr nested_type = array_type->getNestedType();

			if (Impl::needBoolean() && !typeid_cast<const DataTypeUInt8 *>(&*nested_type))
				throw Exception("The only argument for function " + getName() + " must be array of UInt8. Found "
								+ arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			out_return_type = Impl::getReturnType(nested_type, nested_type);
		}
		else
		{
			if (arguments.size() > 2 && Impl::needOneArray())
				throw Exception("Function " + getName() + " needs one array argument.",
								ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

			const ColumnExpression * column_expression = typeid_cast<const ColumnExpression *>(arguments[0].column.get());

			if (!column_expression)
				throw Exception("First argument for function " + getName() + " must be an expression.",
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			/// Типы остальных аргументов уже проверены в getLambdaArgumentTypes.

			/// Попросим добавить в блок все столбцы, упоминаемые в выражении, размноженные в массив, параллельный обрабатываемому.
			const ExpressionActions & expression = *column_expression->getExpression();
			Names required_columns = expression.getRequiredColumns();

			Names argument_name_vector = column_expression->getArgumentNames();
			NameSet argument_names(argument_name_vector.begin(), argument_name_vector.end());

			for (size_t i = 0; i < required_columns.size(); ++i)
			{
				if (argument_names.count(required_columns[i]))
					continue;
				Names replicate_arguments;
				replicate_arguments.push_back(required_columns[i]);
				replicate_arguments.push_back(arguments[1].name);
				out_prerequisites.push_back(ExpressionAction::applyFunction(new FunctionReplicate, replicate_arguments));
			}

			DataTypePtr return_type = column_expression->getReturnType();
			if (Impl::needBoolean() && !typeid_cast<const DataTypeUInt8 *>(&*return_type))
				throw Exception("Expression for function " + getName() + " must return UInt8, found "
								+ return_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const DataTypeArray * first_array_type = typeid_cast<const DataTypeArray *>(&*arguments[1].type);

			out_return_type = Impl::getReturnType(return_type, first_array_type->getNestedType());
		}
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result)
	{
		if (arguments.size() == 1)
		{
			ColumnPtr column_array_ptr = block.getByPosition(arguments[0]).column;
			const ColumnArray * column_array = typeid_cast<const ColumnArray *>(&*column_array_ptr);

			if (!column_array)
			{
				const ColumnConstArray * column_const_array = typeid_cast<const ColumnConstArray *>(&*column_array_ptr);
				if (!column_const_array)
					throw Exception("Expected array column, found " + column_array_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
				column_array_ptr = column_const_array->convertToFullColumn();
				column_array = typeid_cast<const ColumnArray *>(&*column_array_ptr);
			}

			block.getByPosition(result).column = Impl::execute(column_array, column_array->getDataPtr());
		}
		else
		{
			ColumnExpression * column_expression = typeid_cast<ColumnExpression *>(&*block.getByPosition(arguments[0]).column);

			ColumnPtr offsets_column;

			Block temp_block;
			const ExpressionActions & expression = *column_expression->getExpression();
			NamesAndTypes expression_arguments = column_expression->getArguments();
			NameSet argument_names;

			ColumnPtr column_first_array_ptr;
			const ColumnArray * column_first_array = nullptr;

			/// Положим в блок аргументы выражения.

			for (size_t i = 0; i < expression_arguments.size(); ++i)
			{
				const std::string & argument_name = expression_arguments[i].name;
				DataTypePtr argument_type = expression_arguments[i].type;

				ColumnPtr column_array_ptr = block.getByPosition(arguments[i + 1]).column;
				const ColumnArray * column_array = typeid_cast<const ColumnArray *>(&*column_array_ptr);

				if (!column_array)
				{
					const ColumnConstArray * column_const_array = typeid_cast<const ColumnConstArray *>(&*column_array_ptr);
					if (!column_const_array)
						throw Exception("Expected array column, found " + column_array_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
					column_array_ptr = column_const_array->convertToFullColumn();
					column_array = typeid_cast<const ColumnArray *>(&*column_array_ptr);
				}

				if (!offsets_column)
				{
					offsets_column = column_array->getOffsetsColumn();
				}
				else
				{
					/// Первое условие - оптимизация: не сравнивать данные, если указатели равны.
					if (column_array->getOffsetsColumn() != offsets_column
						&& column_array->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets_t &>(*offsets_column).getData())
						throw Exception("Arrays passed to " + getName() + " must have equal size", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
				}

				if (i == 0)
				{
					column_first_array_ptr = column_array_ptr;
					column_first_array = column_array;
				}

				temp_block.insert(ColumnWithTypeAndName(
					column_array->getDataPtr(),
					argument_type,
					argument_name));

				argument_names.insert(argument_name);
			}

			/// Положим в блок все нужные столбцы, размноженные по размерам массивов.

			Names required_columns = expression.getRequiredColumns();
			size_t prerequisite_index = 0;

			for (size_t i = 0; i < required_columns.size(); ++i)
			{
				const String & name = required_columns[i];

				if (argument_names.count(name))
					continue;

				ColumnWithTypeAndName replicated_column = block.getByPosition(prerequisites[prerequisite_index]);

				replicated_column.name = name;
				replicated_column.column = typeid_cast<ColumnArray &>(*replicated_column.column).getDataPtr();
				replicated_column.type = typeid_cast<const DataTypeArray &>(*replicated_column.type).getNestedType(),
				temp_block.insert(replicated_column);

				++prerequisite_index;
			}

			expression.execute(temp_block);

			block.getByPosition(result).column = Impl::execute(column_first_array, temp_block.getByName(column_expression->getReturnName()).column);
		}
	}
};


struct NameArrayMap			{ static constexpr auto name = "arrayMap"; };
struct NameArrayFilter		{ static constexpr auto name = "arrayFilter"; };
struct NameArrayCount		{ static constexpr auto name = "arrayCount"; };
struct NameArrayExists		{ static constexpr auto name = "arrayExists"; };
struct NameArrayAll			{ static constexpr auto name = "arrayAll"; };
struct NameArraySum			{ static constexpr auto name = "arraySum"; };
struct NameArrayFirst		{ static constexpr auto name = "arrayFirst"; };
struct NameArrayFirstIndex	{ static constexpr auto name = "arrayFirstIndex"; };

typedef FunctionArrayMapped<ArrayMapImpl, 		NameArrayMap>		FunctionArrayMap;
typedef FunctionArrayMapped<ArrayFilterImpl, 	NameArrayFilter>	FunctionArrayFilter;
typedef FunctionArrayMapped<ArrayCountImpl, 	NameArrayCount>		FunctionArrayCount;
typedef FunctionArrayMapped<ArrayExistsImpl, 	NameArrayExists>	FunctionArrayExists;
typedef FunctionArrayMapped<ArrayAllImpl,	 	NameArrayAll>		FunctionArrayAll;
typedef FunctionArrayMapped<ArraySumImpl,	 	NameArraySum>		FunctionArraySum;
typedef FunctionArrayMapped<ArrayFirstImpl,	 	NameArrayFirst>		FunctionArrayFirst;
typedef FunctionArrayMapped<ArrayFirstIndexImpl,	 	NameArrayFirstIndex>		FunctionArrayFirstIndex;

}
