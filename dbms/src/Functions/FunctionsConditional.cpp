#include <DB/Functions/FunctionsConditional.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/Conditional/common.h>
#include <DB/Functions/Conditional/ArgsInfo.h>
#include <DB/Functions/Conditional/CondSource.h>
#include <DB/Functions/Conditional/NumericPerformer.h>
#include <DB/Functions/Conditional/StringEvaluator.h>
#include <DB/Functions/Conditional/StringArrayEvaluator.h>
#include <DB/Columns/ColumnNullable.h>

namespace DB
{

namespace
{

/// Check whether at least one of the specified columns is either nullable
/// or null inside its block.
bool hasNullableBranches(const Block & block, const ColumnNumbers & args)
{
	auto check = [](const Block & block, size_t arg)
	{
		const auto & elem = block.unsafeGetByPosition(arg);
		return (elem.column && (elem.column.get()->isNullable() || elem.column.get()->isNull()));
	};

	size_t else_arg = Conditional::elseArg(args);
	for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
	{
		if (check(block, args[i]))
			return true;
	}

	if (check(block, args[else_arg]))
		return true;

	return false;
}

bool hasNullableArgs(const DataTypes & args)
{
	size_t else_arg = Conditional::elseArg(args);

	for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
	{
		if (args[i].get()->isNullable())
			return true;
	}

	return args[else_arg].get()->isNullable();
}

bool hasNullArgs(const DataTypes & args)
{
	size_t else_arg = Conditional::elseArg(args);

	for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
	{
		if (args[i].get()->isNull())
			return true;
	}

	return args[else_arg].get()->isNull();
}

}

void registerFunctionsConditional(FunctionFactory & factory)
{
	factory.registerFunction<FunctionIf>();
	factory.registerFunction<FunctionMultiIf>();
}

FunctionPtr FunctionMultiIf::create(const Context & context)
{
	return std::make_shared<FunctionMultiIf>();
}

String FunctionMultiIf::getName() const
{
	return is_case_mode ? "CASE" : name;
}

bool FunctionMultiIf::hasSpecialSupportForNulls() const
{
	return true;
}

void FunctionMultiIf::setCaseMode()
{
	is_case_mode = true;
}

DataTypePtr FunctionMultiIf::getReturnTypeImpl(const DataTypes & args) const
{
	DataTypePtr data_type;

	try
	{
		data_type = getReturnTypeInternal(args);
	}
	catch (const Conditional::CondException & ex)
	{
		rethrowContextually(ex);
	}

	return data_type;
}

void FunctionMultiIf::executeImpl(Block & block, const ColumnNumbers & args, size_t result)
{
	auto perform_multi_if = [&](Block & block, const ColumnNumbers & args, size_t result, size_t tracker)
	{
		if (performTrivialCase(block, args, result, tracker))
			return;
		if (Conditional::NumericPerformer::perform(block, args, result, tracker))
			return;
		if (Conditional::StringEvaluator::perform(block, args, result, tracker))
			return;
		if (Conditional::StringArrayEvaluator::perform(block, args, result, tracker))
			return;

		if (is_case_mode)
			throw Exception{"Some THEN/ELSE clauses in CASE construction have "
				"illegal or incompatible types", ErrorCodes::ILLEGAL_COLUMN};
		else
			throw Exception{"One or more branch (then, else) columns of function "
				+ getName() + " have illegal or incompatible types",
				ErrorCodes::ILLEGAL_COLUMN};
	};

	try
	{
		if (!hasNullableBranches(block, args))
		{
			perform_multi_if(block, args, result, result);
			return;
		}

		/// The adopted approach is quite similar to how functions
		/// deal with nullable arguments. From the original block,
		/// we create a new block that contains only non-nullable
		/// types. Then we run multiIf on this block. Finally, we
		/// create a null byte map for the result.

		size_t row_count = block.rowsInFirstColumn();

		/// From the block to be processed, deduce a block whose specified
		/// columns are not nullable. We accept null columns because they
		/// are processed independently later.
		ColumnNumbers args_to_transform;
		size_t else_arg = Conditional::elseArg(args);
		for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
			args_to_transform.push_back(args[i]);
		args_to_transform.push_back(args[else_arg]);

		Block non_nullable_block = block.extractNonNullableBlock(args_to_transform);

		/// Append a column that tracks, for each result of multiIf, the index
		/// of the originating column.
		ColumnWithTypeAndName elem;
		elem.type = std::make_shared<DataTypeUInt64>();
		/// A pretty far-fetched name to make sure no conflict may occur.
		elem.name = "BranchTracker(" + block.dumpStructure() + ")";

		size_t tracker = non_nullable_block.columns();
		non_nullable_block.insert(elem);

		/// Really perform multiIf.
		perform_multi_if(non_nullable_block, args, result, tracker);

		/// Store the result.
		const ColumnWithTypeAndName & source_col = non_nullable_block.unsafeGetByPosition(result);
		ColumnWithTypeAndName & dest_col = block.unsafeGetByPosition(result);

		if (source_col.column.get()->isNull())
		{
			/// Degenerate case: the result is a null column.
			dest_col.column = source_col.column;
			return;
		}

		dest_col.column = std::make_shared<ColumnNullable>(source_col.column);

		/// Setup the null byte map of the result column by using the branch tracker column values.
		ColumnPtr tracker_holder = non_nullable_block.unsafeGetByPosition(tracker).column;
		ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*(dest_col.column.get()));

		auto is_null_at = [](const IColumn & col, size_t row)
		{
			return col.isNullable() && static_cast<const ColumnNullable &>(col).isNullAt(row);
		};

		if (auto col = typeid_cast<ColumnConstUInt64 *>(tracker_holder.get()))
		{
			auto pos = col->getData();
			const IColumn & origin = *(block.unsafeGetByPosition(pos).column.get());

			ColumnPtr null_map;

			if (origin.isNull())
				null_map = std::make_shared<ColumnUInt8>(row_count, 1);
			else if (origin.isNullable())
			{
				const ColumnNullable & origin_nullable = static_cast<const ColumnNullable &>(origin);
				null_map = origin_nullable.getNullValuesByteMap().get()->clone();
			}
			else
				null_map = std::make_shared<ColumnUInt8>(row_count, 0);

			nullable_col.getNullValuesByteMap() = null_map;
		}
		else if (auto col = typeid_cast<ColumnUInt64 *>(tracker_holder.get()))
		{
			auto null_map = std::make_shared<ColumnUInt8>(row_count);
			nullable_col.getNullValuesByteMap() = null_map;
			auto & null_map_data = null_map.get()->getData();

			const auto & data = col->getData();
			for (size_t row = 0; row < row_count; ++row)
			{
				const IColumn & origin = *(block.unsafeGetByPosition(data[row]).column.get());
				bool is_null = origin.isNull() || is_null_at(origin, row);
				null_map_data[row] = is_null ? 1 : 0;
			}
		}
		else
			throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
	}
	catch (const Conditional::CondException & ex)
	{
		rethrowContextually(ex);
	}
}

DataTypePtr FunctionMultiIf::getReturnTypeInternal(const DataTypes & args) const
{
	if (!Conditional::hasValidArgCount(args))
	{
		if (is_case_mode)
			throw Exception{"Some mandatory parameters are missing in the CASE "
				"construction", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
		else
			throw Exception{"Invalid number of arguments for function " + getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
	}

	/// Check conditions types.
	for (size_t i = Conditional::firstCond(); i < Conditional::elseArg(args); i = Conditional::nextCond(i))
	{
		const IDataType * observed_type;
		if (args[i].get()->isNullable())
		{
			const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*(args[i].get()));
			observed_type = nullable_type.getNestedType().get();
		}
		else
			observed_type = args[i].get();

		if (!typeid_cast<const DataTypeUInt8 *>(observed_type) && !observed_type->isNull())
		{
			if (is_case_mode)
				throw Exception{"In CASE construction, illegal type of WHEN clause "
				+ toString(i / 2) + ". Must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else
				throw Exception{"Illegal type of argument " + toString(i) + " (condition) "
					"of function " + getName() + ". Must be UInt8.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		}
	}

	bool has_nullable_args = hasNullableArgs(args);
	bool has_null_args = hasNullArgs(args);

	if (Conditional::hasArithmeticBranches(args))
		return Conditional::getReturnTypeForArithmeticArgs(args);
	else if (Conditional::hasArrayBranches(args))
	{
		/// NOTE Сообщения об ошибках будут относится к типам элементов массивов, что немного некорректно.
		DataTypes new_args;
		new_args.reserve(args.size());

		auto push_branch_arg = [&args, &new_args](size_t i)
		{
			if (args[i].get()->isNull())
				new_args.push_back(args[i]);
			else
			{
				const IDataType * observed_type;
				if (args[i].get()->isNullable())
				{
					const auto & nullable_type = static_cast<const DataTypeNullable &>(*(args[i].get()));
					observed_type = nullable_type.getNestedType().get();
				}
				else
					observed_type = args[i].get();

				const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(observed_type);
				if (type_arr == nullptr)
					throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
				new_args.push_back(type_arr->getNestedType());
			}
		};

		for (size_t i = 0; i < Conditional::elseArg(args); ++i)
		{
			if (Conditional::isCond(i))
				new_args.push_back(args[i]);
			else
				push_branch_arg(i);
		}

		push_branch_arg(Conditional::elseArg(args));

		/// NOTE: in a future release, this code will be rewritten. Indeed
		/// the current approach is flawed since it cannot appropriately
		/// deal with null arguments and arrays that contain null elements.
		/// For now we assume that arrays do not contain any such elements.
		DataTypePtr elt_type = getReturnTypeImpl(new_args);
		if (elt_type.get()->isNullable())
		{
			DataTypeNullable & nullable_type = static_cast<DataTypeNullable &>(*(elt_type.get()));
			elt_type = nullable_type.getNestedType();
		}

		DataTypePtr type = std::make_shared<DataTypeArray>(elt_type);
		if (has_nullable_args || has_null_args)
			type = std::make_shared<DataTypeNullable>(type);
		return type;
	}
	else if (!Conditional::hasIdenticalTypes(args))
	{
		if (Conditional::hasFixedStrings(args))
		{
			if (!Conditional::hasFixedStringsOfIdenticalLength(args))
			{
				if (is_case_mode)
					throw Exception{"THEN/ELSE clauses in CASE construction "
						"have FixedString type and different sizes",
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
				else
					throw Exception{"Branch (then, else) arguments of function " + getName() +
						" have FixedString type and different sizes",
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			}

			const IDataType * data = args[Conditional::firstThen()].get();
			const auto * fixed_str = typeid_cast<const DataTypeFixedString *>(data);

			if (fixed_str == nullptr)
				throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

			DataTypePtr type = std::make_shared<DataTypeFixedString>(fixed_str->getN());
			if (has_nullable_args || has_null_args)
				type = std::make_shared<DataTypeNullable>(type);
			return type;
		}
		else if (Conditional::hasStrings(args))
		{
			DataTypePtr type = std::make_shared<DataTypeString>();
			if (has_nullable_args || has_null_args)
				type = std::make_shared<DataTypeNullable>(type);
			return type;
		}
		else
		{
			if (is_case_mode)
				throw Exception{"THEN/ELSE clauses in CASE construction "
					"have incompatible arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else
				throw Exception{
					"Incompatible branch (then, else) arguments for function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
				};
		}
	}
	else
	{
		/// Return the type of the first non-null branch.
		/// Make it nullable if there is at least one nullable branch
		/// or one null branch.

		auto get_type_to_return = [has_nullable_args, has_null_args](const DataTypePtr & arg) -> DataTypePtr
		{
			if (arg.get()->isNullable())
				return arg;
			else if (has_nullable_args || has_null_args)
				return std::make_shared<DataTypeNullable>(arg);
			else
				return arg;
		};

		for (size_t i = Conditional::firstThen(); i < Conditional::elseArg(args); i = Conditional::nextThen(i))
		{
			if (!args[i].get()->isNull())
				return get_type_to_return(args[i]);
		}

		size_t i = Conditional::elseArg(args);
		if (!args[i].get()->isNull())
			return get_type_to_return(args[i]);

		/// All the branches are null.
		return std::make_shared<DataTypeNull>();
	}
}

bool FunctionMultiIf::performTrivialCase(Block & block, const ColumnNumbers & args, size_t result, size_t tracker)
{
	/// Check that all the branches have the same type. Moreover
	/// some or all these branches may be null.
	std::string first_type_name;
	DataTypePtr type;
	Field sample;

	size_t else_arg = Conditional::elseArg(args);
	for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
	{
		if (!block.getByPosition(args[i]).type.get()->isNull())
		{
			const auto & name = block.getByPosition(args[i]).type.get()->getName();
			if (first_type_name.empty())
			{
				first_type_name = name;
				type = block.getByPosition(args[i]).type;
				block.getByPosition(args[i]).column.get()->get(0, sample);
			}
			else
			{
				if (name != first_type_name)
					return false;
			}
		}
	}

	if (!block.getByPosition(args[else_arg]).type.get()->isNull())
	{
		if (first_type_name.empty())
		{
			type = block.getByPosition(args[else_arg]).type;
			block.getByPosition(args[else_arg]).column.get()->get(0, sample);
		}
		else
		{
			const auto & name = block.getByPosition(args[else_arg]).type.get()->getName();
			if (name != first_type_name)
				return false;
		}
	}

	size_t row_count = block.rowsInFirstColumn();
	auto & res_col = block.getByPosition(result).column;

	if (!type)
	{
		/// Degenerate case: all the branches are null.
		res_col = DataTypeNull{}.createConstColumn(row_count, Field{});
		return true;
	}

	/// Check that all the conditions are constants.
	for (size_t i = Conditional::firstCond(); i < else_arg; i = Conditional::nextCond(i))
	{
		const IColumn * col = block.getByPosition(args[i]).column.get();
		if (!col->isConst())
			return false;
	}

	/// Initialize readers for the conditions.
	Conditional::CondSources conds;
	conds.reserve(Conditional::getCondCount(args));

	for (size_t i = Conditional::firstCond(); i < else_arg; i = Conditional::nextCond(i))
		conds.emplace_back(block, args, i);

	/// Perform multiIf.

	auto make_result = [&](size_t index)
	{
		res_col = block.getByPosition(index).column;
		if (res_col.get()->isNull())
			res_col = type.get()->createConstColumn(row_count, sample);
		if (tracker != result)
		{
			ColumnPtr & col = block.getByPosition(tracker).column;
			col = std::make_shared<ColumnConstUInt64>(row_count, index);
		}
	};

	size_t i = Conditional::firstCond();
	for (const auto & cond : conds)
	{
		if (cond.get(0))
		{
			make_result(args[Conditional::thenFromCond(i)]);
			return true;
		}
		i = Conditional::nextCond(i);
	}

	make_result(args[else_arg]);
	return true;
}

/// Translate a context-free error into a contextual error.
void FunctionMultiIf::rethrowContextually(const Conditional::CondException & ex) const
{
	if (is_case_mode)
	{
		/// CASE construction context.
		if (ex.getCode() == Conditional::CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE)
			throw Exception{"Illegal type of column " + ex.getMsg1() +
				" in CASE construction", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		else if (ex.getCode() == Conditional::CondErrorCodes::TYPE_DEDUCER_UPSCALING_ERROR)
			throw Exception{"THEN/ELSE clause parameters in CASE construction are not upscalable to a "
				"common type without loss of precision: " + ex.getMsg1(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		else if (ex.getCode() == Conditional::CondErrorCodes::NUMERIC_PERFORMER_ILLEGAL_COLUMN)
		{
			size_t i = std::stoul(ex.getMsg1());
			if ((i % 2) == 1)
				throw Exception{"Illegal THEN clause " + toString(1 + (i - 1) / 2)
					+ " in CASE construction", ErrorCodes::ILLEGAL_COLUMN};
			else
				throw Exception{"Illegal ELSE clause in CASE construction",
					ErrorCodes::ILLEGAL_COLUMN};
		}
		else if (ex.getCode() == Conditional::CondErrorCodes::COND_SOURCE_ILLEGAL_COLUMN)
		{
			size_t i = std::stoul(ex.getMsg2());
			if ((i % 2) == 1)
				throw Exception{"Illegal column " + ex.getMsg1() + " of THEN clause "
					+ toString(1 + (i - 1) / 2) + " in CASE construction."
					"Must be ColumnUInt8 or ColumnConstUInt8", ErrorCodes::ILLEGAL_COLUMN};
			else
				throw Exception{"Illegal column " + ex.getMsg1() + " of ELSE clause "
					" in CASE construction. Must be ColumnUInt8 or ColumnConstUInt8",
					ErrorCodes::ILLEGAL_COLUMN};
		}
		else if (ex.getCode() == Conditional::CondErrorCodes::NUMERIC_EVALUATOR_ILLEGAL_ARGUMENT)
		{
			size_t i = std::stoul(ex.getMsg1());
			if ((i % 2) == 1)
				throw Exception{"Illegal type of THEN clause " + toString(1 + (i - 1) / 2)
					+ " in CASE construction", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else
				throw Exception{"Illegal type of ELSE clause in CASE construction",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		}
		else if (ex.getCode() == Conditional::CondErrorCodes::ARRAY_EVALUATOR_INVALID_TYPES)
			throw Exception{"Internal logic error: one or more THEN/ELSE clauses of "
				"CASE construction have invalid types", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		else
			throw Exception{"An unexpected error has occurred in CASE construction",
				ErrorCodes::LOGICAL_ERROR};
	}
	else
	{
		/// multiIf function context.
		if (ex.getCode() == Conditional::CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE)
			throw Exception{"Illegal type of column " + ex.getMsg1() +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		else if (ex.getCode() == Conditional::CondErrorCodes::TYPE_DEDUCER_UPSCALING_ERROR)
			throw Exception{"Arguments of function multiIf are not upscalable to a "
				"common type without loss of precision: " + ex.getMsg1(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		else if (ex.getCode() == Conditional::CondErrorCodes::NUMERIC_PERFORMER_ILLEGAL_COLUMN)
			throw Exception{"Illegal argument " + ex.getMsg1() + " of function multiIf",
				ErrorCodes::ILLEGAL_COLUMN};
		else if (ex.getCode() == Conditional::CondErrorCodes::COND_SOURCE_ILLEGAL_COLUMN)
			throw Exception{"Illegal column " + ex.getMsg1() + " of argument "
				+ ex.getMsg2() + " of function multiIf"
				"Must be ColumnUInt8 or ColumnConstUInt8.", ErrorCodes::ILLEGAL_COLUMN};
		else if (ex.getCode() == Conditional::CondErrorCodes::NUMERIC_EVALUATOR_ILLEGAL_ARGUMENT)
			throw Exception{"Illegal type of argument " + ex.getMsg1() + " of function multiIf",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		else if (ex.getCode() == Conditional::CondErrorCodes::ARRAY_EVALUATOR_INVALID_TYPES)
			throw Exception{"Internal logic error: one or more arguments of function "
				"multiIf have invalid types", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		else
			throw Exception{"An unexpected error has occurred while performing multiIf",
				ErrorCodes::LOGICAL_ERROR};
	}
}

}
