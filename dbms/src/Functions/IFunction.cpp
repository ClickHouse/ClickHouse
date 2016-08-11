#include <DB/Functions/IFunction.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/DataTypes/DataTypeNull.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Interpreters/ExpressionActions.h>

namespace DB
{

namespace
{

void createNullValuesByteMap(Block & block, const ColumnNumbers & args, size_t result)
{
	ColumnNullable & res_col = static_cast<ColumnNullable &>(*block.unsafeGetByPosition(result).column);

	for (const auto & arg : args)
	{
		if (arg == result)
			continue;

		const ColumnWithTypeAndName & elem = block.unsafeGetByPosition(arg);
		if (elem.column && elem.column.get()->isNullable())
		{
			const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*elem.column);
			res_col.updateNullValuesByteMap(nullable_col);
		}
	}
}

/// Check if a block contains at least one null column.
bool hasNullColumns(const Block & block, const ColumnNumbers & arguments)
{
	for (const auto & arg : arguments)
	{
		const auto & elem = block.unsafeGetByPosition(arg);
		if (elem.column && elem.column.get()->isNull())
			return true;
	}
	return false;
}

/// Check if at least one column is null.
bool hasNullColumns(const ColumnsWithTypeAndName & args)
{
	for (const auto & arg : args)
	{
		if (arg.type.get()->isNull())
			return true;
	}

	return false;
}

/// Check if at least one argument is null.
bool hasNullArguments(const DataTypes & args)
{
	for (const auto & arg : args)
	{
		if (arg.get()->isNull())
			return true;
	}

	return false;
}

/// Check if a block contains at least one nullable column.
bool hasNullableColumns(const Block & block, const ColumnNumbers & arguments)
{
	for (const auto & arg : arguments)
	{
		const auto & elem = block.unsafeGetByPosition(arg);
		if (elem.column && elem.column.get()->isNullable())
			return true;
	}
	return false;
}

/// Check if at least one column is nullable.
bool hasNullableColumns(const ColumnsWithTypeAndName & args)
{
	for (const auto & arg : args)
	{
		if (arg.type.get()->isNullable())
			return true;
	}

	return false;
}

/// Check if at least one argument is nullable.
bool hasNullableArguments(const DataTypes & args)
{
	for (const auto & arg : args)
	{
		if (arg.get()->isNullable())
			return true;
	}

	return false;
}

/// Turn the specified set of columns into a set of non-nullable columns.
ColumnsWithTypeAndName toNonNullableColumns(const ColumnsWithTypeAndName & args)
{
	ColumnsWithTypeAndName new_args;
	new_args.reserve(args.size());

	for (const auto & arg : args)
	{
		if (arg.type.get()->isNullable())
		{
			auto nullable_col = static_cast<const ColumnNullable *>(arg.column.get());
			ColumnPtr nested_col = (nullable_col != nullptr) ? nullable_col->getNestedColumn() : nullptr;
			auto nullable_type = static_cast<const DataTypeNullable *>(arg.type.get());
			DataTypePtr nested_type = nullable_type->getNestedType();

			new_args.emplace_back(nested_col, nested_type, arg.name);
		}
		else
			new_args.emplace_back(arg.column, arg.type, arg.name);
	}

	return new_args;
}

/// Turn the specified set of data types into a set of non-nullable data types.
DataTypes toNonNullableArguments(const DataTypes & args)
{
	DataTypes new_args;
	new_args.reserve(args.size());

	for (const auto & arg : args)
	{
		if (arg.get()->isNullable())
		{
			auto nullable_type = static_cast<const DataTypeNullable *>(arg.get());
			DataTypePtr nested_type = nullable_type->getNestedType();
			new_args.push_back(nested_type);
		}
		else
			new_args.push_back(arg);
	}

	return new_args;
}

}

DataTypePtr IFunction::getReturnType(const DataTypes & arguments) const
{
	if (!hasSpecialSupportForNulls() && hasNullArguments(arguments))
		return std::make_shared<DataTypeNull>();

	if (!hasSpecialSupportForNulls() && hasNullableArguments(arguments))
	{
		const DataTypes new_args = toNonNullableArguments(arguments);
		return getReturnTypeImpl(new_args);
	}
	else
		return getReturnTypeImpl(arguments);
}

void IFunction::getReturnTypeAndPrerequisites(
	const ColumnsWithTypeAndName & arguments,
	DataTypePtr & out_return_type,
	std::vector<ExpressionAction> & out_prerequisites)
{
	if (!hasSpecialSupportForNulls() && hasNullColumns(arguments))
	{
		out_return_type = std::make_shared<DataTypeNull>();
		return;
	}

	if (!hasSpecialSupportForNulls() && hasNullableColumns(arguments))
	{
		const ColumnsWithTypeAndName new_args = toNonNullableColumns(arguments);
		getReturnTypeAndPrerequisitesImpl(new_args, out_return_type, out_prerequisites);
		out_return_type = std::make_shared<DataTypeNullable>(out_return_type);
	}
	else
		getReturnTypeAndPrerequisitesImpl(arguments, out_return_type, out_prerequisites);
}

void IFunction::getLambdaArgumentTypes(DataTypes & arguments) const
{
	if (!hasSpecialSupportForNulls() && hasNullArguments(arguments))
		return;

	if (!hasSpecialSupportForNulls() && hasNullableArguments(arguments))
	{
		DataTypes new_args = toNonNullableArguments(arguments);
		getLambdaArgumentTypesImpl(new_args);
		arguments = std::move(new_args);
	}
	else
		getLambdaArgumentTypesImpl(arguments);
}

/// Return a copy of a given block in which the specified columns are replaced by
/// their respective nested columns if they are nullable.
Block IFunction::extractNonNullableBlock(const Block & block, const ColumnNumbers args)
{
	std::sort(args.begin(), args.end());

	Block non_nullable_block;

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const auto & col = block.unsafeGetByPosition(i);

		bool found = std::binary_search(args.begin(), args.end(), i) && col.column && col.type;

		if (found && col.column.get()->isNullable())
		{
			auto nullable_col = static_cast<const ColumnNullable *>(col.column.get());
			ColumnPtr nested_col = nullable_col->getNestedColumn();

			auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
			DataTypePtr nested_type = nullable_type->getNestedType();

			non_nullable_block.insert(i, {nested_col, nested_type, col.name});
		}
		else
			non_nullable_block.insert(i, col);
	}

	return non_nullable_block;
}

template <typename Fun>
void IFunction::perform(Block & block, const ColumnNumbers & arguments, size_t result, const Fun & performer)
{
	if (!hasSpecialSupportForNulls() && hasNullColumns(block, arguments))
	{
		ColumnWithTypeAndName & dest_col = block.getByPosition(result);
		dest_col.column =  std::make_shared<ColumnNull>(block.rowsInFirstColumn(), Null());
		return;
	}

	if (!hasSpecialSupportForNulls() && hasNullableColumns(block, arguments))
	{
		Block non_nullable_block = extractNonNullableBlock(block, arguments);
		performer(non_nullable_block, arguments, result);
		const ColumnWithTypeAndName & source_col = non_nullable_block.getByPosition(result);
		ColumnWithTypeAndName & dest_col = block.getByPosition(result);
		dest_col.column = std::make_shared<ColumnNullable>(source_col.column);
		ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*dest_col.column);
		nullable_col.getNullValuesByteMap() = std::make_shared<ColumnUInt8>(dest_col.column->size(), 0);
		createNullValuesByteMap(block, arguments, result);
	}
	else
		performer(block, arguments, result);
}

void IFunction::execute(Block & block, const ColumnNumbers & arguments, size_t result)
{
	auto performer = [&](Block & block, const ColumnNumbers & arguments, size_t result)
	{
		executeImpl(block, arguments, result);
	};

	perform(block, arguments, result, performer);
}

void IFunction::execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result)
{
	auto performer = [&](Block & block, const ColumnNumbers & arguments, size_t result)
	{
		executeImpl(block, arguments, prerequisites, result);
	};

	perform(block, arguments, result, performer);
}

}

