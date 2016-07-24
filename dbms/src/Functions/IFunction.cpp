#include <DB/Functions/IFunction.h>
#include <DB/Columns/ColumnNull.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/DataTypes/DataTypeNull.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Interpreters/ExpressionActions.h>

namespace DB
{

namespace
{

void createNullValuesByteMap(Block & block, size_t result)
{
	ColumnNullable & res_col = static_cast<ColumnNullable &>(*(block.unsafeGetByPosition(result).column.get()));

	for (size_t i = 0; i < block.columns(); ++i)
	{
		if (i == result)
			continue;

		const ColumnWithTypeAndName & elem = block.unsafeGetByPosition(i);
		if (elem.column && elem.type && (elem.type.get()->isNullable()))
		{
			const ColumnNullable & concrete_col = static_cast<const ColumnNullable &>(*(elem.column.get()));
			res_col.updateNullValuesByteMap(concrete_col);
		}
	}
}

bool hasNullColumns(const ColumnsWithTypeAndName & args)
{
	for (const auto & arg : args)
	{
		if (arg.type.get()->isNull())
			return true;
	}

	return false;
}

bool hasNullColumns(const DataTypes & args)
{
	for (const auto & arg : args)
	{
		if (arg.get()->isNull())
			return true;
	}

	return false;
}

bool hasNullableColumns(const ColumnsWithTypeAndName & args)
{
	for (const auto & arg : args)
	{
		if (arg.type.get()->isNullable())
			return true;
	}

	return false;
}

bool hasNullableColumns(const DataTypes & args)
{
	for (const auto & arg : args)
	{
		if (arg.get()->isNullable())
			return true;
	}

	return false;
}

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

DataTypes toNonNullableColumns(const DataTypes & args)
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
	if (!hasSpecialSupportForNulls() && hasNullColumns(arguments))
		return std::make_shared<DataTypeNull>();

	if (!hasSpecialSupportForNulls() && hasNullableColumns(arguments))
	{
		const DataTypes new_args = toNonNullableColumns(arguments);
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
	if (!hasSpecialSupportForNulls() && hasNullColumns(arguments))
		return;

	if (!hasSpecialSupportForNulls() && hasNullableColumns(arguments))
	{
		DataTypes new_args = toNonNullableColumns(arguments);
		getLambdaArgumentTypesImpl(new_args);
		arguments = std::move(new_args);
	}
	else
		getLambdaArgumentTypesImpl(arguments);
}

void IFunction::execute(Block & block, const ColumnNumbers & arguments, size_t result)
{
	if (!hasSpecialSupportForNulls() && !hasSpecialSupportForNullValues() && block.hasNullColumns())
	{
		ColumnWithTypeAndName & dest_col = block.getByPosition(result);
		dest_col.column =  std::make_shared<ColumnNull>(block.rowsInFirstColumn());
		return;
	}

	if (!hasSpecialSupportForNulls() && block.hasNullableColumns(arguments))
	{
		Block non_nullable_block = block.extractNonNullableBlock();
		executeImpl(non_nullable_block, arguments, result);
		const ColumnWithTypeAndName & source_col = non_nullable_block.getByPosition(result);
		ColumnWithTypeAndName & dest_col = block.getByPosition(result);
		dest_col.column = std::make_shared<ColumnNullable>(source_col.column);
		createNullValuesByteMap(block, result);
	}
	else
		executeImpl(block, arguments, result);
}

void IFunction::execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result)
{
	if (!hasSpecialSupportForNulls() && !hasSpecialSupportForNullValues() && block.hasNullColumns())
	{
		ColumnWithTypeAndName & dest_col = block.getByPosition(result);
		dest_col.column =  std::make_shared<ColumnNull>(block.rowsInFirstColumn());
		return;
	}

	if (!hasSpecialSupportForNulls() && block.hasNullableColumns(arguments))
	{
		Block non_nullable_block = block.extractNonNullableBlock();
		executeImpl(non_nullable_block, arguments, prerequisites, result);
		const ColumnWithTypeAndName & source_col = non_nullable_block.getByPosition(result);
		ColumnWithTypeAndName & dest_col = block.getByPosition(result);
		dest_col.column = std::make_shared<ColumnNullable>(source_col.column);
		createNullValuesByteMap(block, result);
	}
	else
		executeImpl(block, arguments, prerequisites, result);
}

}

