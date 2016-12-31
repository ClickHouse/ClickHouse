#include <math.h>

#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArithmetic.h>
#include <DB/Functions/FunctionsMiscellaneous.h>
#include <DB/Functions/DataTypeTraits.h>
#include <DB/DataTypes/DataTypeEnum.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Columns/ColumnNullable.h>
#include <common/ClickHouseRevision.h>
#include <ext/enumerate.hpp>


namespace DB
{


static size_t widthOfUTF8String(const String & s)
{
	size_t res = 0;
	for (auto c : s)	/// Skip UTF-8 continuation bytes.
		res += (UInt8(c) <= 0x7F || UInt8(c) >= 0xC0);
	return res;
}


void FunctionVisibleWidth::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	auto & src = block.getByPosition(arguments[0]);
	size_t size = block.rowsInFirstColumn();

	if (!src.column->isConst())
	{
		auto res_col = std::make_shared<ColumnUInt64>(size);
		auto & res_data = static_cast<ColumnUInt64 &>(*res_col).getData();
		block.getByPosition(result).column = res_col;

		/// For simplicity reasons, function is implemented by serializing into temporary buffer.

		String tmp;
		for (size_t i = 0; i < size; ++i)
		{
			{
				WriteBufferFromString out(tmp);
				src.type->serializeTextEscaped(*src.column, i, out);
			}

			res_data[i] = widthOfUTF8String(tmp);
		}
	}
	else
	{
		String tmp;
		{
			WriteBufferFromString out(tmp);
			src.type->serializeTextEscaped(*src.column->cut(0, 1)->convertToFullColumnIfConst(), 0, out);
		}

		block.getByPosition(result).column = std::make_shared<ColumnConstUInt64>(size, widthOfUTF8String(tmp));
	}
}


void FunctionHasColumnInTable::getReturnTypeAndPrerequisitesImpl(
	const ColumnsWithTypeAndName & arguments,
	DataTypePtr & out_return_type,
	ExpressionActions::Actions & out_prerequisites)
{
	static const std::string arg_pos_description[] = {"First", "Second", "Third"};
	for (size_t i = 0; i < getNumberOfArguments(); ++i)
	{
		const ColumnWithTypeAndName & argument = arguments[i];

		const ColumnConstString * column = typeid_cast<const ColumnConstString *>(argument.column.get());
		if (!column)
		{
			throw Exception(
				arg_pos_description[i] + " argument for function " + getName() + " must be const String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
	}

	out_return_type = std::make_shared<DataTypeUInt8>();
}


void FunctionHasColumnInTable::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	auto get_string_from_block =
		[&](size_t column_pos) -> const String &
		{
			ColumnPtr column = block.getByPosition(column_pos).column;
			const ColumnConstString * const_column = typeid_cast<const ColumnConstString *>(column.get());
			return const_column->getData();
		};

	const String & database_name = get_string_from_block(arguments[0]);
	const String & table_name = get_string_from_block(arguments[1]);
	const String & column_name = get_string_from_block(arguments[2]);

	const StoragePtr & table = global_context.getTable(database_name, table_name);
	const bool has_column = table->hasColumn(column_name);

	block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(
		block.rowsInFirstColumn(), has_column);
}


std::string FunctionVersion::getVersion() const
{
	std::ostringstream os;
	os << DBMS_VERSION_MAJOR << "." << DBMS_VERSION_MINOR << "." << ClickHouseRevision::get();
	return os.str();
}


void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
	factory.registerFunction<FunctionCurrentDatabase>();
	factory.registerFunction<FunctionHostName>();
	factory.registerFunction<FunctionVisibleWidth>();
	factory.registerFunction<FunctionToTypeName>();
	factory.registerFunction<FunctionToColumnTypeName>();
	factory.registerFunction<FunctionBlockSize>();
	factory.registerFunction<FunctionBlockNumber>();
	factory.registerFunction<FunctionRowNumberInBlock>();
	factory.registerFunction<FunctionRowNumberInAllBlocks>();
	factory.registerFunction<FunctionSleep>();
	factory.registerFunction<FunctionMaterialize>();
	factory.registerFunction<FunctionIgnore>();
	factory.registerFunction<FunctionIndexHint>();
	factory.registerFunction<FunctionIdentity>();
	factory.registerFunction<FunctionArrayJoin>();
	factory.registerFunction<FunctionReplicate>();
	factory.registerFunction<FunctionBar>();
	factory.registerFunction<FunctionHasColumnInTable>();

	factory.registerFunction<FunctionTuple>();
	factory.registerFunction<FunctionTupleElement>();
	factory.registerFunction<FunctionIn<false, false>>();
	factory.registerFunction<FunctionIn<false, true>>();
	factory.registerFunction<FunctionIn<true, false>>();
	factory.registerFunction<FunctionIn<true, true>>();

	factory.registerFunction<FunctionIsFinite>();
	factory.registerFunction<FunctionIsInfinite>();
	factory.registerFunction<FunctionIsNaN>();

	factory.registerFunction<FunctionVersion>();
	factory.registerFunction<FunctionUptime>();

	factory.registerFunction<FunctionRunningAccumulate>();
	factory.registerFunction<FunctionRunningDifference>();
	factory.registerFunction<FunctionFinalizeAggregation>();
}

}
