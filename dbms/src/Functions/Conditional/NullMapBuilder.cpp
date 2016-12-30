#include <DB/Functions/Conditional/NullMapBuilder.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace Conditional
{
	Block NullMapBuilder::empty_block;

	void NullMapBuilder::init(const ColumnNumbers & args)
	{
		null_map = std::make_shared<ColumnUInt8>(row_count);

		cols_properties.resize(block.columns());
		for (const auto & arg : args)
		{
			const auto & col = *block.unsafeGetByPosition(arg).column;
			if (col.isNullable())
				cols_properties[arg] = IS_NULLABLE;
			else if (col.isNull())
				cols_properties[arg] = IS_NULL;
			else
				cols_properties[arg] = IS_ORDINARY;
		}
	}

	void NullMapBuilder::update(size_t index, size_t row)
	{
		const IColumn & from = *block.unsafeGetByPosition(index).column;

		bool is_null;

		auto property = cols_properties[index];

		if (property == IS_NULL)
			is_null = true;
		else if (property == IS_NULLABLE)
		{
			const auto & nullable_col = static_cast<const ColumnNullable &>(from);
			is_null = nullable_col.isNullAt(row);
		}
		else if (property == IS_ORDINARY)
			is_null = false;
		else
			throw Exception{"NullMapBuilder: internal error", ErrorCodes::LOGICAL_ERROR};

		auto & null_map_data = static_cast<ColumnUInt8 &>(*null_map).getData();
		null_map_data[row] = is_null ? 1 : 0;
	}

	void NullMapBuilder::build(size_t index)
	{
		const IColumn & from = *block.unsafeGetByPosition(index).column;

		if (from.isNull())
			null_map = std::make_shared<ColumnUInt8>(row_count, 1);
		else if (from.isNullable())
		{
			const auto & nullable_col = static_cast<const ColumnNullable &>(from);
			null_map = nullable_col.getNullMapColumn();
		}
		else
			null_map = std::make_shared<ColumnUInt8>(row_count, 0);
	}
}

}
