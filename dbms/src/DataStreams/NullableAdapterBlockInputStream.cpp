#include <DB/DataStreams/NullableAdapterBlockInputStream.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{

extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
extern const int NO_SUCH_COLUMN_IN_TABLE;
extern const int LOGICAL_ERROR;

}

NullableAdapterBlockInputStream::NullableAdapterBlockInputStream(
	BlockInputStreamPtr input_,
	const Block & in_sample_, const Block & out_sample_,
	const NamesAndTypesListPtr & required_columns_)
	: required_columns{required_columns_},
	actions{getActions(in_sample_, out_sample_)},
	must_transform{mustTransform()}
{
	children.push_back(input_);
}

String NullableAdapterBlockInputStream::getID() const
{
	std::stringstream res;
	res << "NullableAdapterBlockInputStream(" << children.back()->getID() << ")";
	return res.str();
}

Block NullableAdapterBlockInputStream::readImpl()
{
	Block block = children.back()->read();

	if (!block || !must_transform)
		return block;

	Block res;
	size_t s = block.columns();

	for (size_t i = 0; i < s; ++i)
	{
		const auto & elem = block.unsafeGetByPosition(i);
		ColumnWithTypeAndName new_elem;

		if (actions[i] == TO_ORDINARY)
		{
			const auto & nullable_col = static_cast<const ColumnNullable &>(*elem.column);
			const auto & nullable_type = static_cast<const DataTypeNullable &>(*elem.type);

			const auto & null_map = nullable_col.getNullMap();
			bool has_nulls = std::any_of(null_map.begin(), null_map.end(), [](UInt8 val){ return val == 1; });

			if (has_nulls)
				throw Exception{"Cannot insert NULL value into non-nullable column",
					ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};
			else
				res.insert({
					nullable_col.getNestedColumn(),
					nullable_type.getNestedType(),
					elem.name
				});
		}
		else if (actions[i] == TO_NULLABLE)
		{
			auto null_map = std::make_shared<ColumnUInt8>(elem.column->size(), 0);

			res.insert({
				std::make_shared<ColumnNullable>(elem.column, null_map),
				std::make_shared<DataTypeNullable>(elem.type),
				elem.name
			});
		}
		else if (actions[i] == NONE)
			res.insert(elem);
		else
			throw Exception{"NullableAdapterBlockInputStream: internal error", ErrorCodes::LOGICAL_ERROR};
	}

	return res;
}

bool NullableAdapterBlockInputStream::mustTransform() const
{
	return !std::all_of(actions.begin(), actions.end(), [](Action action) { return action == NONE; });
}

NullableAdapterBlockInputStream::Actions NullableAdapterBlockInputStream::getActions(
	const Block & in_sample, const Block & out_sample) const
{
	size_t in_size = in_sample.columns();
	size_t out_size = out_sample.columns();

	Actions actions;
	actions.reserve(in_size);

	size_t j = 0;
	for (size_t i = 0; i < in_size; ++i)
	{
		const auto & in_elem = in_sample.unsafeGetByPosition(i);
		while (j < out_size)
		{
			const auto & out_elem = out_sample.unsafeGetByPosition(j);
			if (in_elem.name == out_elem.name)
			{
				bool is_in_nullable = in_elem.type->isNullable();
				bool is_out_nullable = out_elem.type->isNullable();

				if (is_in_nullable && !is_out_nullable)
					actions.push_back(TO_ORDINARY);
				else if (!is_in_nullable && is_out_nullable)
					actions.push_back(TO_NULLABLE);
				else
					actions.push_back(NONE);

				++j;
				break;
			}
			else
			{
				++j;
				if (j == out_size)
				{
					auto print_columns = [](const NamesAndTypesList & columns)
					{
						bool is_first = true;
						std::ostringstream ostr;
						for (const auto & it : columns)
						{
							if (is_first)
								is_first = false;
							else
								ostr << ", ";
							ostr << it.name;
						}
						return ostr.str();
					};

					throw Exception{"There is no column with name " + in_elem.name
						+ ". There are columns: "
						+ print_columns(*required_columns),
						ErrorCodes::NO_SUCH_COLUMN_IN_TABLE};
				}
			}
		}
	}

	return actions;
}

}
