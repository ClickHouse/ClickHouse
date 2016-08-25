#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{

extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;

};

/// This streams allows perfoming INSERT requests in which the types of
/// the target and source blocks are compatible up to nullability:
///
/// - if a target column is nullable while the corresponding source
/// column is not, we embed the source column into a nullable column;
/// - if a source column is nullable while the corresponding target
/// column is not, we extract the nested column from the source;
/// - otherwise we just perform an identity mapping.
class NullableAdapterBlockInputStream : public IProfilingBlockInputStream
{
private:
	/// Given a column of a block we have just read,
	/// how must we process it?
	enum Action
	{
		/// Do nothing.
		NONE = 0,
		/// Convert nullable column to ordinary column.
		TO_ORDINARY,
		/// Convert non-nullable column to nullable column.
		TO_NULLABLE
	};

	/// Actions to be taken for each column of a block.
	using Actions = std::vector<Action>;

public:
	NullableAdapterBlockInputStream(BlockInputStreamPtr input_, const Block & in_sample_, const Block & out_sample_)
		: actions{getActions(in_sample_, out_sample_)},
		must_transform{mustTransform()}
	{
		children.push_back(input_);
	}

	String getName() const override { return "NullableAdapterBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << "NullableAdapterBlockInputStream(" << children.back()->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl() override
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

				const auto & null_map = static_cast<const ColumnUInt8 &>(*nullable_col.getNullValuesByteMap()).getData();
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

private:
	/// Return true if we must transform the blocks we read.
	bool mustTransform() const
	{
		return !std::all_of(actions.begin(), actions.end(), [](Action action) { return action == NONE; });
	}

	/// Determine the actions to be taken using the source sample block,
	/// which describes the columns from which we fetch data inside an INSERT
	/// query, and the target sample block, which describes the columns to
	/// which we insert data inside the INSERT query.
	Actions getActions(const Block & in_sample, const Block & out_sample)
	{
		size_t s = in_sample.columns();

		if (s != out_sample.columns())
			throw Exception{"NullableAdapterBlockInputStream: internal error", ErrorCodes::LOGICAL_ERROR};

		Actions actions;
		actions.reserve(s);

		for (size_t i = 0; i < s; ++i)
		{
			bool is_in_nullable = in_sample.unsafeGetByPosition(i).type->isNullable();
			bool is_out_nullable = out_sample.unsafeGetByPosition(i).type->isNullable();

			if (is_in_nullable && !is_out_nullable)
				actions.push_back(TO_ORDINARY);
			else if (!is_in_nullable && is_out_nullable)
				actions.push_back(TO_NULLABLE);
			else
				actions.push_back(NONE);
		}

		return actions;
	}

private:
	const Actions actions;
	const bool must_transform;
};

}
