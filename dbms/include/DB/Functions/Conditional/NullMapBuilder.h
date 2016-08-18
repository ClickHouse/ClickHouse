#pragma once

#include <DB/Core/Block.h>

namespace DB
{

namespace Conditional
{

/// Here is provided a way to incrementally build the null map of the result column
/// of a multiIf invokation if its type is nullable.
class NullMapBuilder final
{
public:
	/// Create a dummy builder when we don't need any builder, i.e. when the result
	/// of multiIf is not nullable.
	NullMapBuilder()
		: block{empty_block}
	{
	}

	/// This constructor takes the block that contains the original data received
	/// by multiIf, i.e. they have not been processed.
	NullMapBuilder(Block & block_)
		: block{block_}, row_count{block.rowsInFirstColumn()}
	{
	}

	/// Check whether the builder is dummy or not.
	operator bool() const { return block; }
	bool operator!() const { return !block; }

	/// Initialize the builder. For the non-trivial execution paths of multiIf.
	void init(const ColumnNumbers & args);

	/// Update the null map being built at the row that has just been processed
	/// by multiIf. The parameter index indicates the index of the column being
	/// checked for nullity. For non-trivial execution paths of multiIf.
	void update(size_t index, size_t row);

	/// Build the null map. The parameter index has the same meaning as above.
	/// For the trivial execution path of multiIf.
	void build(size_t index);

	/// Accessor needed to return the fully built null map.
	ColumnPtr getNullMap() const { return null_map; }

private:
	/// Property of a column.
	enum Property
	{
		/// Neither nullable nor null.
		IS_ORDINARY = 0,
		/// Nullable column.
		IS_NULLABLE,
		/// Null column.
		IS_NULL
	};

private:
	Block & block;

	/// Remember for each column representing an argument whether it is
	/// nullable, null, or neither of them. This avoids us many costly
	/// calls to virtual functions.
	PaddedPODArray<Property> cols_properties;

	ColumnPtr null_map;
	size_t row_count;

private:
	static Block empty_block;
};

}

}
