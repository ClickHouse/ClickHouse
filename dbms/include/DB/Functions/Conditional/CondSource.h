#pragma once

#include <DB/Common/PODArray.h>
#include <DB/Core/Block.h>
#include <DB/Core/ColumnNumbers.h>

namespace DB
{

namespace Conditional
{

/// This class provides access to the values of a condition in a multiIf function.
class CondSource final
{
public:
	CondSource(const Block & block, const ColumnNumbers & args, size_t i);

	CondSource(const CondSource &) = delete;
	CondSource & operator=(const CondSource &) = delete;

	CondSource(CondSource &&) = default;
	CondSource & operator=(CondSource &&) = default;

	/// Get the value of this condition for a given row.
	inline UInt8 get(size_t row) const
	{
		return data_array[row];
	}

	inline UInt8 getSize() const
	{
		return data_array.size();
	}

private:
	static const ColumnPtr initMaterializedCol(const Block & block, const ColumnNumbers & args, size_t i);

	static const PaddedPODArray<UInt8> & initDataArray(const Block & block, const ColumnNumbers & args,
		size_t i, const ColumnPtr & materialized_col_);

private:
	const ColumnPtr materialized_col;
	const PaddedPODArray<UInt8> & data_array;
	static const ColumnPtr null_materialized_col;
};

using CondSources = std::vector<CondSource>;

}

}
