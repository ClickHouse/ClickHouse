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
	inline bool get(size_t row) const
	{
		bool is_null = !null_map.empty() ? (null_map[row] != 0) : false;
		return is_null ? false : static_cast<bool>(data_array[row]);
	}

	inline size_t getSize() const
	{
		return data_array.size();
	}

private:
	static const ColumnPtr initMaterializedCol(const Block & block, const ColumnNumbers & args, size_t i);

	static const PaddedPODArray<UInt8> & initDataArray(const Block & block, const ColumnNumbers & args,
		size_t i, const ColumnPtr & materialized_col_);

	static const PaddedPODArray<UInt8> & initNullMap(const Block & block, const ColumnNumbers & args, size_t i);

private:
	const ColumnPtr materialized_col;
	const PaddedPODArray<UInt8> & data_array;
	const PaddedPODArray<UInt8> & null_map;

	static const ColumnPtr null_materialized_col;
	static const PaddedPODArray<UInt8> empty_null_map;
};

using CondSources = std::vector<CondSource>;

}

}
