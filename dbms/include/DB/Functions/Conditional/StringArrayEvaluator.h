#pragma once

#include <DB/Core/Block.h>
#include <DB/Core/ColumnNumbers.h>

namespace DB
{

namespace Conditional
{

class NullMapBuilder;

class StringArrayEvaluator
{
public:
	/// The tracker parameter is an index to a column that tracks the originating column of each value of
	/// the result column. Calling this function with result == tracker means that no such tracking is
	/// required, which happens if multiIf is called with no nullable parameters.
	static bool perform(Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder);
};

}

}
