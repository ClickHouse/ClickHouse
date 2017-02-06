#pragma once

#include <DB/Core/Block.h>
#include <DB/Core/ColumnNumbers.h>

namespace DB
{

namespace Conditional
{

class NullMapBuilder;

struct NumericPerformer
{
	/// Perform a multiIf function for numeric branch (then, else) arguments
	/// that may have either scalar types or array types.
	/// For the meaning of the builder parameter, see the FunctionMultiIf::perform() declaration.
	static bool perform(Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder);
};

}

}
