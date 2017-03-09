#pragma once

#include <DB/Core/Block.h>
#include <DB/Core/ColumnNumbers.h>

namespace DB
{

namespace Conditional
{

class NullMapBuilder;

struct StringEvaluator final
{
	/// For the meaning of the builder parameter, see the FunctionMultiIf::perform() declaration.
	static bool perform(Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder);
};

}

}
