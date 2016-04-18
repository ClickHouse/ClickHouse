#pragma once

#include <DB/Core/Block.h>
#include <DB/Core/ColumnNumbers.h>

namespace DB
{

namespace Conditional
{

struct StringEvaluator final
{
	static bool perform(Block & block, const ColumnNumbers & args, size_t result);
};

}

}
