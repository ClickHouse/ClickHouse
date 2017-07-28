#pragma once

#include <Core/Block.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

namespace Conditional
{

class NullMapBuilder;

class StringArrayEvaluator
{
public:
    static bool perform(Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder);
};

}

}
