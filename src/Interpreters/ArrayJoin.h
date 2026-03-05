#pragma once
#include <Core/Names.h>

namespace DB
{

struct ArrayJoin
{
    Names columns;
    bool is_left = false;
    bool array_join_use_nulls = false;
};

}
