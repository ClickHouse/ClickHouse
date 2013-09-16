#pragma once

#include <DB/Core/Block.h>
#include <DB/Core/SortDescription.h>


namespace DB
{

/// Отсортировать один блок по описанию desc. Если limit != 0, то производится partial sort первых limit строк.
void sortBlock(Block & block, const SortDescription & description, size_t limit = 0);

}
