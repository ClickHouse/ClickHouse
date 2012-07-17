#pragma once

#include <DB/Core/Block.h>
#include <DB/Core/SortDescription.h>


namespace DB
{

/// Отсортировать один блок по описанию desc.
void sortBlock(Block & block, const SortDescription & description);

}
