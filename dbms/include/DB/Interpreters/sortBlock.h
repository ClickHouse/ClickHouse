#pragma once

#include <DB/Core/Block.h>
#include <DB/Core/SortDescription.h>


namespace DB
{

/// Отсортировать один блок по описанию desc. Если limit != 0, то производится partial sort первых limit строк.
void sortBlock(Block & block, const SortDescription & description, size_t limit = 0);


/** Используется только в StorageMergeTree для сортировки данных при INSERT-е.
  * Сортировка стабильная. Это важно для сохранения порядка записей в движке CollapsingMergeTree
  *  - так как на основе порядка записей определяется, удалять ли или оставлять группы строчек при коллапсировании.
  * Не поддерживаются collations. Не поддерживается частичная сортировка.
  */
void stableSortBlock(Block & block, const SortDescription & description);

}
