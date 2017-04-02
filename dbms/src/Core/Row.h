#pragma once

#include <vector>

#include <Common/AutoArray.h>
#include <Core/Field.h>


namespace DB
{

/** Тип данных для представления одной строки таблицы в оперативке.
  * Внимание! Предпочтительно вместо единичных строк хранить блоки столбцов. См. Block.h
  */

using Row = AutoArray<Field>;

}
