#pragma once

#include <DB/Columns/IColumnDummy.h>


namespace DB
{

/** Содержит промежуточные данные для вычисления выражений в функциях высшего порядка.
  * Это - вложенный столбец произвольного размера.
  * Сам ColumnReplicated притворяется, как столбец указанного в конструкторе размера.
  */
class ColumnReplicated final : public IColumnDummy
{
public:
	ColumnReplicated(size_t s_, ColumnPtr nested_) : IColumnDummy(s_), nested(nested_) {}
	std::string getName() const override { return "ColumnReplicated"; }
	ColumnPtr cloneDummy(size_t s_) const override { return new ColumnReplicated(s_, nested); }

	ColumnPtr & getData() { return nested; }
private:
	ColumnPtr nested;
};

}
