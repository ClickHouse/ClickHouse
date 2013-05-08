#pragma once

#include <DB/Columns/IColumnDummy.h>
#include <DB/Interpreters/Set.h>


namespace DB
{

/** Столбец, содержащий множество значений в секции IN.
  * Ведёт себя как столбец-константа (так как множество одно, а не своё на каждую строку).
  * Значение у этого столбца нестандартное, поэтому его невозможно получить через обычный интерфейс.
  */
class ColumnSet : public IColumnDummy
{
public:
	ColumnSet(size_t s_, SetPtr data_) : IColumnDummy(s_), data(data_) {}

	std::string getName() const { return "ColumnSet"; }
	ColumnPtr cloneDummy(size_t s_) const { return new ColumnSet(s_, data); }
	
	SetPtr & getData() { return data; }
	const SetPtr & getData() const { return data; }

private:
	SetPtr data;
};

}
