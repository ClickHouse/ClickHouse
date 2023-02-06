//
// Table.cpp
//

#include "Poco/PDF/Table.h"


namespace Poco {
namespace PDF {


Table::Table(int columnCount, int rowCount, const std::string& name, Cell::FontMapPtr pFontMap):
	_name(name), _cells(rowCount, TableRow(columnCount))
{
	setFonts(pFontMap);
}


Table::~Table()
{
}


void Table::addRow()
{
	_cells.push_back(TableRow(columns()));
}


void Table::addRow(const TableRow& row)
{
	if (_cells.empty())
	{
		_cells.push_back(row);
	}
	else
	{
		_cells.push_back(row);
		_cells.back().resize(_cells.front().size());
	}
}


void Table::setCell(int col, int row, const Cell& cell)
{
	_cells[row][col] = cell;
	if (_pFontMap && !cell.getFonts()) _cells[row][col].setFonts(_pFontMap);
}


void Table::setColumnWidth(int col, double width)
{
}


void Table::setFonts(Cell::FontMapPtr pFontMap)
{
	_pFontMap = pFontMap;
	if (_pFontMap) { poco_assert(_pFontMap->size() == 4); }
}


void Table::draw(Page& page, float x, float y, float width, float height)
{
	if (_cells.size())
	{
		int rows = _cells.size();
		int cols = _cells[0].size();
		int r = 0;
		for (Cells::iterator it = _cells.begin(); it != _cells.end(); ++it)
		{
			TableRow& row(*it);
			float h = height / rows;
			int c = 0;
			for (TableRow::iterator itr = row.begin(); itr != row.end(); ++itr)
			{
				Cell& cell(*itr);
				float w = width / cols;
				cell.draw(page, x + (w * c), y - (h * r), w, h);
				++c;
			}
			++r;
		}
	}
}


} } // namespace Poco::PDF
