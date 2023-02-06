//
// Table.h
//


#ifndef PDF_Table_INCLUDED
#define PDF_Table_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Page.h"
#include "Poco/PDF/Cell.h"
#include "Poco/SharedPtr.h"
#include <string>


namespace Poco {
namespace PDF {


class PDF_API Table
{
public:
	typedef SharedPtr<Table> Ptr;
	typedef std::vector<TableRow> Cells;

	Table(int columnCount, int rowCount, const std::string& name, Cell::FontMapPtr pFontMap = 0);
	~Table();

	void setCell(int col, int row, const Cell& cell);
	void setColumnWidth(int col, double width);
	void setFonts(Cell::FontMapPtr pFontMap);

	const std::string name() const;
	const Cells& cells() const;

	void addRow();
	void addRow(const TableRow& row);

	std::size_t rows() const;
	std::size_t columns() const;

	void draw(Page& page, float x, float y, float width, float height);

private:
	Table();

	std::string      _name;
	Cells            _cells;
	Cell::FontMapPtr _pFontMap;
};


//
// inlines
//

inline const std::string Table::name() const
{
	return _name;
}


inline const Table::Cells& Table::cells() const
{
	return _cells;
}


inline std::size_t Table::rows() const
{
	return _cells.size();
}


inline std::size_t Table::columns() const
{
	return _cells[0].size();
}


} } // namespace Poco::PDF


#endif // PDF_Table_INCLUDED
