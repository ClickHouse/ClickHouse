//
// Cell.h
//


#ifndef PDF_Cell_INCLUDED
#define PDF_Cell_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Page.h"
#include "Poco/PDF/AttributedString.h"
#include "Poco/SharedPtr.h"
#include <string>


namespace Poco {
namespace PDF {


class PDF_API Cell
{
public:
	typedef SharedPtr<Cell> Ptr;
	typedef std::map<int, std::string> FontMap;
	typedef SharedPtr<FontMap> FontMapPtr;

	enum Outline
	{
		OUTLINE_NONE = 0,
		OUTLINE_LEFT = 1,
		OUTLINE_TOP = 2,
		OUTLINE_RIGHT = 4,
		OUTLINE_BOTTOM = 8
	};

	Cell(const AttributedString& content = "", const std::string& name = "", FontMapPtr pFontMap = 0);
	Cell(const AttributedString& content, FontMapPtr pFontMap, const std::string& encoding = "UTF-8" , bool trueType = true);
	~Cell();

	const std::string& getName() const;
	void setName(const std::string& name);
	const AttributedString& getContent() const;
	void setContent(const AttributedString& content);
	unsigned getOutline() const;
	void setOutline(Outline outline, bool show = true);
	void borderLeft(bool show = true);
	void borderTop(bool show = true);
	void borderRight(bool show = true);
	void borderBottom(bool show = true);
	void borderTopBottom(bool show = true);
	void borderLeftRight(bool show = true);
	void borderAll(bool show = true);
	float getLineWidth() const;
	void setLineWidth(float width);
	void setFonts(FontMapPtr pFontMap);
	FontMapPtr getFonts() const { return _pFontMap; }
	void draw(Page& page, float x, float y, float width, float height);

private:
	AttributedString   _content;
	std::string        _name;
	unsigned           _outline;
	float              _lineWidth;
	FontMapPtr         _pFontMap;
	std::string        _encoding;
	bool               _trueType;
};


typedef std::vector<Cell> TableRow;


//
// inlines
//

inline const std::string& Cell::getName() const
{
	return _name;
}


inline void Cell::setName(const std::string& name)
{
	_name = name;
}


inline const AttributedString& Cell::getContent() const
{
	return _content;
}


inline void Cell::setContent(const AttributedString& content)
{
	_content = content;
}


inline unsigned Cell::getOutline() const
{
	return _outline;
}


inline void Cell::setOutline(Cell::Outline outline, bool show)
{
	if (show) _outline |= outline;
	else      _outline &= ~outline;
}


inline void Cell::borderLeft(bool show)
{
	setOutline(OUTLINE_LEFT, show);
}


inline void Cell::borderTop(bool show)
{
	setOutline(OUTLINE_TOP, show);
}


inline void Cell::borderRight(bool show)
{
	setOutline(OUTLINE_RIGHT, show);
}


inline void Cell::borderBottom(bool show)
{
	setOutline(OUTLINE_BOTTOM, show);
}


inline float Cell::getLineWidth() const
{
	return _lineWidth;
}


inline void Cell::setLineWidth(float width)
{
	_lineWidth = width;
}


} } // namespace Poco::PDF


#endif // PDF_Cell_INCLUDED
