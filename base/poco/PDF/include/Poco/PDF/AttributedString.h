//
// DocumentTemplate.h
//


#ifndef PDF_AttributedString_INCLUDED
#define PDF_AttributedString_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Font.h"
#include "Poco/Dynamic/Var.h"


namespace Poco {
namespace PDF {


class PDF_API AttributedString
{
public:
	enum Alignment
	{
		ALIGN_LEFT = -1,
		ALIGN_CENTER = 0,
		ALIGN_RIGHT = 1
	};

	enum Style
	{
		STYLE_PLAIN = 0,
		STYLE_BOLD = 1,
		STYLE_ITALIC = 2
	};

	enum Attributes
	{
		ATTR_FONT = 1, // font name (std::string)
		ATTR_SIZE = 2, // font size (int)
		ATTR_STYLE = 3, // style bitmask (int)
		ATTR_ALIGN = 4, // alignment (-1 = left, 0 = center, 1 = right)
	};

	AttributedString();
	~AttributedString();
	AttributedString(const char* str);
	AttributedString(const std::string& str, Alignment align = ALIGN_LEFT, int style = (int)STYLE_PLAIN);
	AttributedString& operator=(const std::string&);
	AttributedString& operator=(const char*);
	operator const std::string&();

	void setAttribute(int attr, const Poco::Dynamic::Var& value);
	Poco::Dynamic::Var getAttribute(int attr);
	void clearAttribute(int attr);

private:
	std::string _content;
	Alignment   _align;
	int         _style;
	std::string _fontName;
	int         _fontSize;
};


//
// inlines
//

inline AttributedString& AttributedString::operator=(const std::string& content)
{
	_content = content;
	return *this;
}


inline AttributedString& AttributedString::operator=(const char* content)
{
	_content = content;
	return *this;
}


inline AttributedString::operator const std::string&()
{
	return _content;
}


} } // namespace Poco::PDF


#endif // PDF_AttributedString_INCLUDED
