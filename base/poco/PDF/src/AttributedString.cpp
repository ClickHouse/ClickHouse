//
// AttributedString.cpp
//

#include "Poco/PDF/AttributedString.h"
#include "Poco/Format.h"

namespace Poco {
namespace PDF {


AttributedString::AttributedString():
	_align(ALIGN_LEFT),
	_style(STYLE_PLAIN),
	_fontName("Helvetica"),
	_fontSize(10)
{
}


AttributedString::AttributedString(const char* content): 
	_content(content),
	_align(ALIGN_LEFT),
	_style(STYLE_PLAIN),
	_fontName("Helvetica"),
	_fontSize(10)
{
}


AttributedString::AttributedString(const std::string& content, Alignment align, int style):
	_content(content), 
	_align(align), 
	_style(static_cast<Style>(style)),
	_fontName("Helvetica"),
	_fontSize(10)
{
}


AttributedString::~AttributedString()
{
}


void AttributedString::setAttribute(int attr, const Poco::Dynamic::Var& value)
{
	switch (attr)
	{
	case ATTR_FONT:
		_fontName = value.toString();
		return;
	case ATTR_SIZE:
		_fontSize = value;
		return;
	case ATTR_STYLE:
		_style = value.convert<int>();
		return;
	case ATTR_ALIGN:
		_align = static_cast<Alignment>(value.convert<int>());
		return;
	default:
		throw InvalidArgumentException(
			format("AttributeString::setAttribute: %d", attr));
	}
}


Poco::Dynamic::Var AttributedString::getAttribute(int attr)
{
	switch (attr)
	{
	case ATTR_FONT:  return _fontName;
	case ATTR_SIZE:  return _fontSize;
	case ATTR_STYLE: return static_cast<int>(_style);
	case ATTR_ALIGN: return static_cast<int>(_align);
	default:
		throw InvalidArgumentException(
			format("AttributeString::setAttribute: %d", attr));
	}
}


void AttributedString::clearAttribute(int attr)
{
	switch (attr)
	{
	case ATTR_FONT:  _fontName = "Helvetica"; return;
	case ATTR_SIZE:  _fontSize = 10;          return;
	case ATTR_STYLE: _style    = STYLE_PLAIN; return;
	case ATTR_ALIGN: _align    = ALIGN_LEFT;  return;
	default:
		throw InvalidArgumentException(
			format("AttributeString::setAttribute: %d", attr));
	}
}


} } // namespace Poco::PDF
