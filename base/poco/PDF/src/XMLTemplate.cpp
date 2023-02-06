//
// XMLTemplate.cpp
//


#include "Poco/PDF/XMLTemplate.h"
#include "Poco/PDF/Table.h"
#include "Poco/SAX/SAXParser.h"
#include "Poco/SAX/DefaultHandler.h"
#include "Poco/SAX/Attributes.h"
#include "Poco/SAX/InputSource.h"
#include "Poco/Util/PropertyFileConfiguration.h"
#include "Poco/FileStream.h"
#include "Poco/AutoPtr.h"
#include "Poco/String.h"
#include "Poco/Exception.h"
#include "Poco/Path.h"
#include "Poco/TextConverter.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/UTF8String.h"
#include <vector>
#include <set>
#include <sstream>
#include <algorithm>


namespace Poco {
namespace PDF {


class StackedConfiguration : public Poco::Util::AbstractConfiguration
{
public:
	typedef Poco::AutoPtr<Poco::Util::AbstractConfiguration> ConfigPtr;
	typedef std::vector<ConfigPtr> ConfigStack;

	void push(ConfigPtr pConfig)
	{
		_stack.push_back(pConfig);
	}

	void pop()
	{
		_stack.pop_back();
	}

	ConfigPtr current() const
	{
		poco_assert(_stack.size() > 0);
		return _stack.back();
	}

	float getFloat(const std::string& value)
	{
		return static_cast<float>(getDouble(value));
	}

	float getFloat(const std::string& value, float deflt)
	{
		return static_cast<float>(getDouble(value, deflt));
	}

	// AbstractConfiguration
	bool getRaw(const std::string& key, std::string& value) const
	{
		for (ConfigStack::const_reverse_iterator it = _stack.rbegin(); it != _stack.rend(); ++it)
		{
			if ((*it)->has(key))
			{
				value = (*it)->getRawString(key);
				return true;
			}
		}
		return false;
	}

	void setRaw(const std::string& key, const std::string& value)
	{
		throw Poco::InvalidAccessException("not writable");
	}

	void enumerate(const std::string& key, Poco::Util::AbstractConfiguration::Keys& range) const
	{
		std::set<std::string> keys;
		for (ConfigStack::const_iterator itc = _stack.begin(); itc != _stack.end(); ++itc)
		{
			Poco::Util::AbstractConfiguration::Keys partRange;
			(*itc)->keys(key, partRange);
			for (Poco::Util::AbstractConfiguration::Keys::const_iterator itr = partRange.begin(); itr != partRange.end(); ++itr)
			{
				if (keys.find(*itr) == keys.end())
				{
					range.push_back(*itr);
					keys.insert(*itr);
				}
			}
		}
	}

	void removeRaw(const std::string& key)
	{
		throw Poco::InvalidAccessException("not writable");
	}

private:
	std::vector<ConfigPtr> _stack;
};


class Box
{
public:
	Box() :
		_x(0),
		_y(0),
		_width(0),
		_height(0)
	{
	}

	Box(float x, float y, float width, float height) :
		_x(x),
		_y(y),
		_width(width),
		_height(height)
	{

	}

	Box(const Box& box) :
		_x(box._x),
		_y(box._y),
		_width(box._width),
		_height(box._height)
	{
	}

	~Box()
	{
	}

	Box& operator = (const Box& box)
	{
		Box tmp(box);
		tmp.swap(*this);
		return *this;
	}

	void swap(Box& box)
	{
		std::swap(_x, box._x);
		std::swap(_y, box._y);
		std::swap(_width, box._width);
		std::swap(_height, box._height);
	}

	float left() const
	{
		return _x;
	}

	float right() const
	{
		return _x + _width;
	}

	float bottom() const
	{
		return _y;
	}

	float top() const
	{
		return _y + _height;
	}

	float width() const
	{
		return _width;
	}

	float height() const
	{
		return _height;
	}

	void inset(float delta)
	{
		_x += delta;
		_y -= delta;
		_width -= 2 * delta;
		_height -= 2 * delta;
	}

	void extend(float delta)
	{
		inset(-delta);
	}

	void inset(float left, float right, float top, float bottom)
	{
		_x += left;
		_y += bottom;
		_width -= (left + right);
		_height -= (top + bottom);
	}

private:
	float _x;
	float _y;
	float _width;
	float _height;
};


class TemplateHandler : public Poco::XML::DefaultHandler
{
public:
	typedef Poco::AutoPtr<Poco::Util::AbstractConfiguration> StylePtr;

	TemplateHandler(const Poco::Path& base) :
		_base(base),
		_pDocument(0),
		_pPage(0),
		_y(0)
	{
		_styles.push(parseStyle("font-family: Helvetica; font-size: 12; line-height: 1.2"));
	}

	~TemplateHandler()
	{
		_styles.pop();
		delete _pPage;
		delete _pDocument;
	}

	Document* document()
	{
		Document* pDocument = _pDocument;
		_pDocument = 0;
		return pDocument;
	}

	void startDoc(const Poco::XML::Attributes& attributes)
	{
		if (_pDocument) throw Poco::IllegalStateException("only one <document> element is allowed");

		StylePtr pStyle = pushStyle(attributes);

		_size = attributes.getValue("size");
		if (_size.empty()) _size = "A4";
		_orientation = attributes.getValue("orientation");
		if (_orientation.empty()) _orientation = "portrait";

		_encoding = attributes.getValue("encoding");
		if (_encoding.empty()) _encoding = "WinAnsiEncoding";

		_pDocument = new Document(0, parsePageSize(_size), parseOrientation(_orientation));
	}

	void endDoc()
	{
		popStyle();
	}

	void startPage(const Poco::XML::Attributes& attributes)
	{
		if (!_pDocument) throw Poco::IllegalStateException("missing <document> element");
		if (_pPage) throw Poco::IllegalStateException("nested <page> elements are not allowed");

		StylePtr pStyle = pushStyle(attributes);

		std::string size = attributes.getValue("size");
		if (size.empty()) size = _size;
		std::string orientation = attributes.getValue("orientation");
		if (orientation.empty()) orientation = _orientation;

		_pPage = new Page(_pDocument->addPage(parsePageSize(_size), parseOrientation(orientation)));
		_pPage->setLineWidth(0.2f);
		RGBColor black = {0, 0, 0};
		_pPage->setRGBStroke(black);
		_boxes.push_back(Box(0, 0, _pPage->getWidth(), _pPage->getHeight()));

		float margin = _styles.getFloat("margin", 0);
		float marginLeft = _styles.getFloat("margin-left", margin);
		float marginRight = _styles.getFloat("margin-right", margin);
		float marginTop = _styles.getFloat("margin-top", margin);
		float marginBottom = _styles.getFloat("margin-bottom", margin);

		_boxes.back().inset(marginLeft, marginRight, marginTop, marginBottom);

		_y = _boxes.back().top();
	}

	void endPage()
	{
		_boxes.pop_back();
		delete _pPage;
		_pPage = 0;
		popStyle();
	}

	void startSpan(const Poco::XML::Attributes& attributes)
	{
		if (!_pPage) throw Poco::IllegalStateException("missing <page> element");

		StylePtr pStyle = pushStyle(attributes);

		_text.clear();
	}

	void endSpan()
	{
		std::string fontFamily = _styles.getString("font-family");
		float fontSize = _styles.getFloat("font-size");
		float lineHeight = _styles.getFloat("line-height");
		std::string textAlign = _styles.getString("text-align", "left");
		std::string fontStyle = _styles.getString("font-style", "normal");
		std::string fontWeight = _styles.getString("font-weight", "normal");
		std::string textTransform = _styles.getString("text-transform", "none");

		_text = transform(_text, textTransform);
		_text = transcode(_text);

		Font font = loadFont(fontFamily, fontStyle, fontWeight);

		_pPage->setFont(font, fontSize);

		float width = static_cast<float>(font.textWidth(_text).width*fontSize / 1000);
		float height = static_cast<float>(font.upperHeight()*fontSize / 1000)*lineHeight;

		float x = static_cast<float>(_styles.current()->getDouble("left", _styles.current()->getDouble("right", width) - width));
		float y = static_cast<float>(_styles.current()->getDouble("bottom", _styles.current()->getDouble("top", _y) - height));

		if (textAlign == "center")
			x = (box().width() - width) / 2;
		else if (textAlign == "right")
			x = box().width() - width;

		translateInBox(x, y);

		_pPage->writeOnce(x, y, _text);

		moveY(y);

		popStyle();
	}

	void startImg(const Poco::XML::Attributes& attributes)
	{
		if (!_pPage) throw Poco::IllegalStateException("missing <page> element");

		StylePtr pStyle = pushStyle(attributes);

		std::string path = attributes.getValue("src");
		Image image = loadImage(path);

		float width = static_cast<float>(pStyle->getDouble("width", image.width()));
		float height = static_cast<float>(pStyle->getDouble("height", image.height()));

		float scale = static_cast<float>(pStyle->getDouble("scale", 1.0));
		float scaleX = static_cast<float>(pStyle->getDouble("scale-x", scale));
		float scaleY = static_cast<float>(pStyle->getDouble("scale-y", scale));

		width *= scaleX;
		height *= scaleY;

		float x = static_cast<float>(_styles.current()->getDouble("left", _styles.current()->getDouble("right", width) - width));
		float y = static_cast<float>(_styles.current()->getDouble("bottom", _styles.current()->getDouble("top", _y) - height));

		translateInBox(x, y);

		_pPage->drawImage(image, x, y, width, height);

		moveY(y);
	}

	void endImg()
	{
		popStyle();
	}

	void startTable(const Poco::XML::Attributes& attributes)
	{
		if (!_pPage) throw Poco::IllegalStateException("missing <page> element");

		StylePtr pStyle = pushStyle(attributes);

		_pTable = new Table(0, 0, attributes.getValue("name"));
	}

	void endTable()
	{
		if (_pTable->rows() > 0)
		{
			std::string fontFamily = _styles.getString("font-family");
			float fontSize = _styles.getFloat("font-size");
			std::string fontStyle = _styles.getString("font-style", "normal");
			std::string fontWeight = _styles.getString("font-weight", "normal");

			Font font = loadFont(fontFamily, fontStyle, fontWeight);

			float lineHeight = static_cast<float>((font.ascent() - font.descent())*fontSize / 1000)*_styles.getFloat("line-height");

			float width = static_cast<float>(_styles.current()->getDouble("width", box().width()));
			float height = static_cast<float>(_styles.current()->getDouble("height", _pTable->rows()*lineHeight));

			float x = static_cast<float>(_styles.current()->getDouble("left", _styles.current()->getDouble("right", width) - width));
			float y = static_cast<float>(_styles.current()->getDouble("bottom", _styles.current()->getDouble("top", _y) - height) + height);
			y -= lineHeight;

			translateInBox(x, y);

			_pTable->draw(*_pPage, x, y, width, height);

			y -= height - lineHeight;

			moveY(y);
		}
		_pTable = 0;
		popStyle();
	}

	void startTr(const Poco::XML::Attributes& attributes)
	{
		if (!_pTable) throw Poco::IllegalStateException("missing <table> element");

		StylePtr pStyle = pushStyle(attributes);
		_row.clear();
	}

	void endTr()
	{
		_pTable->addRow(_row);
		_row.clear();
		popStyle();
	}

	void startTd(const Poco::XML::Attributes& attributes)
	{
		StylePtr pStyle = pushStyle(attributes);

		_text.clear();
	}

	void endTd()
	{
		AttributedString::Alignment align = AttributedString::ALIGN_LEFT;
		int style = AttributedString::STYLE_PLAIN;

		std::string fontFamily = _styles.getString("font-family");
		float       fontSize = _styles.getFloat("font-size");
		std::string textAlign = _styles.getString("text-align", "left");
		std::string fontStyle = _styles.getString("font-style", "normal");
		std::string fontWeight = _styles.getString("font-weight", "normal");
		std::string textTransform = _styles.getString("text-transform", "none");

		_text = transform(_text, textTransform);
		_text = transcode(_text);

		if (textAlign == "right")
			align = AttributedString::ALIGN_RIGHT;
		else if (textAlign == "left")
			align = AttributedString::ALIGN_LEFT;

		if (fontStyle == "italic" || fontStyle == "oblique")
			style |= AttributedString::STYLE_ITALIC;

		if (fontWeight == "bold")
			style |= AttributedString::STYLE_BOLD;

		AttributedString content(_text, align, style);
		content.setAttribute(AttributedString::ATTR_SIZE, fontSize);

		Cell::FontMapPtr pFontMap = new Cell::FontMap;
		std::string normalizedFontFamily(normalizeFontName(fontFamily));
		(*pFontMap)[AttributedString::STYLE_PLAIN] = normalizedFontFamily;
		(*pFontMap)[AttributedString::STYLE_BOLD] = boldFontName(normalizedFontFamily);
		(*pFontMap)[AttributedString::STYLE_ITALIC] = italicFontName(normalizedFontFamily);
		(*pFontMap)[AttributedString::STYLE_BOLD | AttributedString::STYLE_ITALIC] = boldItalicFontName(normalizedFontFamily);

		_row.push_back(Cell(content, pFontMap, _encoding, false));

		popStyle();
	}

	void startHr(const Poco::XML::Attributes& attributes)
	{
		if (!_pPage) throw Poco::IllegalStateException("missing <page> element");

		StylePtr pStyle = pushStyle(attributes);

		float width = static_cast<float>(_styles.current()->getDouble("width", box().width()));
		float height = static_cast<float>(_styles.current()->getDouble("height", 0.2));

		float x = static_cast<float>(_styles.current()->getDouble("left", _styles.current()->getDouble("right", width) - width));
		float y = static_cast<float>(_styles.current()->getDouble("bottom", _styles.current()->getDouble("top", _y) - height));

		translateInBox(x, y);

		_pPage->moveTo(x, y);
		_pPage->lineTo(x + width, y);
		_pPage->stroke();

		moveY(y);
	}

	void endHr()
	{
		popStyle();
	}

	// DocumentHandler
	void startDocument()
	{
	}

	void endDocument()
	{
	}

	void startElement(const Poco::XML::XMLString& uri, const Poco::XML::XMLString& localName, const Poco::XML::XMLString& qname, const Poco::XML::Attributes& attributes)
	{
		if (localName == "document")
			startDoc(attributes);
		else if (localName == "page")
			startPage(attributes);
		else if (localName == "span")
			startSpan(attributes);
		else if (localName == "img")
			startImg(attributes);
		else if (localName == "table")
			startTable(attributes);
		else if (localName == "tr")
			startTr(attributes);
		else if (localName == "td")
			startTd(attributes);
		else if (localName == "hr")
			startHr(attributes);
	}

	void endElement(const Poco::XML::XMLString& uri, const Poco::XML::XMLString& localName, const Poco::XML::XMLString& qname)
	{
		if (localName == "document")
			endDoc();
		else if (localName == "page")
			endPage();
		else if (localName == "span")
			endSpan();
		else if (localName == "img")
			endImg();
		else if (localName == "table")
			endTable();
		else if (localName == "tr")
			endTr();
		else if (localName == "td")
			endTd();
		else if (localName == "hr")
			endHr();
	}

	void characters(const Poco::XML::XMLChar ch[], int start, int length)
	{
		_text.append(ch + start, length);
	}

protected:
	StylePtr pushStyle(const Poco::XML::Attributes& attributes)
	{
		StylePtr pStyle = parseStyle(attributes);
		if (_boxes.size() > 0)
		{
			pStyle->setDouble("box.width", box().width());
			pStyle->setDouble("box.height", box().height());
		}
		_styles.push(pStyle);
		return pStyle;
	}

	void popStyle()
	{
		_styles.pop();
	}

	StylePtr parseStyle(const Poco::XML::Attributes& attributes)
	{
		return parseStyle(attributes.getValue("style"));
	}

	StylePtr parseStyle(const std::string& style) const
	{
		std::string props = Poco::translate(style, ";", "\n");
		std::istringstream istr(props);
		return new Poco::Util::PropertyFileConfiguration(istr);
	}

	static Page::Size parsePageSize(const std::string& size)
	{
		using Poco::icompare;
		if (icompare(size, "letter") == 0)
			return Page::PAGE_SIZE_LETTER;
		else if (icompare(size, "legal") == 0)
			return Page::PAGE_SIZE_LEGAL;
		else if (icompare(size, "a3") == 0)
			return Page::PAGE_SIZE_A3;
		else if (icompare(size, "a4") == 0)
			return Page::PAGE_SIZE_A4;
		else if (icompare(size, "a5") == 0)
			return Page::PAGE_SIZE_A5;
		else if (icompare(size, "b4") == 0)
			return Page::PAGE_SIZE_B4;
		else if (icompare(size, "b5") == 0)
			return Page::PAGE_SIZE_B5;
		else if (icompare(size, "executive") == 0)
			return Page::PAGE_SIZE_EXECUTIVE;
		else if (icompare(size, "us4x6") == 0)
			return Page::PAGE_SIZE_US4x6;
		else if (icompare(size, "us4x8") == 0)
			return Page::PAGE_SIZE_US4x8;
		else if (icompare(size, "us5x7") == 0)
			return Page::PAGE_SIZE_US5x7;
		else if (icompare(size, "comm10") == 0)
			return Page::PAGE_SIZE_COMM10;
		else throw Poco::InvalidArgumentException("size", size);
	}

	static Page::Orientation parseOrientation(const std::string& orientation)
	{
		if (icompare(orientation, "portrait") == 0)
			return Page::ORIENTATION_PORTRAIT;
		else if (icompare(orientation, "landscape") == 0)
			return Page::ORIENTATION_LANDSCAPE;
		else throw Poco::InvalidArgumentException("orientation", orientation);
	}

	std::string normalizeFontName(const std::string& fontFamily)
	{
		poco_assert(fontFamily.size());

		std::string fontName = toLower(fontFamily);
		fontName[0] = Poco::Ascii::toUpper(fontName[0]);
		return fontName;
	}

	Font loadFont(const std::string& fontFamily, const std::string& fontStyle, const std::string& fontWeight)
	{
		poco_assert(_pDocument);

		std::string normalizedFontFamily = normalizeFontName(fontFamily);
		std::string fontName;
		if (fontStyle == "italic" || fontStyle == "oblique")
		{
			if (fontWeight == "bold")
				fontName = boldItalicFontName(normalizedFontFamily);
			else
				fontName = italicFontName(normalizedFontFamily);
		} else if (fontWeight == "bold")
		{
			fontName = boldFontName(normalizedFontFamily);
		} else
		{
			fontName = normalizedFontFamily;
		}
		return _pDocument->font(fontName, _encoding);
	}

	std::string boldFontName(const std::string& fontFamily)
	{
		try
		{
			return _pDocument->font(fontFamily + "-Bold", _encoding).name();
		} catch (...)
		{
		}
		return fontFamily;
	}

	std::string italicFontName(const std::string& fontFamily)
	{
		try
		{
			return _pDocument->font(fontFamily + "-Oblique", _encoding).name();
		} catch (...)
		{
		}
		try
		{
			return _pDocument->font(fontFamily + "-Italic", _encoding).name();
		} catch (...)
		{
		}
		return fontFamily;
	}

	std::string boldItalicFontName(const std::string& fontFamily)
	{
		try
		{
			return _pDocument->font(fontFamily + "-BoldOblique", _encoding).name();
		} catch (...)
		{
		}
		try
		{
			Font font = _pDocument->font(fontFamily + "-BoldItalic", _encoding);
		} catch (...)
		{
		}
		return fontFamily;
	}

	Image loadImage(const std::string& path)
	{
		Poco::Path p(_base);
		p.resolve(path);
		if (Poco::icompare(p.getExtension(), "jpg") == 0 || icompare(p.getExtension(), "jpeg") == 0)
			return _pDocument->loadJPEGImage(p.toString());
		else if (Poco::icompare(p.getExtension(), "png") == 0)
			return _pDocument->loadPNGImage(p.toString());
		else
			throw Poco::InvalidArgumentException("cannot determine image type", path);
	}

	void translateInBox(float& x, float& y)
	{
		if (x < 0)
			x = box().right() + x;
		else
			x += box().left();

		if (y < 0)
			y = box().top() + y;
		else
			y += box().bottom();
	}

	void moveY(float y)
	{
		float padding = _styles.getFloat("padding", 0);
		float newY = y - padding;
		newY -= box().bottom();

		_y = std::min(_y, newY);
	}

	const Box& box() const
	{
		poco_assert(_boxes.size() > 0);
		return _boxes.back();
	}

	std::string transcode(const std::string& text)
	{
		std::string result;
		Poco::UTF8Encoding inEncoding;
		Poco::TextEncoding& outEncoding = Poco::TextEncoding::byName(mapEncoding(_encoding));
		Poco::TextConverter converter(inEncoding, outEncoding);
		converter.convert(text, result);
		return result;
	}

	static std::string mapEncoding(const std::string& encoding)
	{
		if (encoding == "WinAnsiEncoding")
			return "Latin-1";
		else if (encoding == "ISO8859-2")
			return "Latin-2";
		else if (encoding == "ISO8859-15")
			return "Latin-9";
		else if (encoding == "CP1250")
			return "CP1250";
		else if (encoding == "CP1251")
			return "CP1251";
		else if (encoding == "CP1252")
			return "CP1252";
		else
			throw Poco::InvalidArgumentException("PDF Document encoding not supported", encoding);
	}

	static std::string transform(const std::string& text, const std::string& trans)
	{
		if (trans == "uppercase")
			return UTF8::toUpper(text);
		else if (trans == "lowercase")
			return UTF8::toLower(text);
		else
			return text;
	}

private:
	Poco::Path _base;
	std::string _encoding;
	Document* _pDocument;
	Page* _pPage;
	Table::Ptr _pTable;
	TableRow _row;
	StackedConfiguration _styles;
	std::vector<Box> _boxes;
	std::string _size;
	std::string _orientation;
	std::string _text;
	float _y;
};


XMLTemplate::XMLTemplate(std::istream& xmlStream, const std::string& base) :
	_base(base),
	_pDocument(0)
{
	load(xmlStream);
}


XMLTemplate::XMLTemplate(const std::string& path) :
	_base(path),
	_pDocument(0)
{
	Poco::FileInputStream xmlStream(path);
	load(xmlStream);
}


XMLTemplate::~XMLTemplate()
{
	delete _pDocument;
}


void XMLTemplate::load(std::istream& xmlStream)
{
	Poco::XML::InputSource xmlSource(xmlStream);
	Poco::XML::SAXParser parser;
	TemplateHandler handler(_base);
	parser.setContentHandler(&handler);
	parser.setFeature(Poco::XML::XMLReader::FEATURE_NAMESPACES, true);
	parser.setFeature(Poco::XML::XMLReader::FEATURE_NAMESPACE_PREFIXES, true);
	parser.parse(&xmlSource);

	_pDocument = handler.document();
}


void XMLTemplate::create(const std::string& path)
{
	_pDocument->save(path);
}


} } // namespace Poco::PDF
