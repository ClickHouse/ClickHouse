//
// Page.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  Page
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/Page.h"
#include "Poco/PDF/Document.h"
#include "Poco/PDF/PDFException.h"
#undef min
#undef max
#include <limits>


namespace Poco {
namespace PDF {


Page::Page(Document* pDocument,
	const HPDF_Page& page,
	Size pageSize,
	Orientation orientation):
		_pDocument(pDocument),
		_page(page),
		_size(pageSize),
		_orientation(orientation),
		_pCurrentFont(0)
{
}


Page::Page(const Page& other):
	_pDocument(other._pDocument),
	_page(other._page),
	_size(other._size),
	_orientation(other._orientation),
	_pCurrentFont(other._pCurrentFont ? new Font(*other._pCurrentFont) : (Font*)0)
{
}


Page::~Page()
{
}


Page& Page::operator = (const Page& page)
{
	Page tmp(page);
	swap(tmp);
	return *this;
}


bool Page::operator == (const Page& other) const
{
	return &_pDocument->handle() == &other._pDocument->handle() && _page == other._page;
}


void Page::swap(Page& other)
{
	using std::swap;

	swap(_pDocument, other._pDocument);
	swap(_page, other._page);
	swap(_size, other._size);
	swap(_orientation, other._orientation);
	swap(_pCurrentFont, other._pCurrentFont);
}


void Page::writeOnce(float xPos, float yPos, const std::string& text)
{
	beginText();
	write(xPos, yPos, text);
	endText();
}


int Page::writeOnceInRectangle(float left,
		float top,
		float right,
		float bottom,
		const std::string& text,
		TextAlignment align)
{
	beginText();
	int ret = writeInRectangle(left, top, right, bottom, text, align);
	endText();
	return ret;
}


float Page::textWidth(const std::string& text)
{
	return HPDF_Page_TextWidth(_page, text.c_str());
}


void Page::setFont(const std::string& name, float size, const std::string& encoding)
{
	setFont(_pDocument->font(name, encoding), size);
}


void Page::setTTFont(const std::string& name, float size, const std::string& encoding, bool embed)
{
	setFont(_pDocument->font(_pDocument->loadTTFont(name, embed), encoding), size);
}


const Font& Page::getFont() const
{
	delete _pCurrentFont;
	return *(_pCurrentFont = new Font(&_pDocument->handle(), HPDF_Page_GetCurrentFont(_page)));
}


void Page::setRotation(int angle)
{
	if (0 != angle % 90 || angle > std::numeric_limits<HPDF_UINT16>::max())
		throw InvalidArgumentException("Invalid angle value.");

	HPDF_Page_SetRotate(_page, static_cast<HPDF_UINT16>(angle));
}


const Destination& Page::createDestination(const std::string& name)
{
	DestinationContainer::iterator it = _destinations.find(name);
	if (_destinations.end() != it) 
		throw InvalidArgumentException("Destination already exists.");

	Destination dest(&_pDocument->handle(), HPDF_Page_CreateDestination(_page), name);
	std::pair<DestinationContainer::iterator, bool> ret = 
		_destinations.insert(DestinationContainer::value_type(name, dest));

	if (ret.second) return ret.first->second;

	throw IllegalStateException("Could not create destination.");
}


const TextAnnotation& Page::createTextAnnotation(const std::string& name, 
	const Rectangle& rect,
	const std::string& text,
	const Encoder& encoder)
{
	TextAnnotationContainer::iterator it = _textAnnotations.find(name);
	if (_textAnnotations.end() != it) 
		throw InvalidArgumentException("Annotation already exists.");

	TextAnnotation ann(&_pDocument->handle(),
		HPDF_Page_CreateTextAnnot(_page, rect, text.c_str(), encoder),
		name);

	std::pair<TextAnnotationContainer::iterator, bool> ret = 
		_textAnnotations.insert(TextAnnotationContainer::value_type(name, ann));

	if (ret.second) return ret.first->second;

	throw IllegalStateException("Could not create annotation.");
}


const LinkAnnotation& Page::createLinkAnnotation(const std::string& name, 
	const Rectangle& rect,
	const Destination& dest)
{
	LinkAnnotationContainer::iterator it = _linkAnnotations.find(name);
	if (_linkAnnotations.end() != it) 
		throw InvalidArgumentException("Annotation already exists.");

	LinkAnnotation ann(&_pDocument->handle(),
		HPDF_Page_CreateLinkAnnot(_page, rect, dest),
		name);
	std::pair<LinkAnnotationContainer::iterator, bool> ret = 
		_linkAnnotations.insert(LinkAnnotationContainer::value_type(name, ann));

	if (ret.second) return ret.first->second;

	throw IllegalStateException("Could not create annotation.");
}


const LinkAnnotation& Page::createURILinkAnnotation(const std::string& name, 
	const Rectangle& rect,
	const std::string& uri)
{
	LinkAnnotationContainer::iterator it = _linkAnnotations.find(name);
	if (_linkAnnotations.end() != it) 
		throw InvalidArgumentException("Annotation already exists.");

	LinkAnnotation ann(&_pDocument->handle(),
		HPDF_Page_CreateURILinkAnnot(_page, rect, uri.c_str()),
		name);
	std::pair<LinkAnnotationContainer::iterator, bool> ret = 
		_linkAnnotations.insert(LinkAnnotationContainer::value_type(name, ann));

	if (ret.second) return ret.first->second;

	throw IllegalStateException("Could not create annotation.");
}


} } // namespace Poco::PDF
