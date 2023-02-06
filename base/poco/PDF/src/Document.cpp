//
// Document.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  Document
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/Document.h"
#include "Poco/PDF/PDFException.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/LocalDateTime.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/StringTokenizer.h"
#include "Poco/NumberParser.h"
#include <utility>


namespace Poco {
namespace PDF {


Document::Document(const std::string fileName,
	Poco::UInt32 pageCount,
	Page::Size pageSize,
	Page::Orientation orientation): 
	_pdf(HPDF_New(HPDF_Error_Handler, 0)),
	_fileName(fileName),
	_pRawData(0),
	_size(0)
{
	init(pageCount, pageSize, orientation);
}


Document::Document(Poco::UInt32 pageCount,
	Page::Size pageSize,
	Page::Orientation orientation): 
	_pdf(HPDF_New(HPDF_Error_Handler, 0)),
	_pRawData(0),
	_size(0)
{
	init(pageCount, pageSize, orientation);
}


Document::~Document()
{
	HPDF_Free(_pdf);
	delete _pRawData;
}


void Document::init(Poco::UInt32 pageCount,
	Page::Size pageSize, Page::Orientation orientation)
{
	useUTF8Encoding();
	compression(COMPRESSION_ALL);
	for (Poco::UInt32 i = 0; i < pageCount; ++i)
		addPage(pageSize, orientation);
}


void Document::createNew(bool resetAll)
{
	reset(resetAll);
	HPDF_NewDoc(_pdf);
}


void Document::save(const std::string fileName)
{
	std::string fn = fileName.empty() ? _fileName : fileName;
	if (fn.empty())
		HPDF_SaveToStream (_pdf);
	else
		HPDF_SaveToFile(_pdf, fn.c_str());
}


const Document::DataPtr Document::data(SizeType& sz)
{
	sz = size();
	delete _pRawData;
	_pRawData = new HPDF_BYTE[sz];
	HPDF_ReadFromStream(_pdf, _pRawData, &sz);
	return _pRawData;
}


Document::SizeType Document::size()
{
	if (HPDF_Stream_Validate(_pdf->stream)) HPDF_ResetStream(_pdf);
	HPDF_SaveToStream(_pdf);
	return _size = HPDF_GetStreamSize(_pdf);
}


const Page& Document::addPage(Page::Size pageSize, Page::Orientation orientation)
{
	Page page(this, HPDF_AddPage(_pdf), pageSize);
	page.setSizeAndOrientation(pageSize, orientation);
	_pages.push_back(page);
	return _pages.back();
}


const Page& Document::insertPage(int index,
	Page::Size pageSize,
	Page::Orientation orientation)
{
	poco_assert (index > 0);
	poco_assert (index < _pages.size());
	HPDF_Page target = *((HPDF_Page*) HPDF_List_ItemAt(_pdf->page_list, static_cast<HPDF_UINT>(index)));
	return *_pages.insert(_pages.begin() + index,
		Page(this,
			HPDF_InsertPage(_pdf, target),
			pageSize,
			orientation));
}


const Page& Document::getCurrentPage()
{
	Page p(this, HPDF_GetCurrentPage(_pdf));
	PageContainer::iterator it = _pages.begin();
	PageContainer::iterator end = _pages.end();
	for (;it != end; ++it)
		if (*it == p) return *it;

	throw NotFoundException("Current page not found.");
}


const Font& Document::loadFont(const std::string& name, const std::string& encoding)
{
	Font font(&_pdf, HPDF_GetFont(_pdf, name.c_str(), encoding.empty() ? 0 : encoding.c_str()));
	std::pair<FontContainer::iterator, bool> ret = 
		_fonts.insert(FontContainer::value_type(name, font));

	if (ret.second) return ret.first->second;

	throw IllegalStateException("Could not create font.");
}


const Font& Document::font(const std::string& name, const std::string& encoding)
{
	FontContainer::iterator it = _fonts.find(name);
	if (_fonts.end() != it) return it->second;

	return loadFont(name, encoding);
}


std::string Document::loadType1Font(const std::string& afmFileName, const std::string& pfmFileName)
{
	return HPDF_LoadType1FontFromFile(_pdf, afmFileName.c_str(), pfmFileName.c_str());
}


std::string Document::loadTTFont(const std::string& fileName, bool embed, int index)
{
	if (-1 == index)
	{
		return HPDF_LoadTTFontFromFile(_pdf,
			fileName.c_str(),
			embed ? HPDF_TRUE : HPDF_FALSE);
	}
	else if (index >= 0)
	{
		return HPDF_LoadTTFontFromFile2(_pdf, 
			fileName.c_str(), 
			static_cast<HPDF_UINT>(index), 
			embed ? HPDF_TRUE : HPDF_FALSE);
	}
	else
		throw InvalidArgumentException("Invalid font index.");
}


const Image& Document::loadPNGImageImpl(const std::string& fileName, bool doLoad)
{
	Path path(fileName);

	if (File(path).exists())
	{
		std::pair<ImageContainer::iterator, bool> it;
		if (doLoad)
		{
			Image image(&_pdf, HPDF_LoadPngImageFromFile(_pdf, fileName.c_str()));
			it = _images.insert(ImageContainer::value_type(path.getBaseName(), image));
		}
		else
		{
			Image image(&_pdf, HPDF_LoadPngImageFromFile2(_pdf, fileName.c_str()));
			it = _images.insert(ImageContainer::value_type(path.getBaseName(), image));
		}
		if (it.second) return it.first->second;
		else throw IllegalStateException("Could not insert image.");
	}
	else 
		throw NotFoundException("File not found: " + fileName);
}


const Image& Document::loadJPEGImage(const std::string& fileName)
{
	Path path(fileName);

	if (File(path).exists())
	{
		Image image(&_pdf, HPDF_LoadJpegImageFromFile(_pdf, fileName.c_str()));
		std::pair<ImageContainer::iterator, bool> it =
			_images.insert(ImageContainer::value_type(path.getBaseName(), image));
		if (it.second) return it.first->second;
		else throw IllegalStateException("Could not insert image.");
	}
	else 
		throw NotFoundException("File not found: " + fileName);
}


void Document::encryption(Encryption mode, Poco::UInt32 keyLength)
{
	if (ENCRYPT_R3 == mode && (keyLength < 5 || keyLength > 128))
		throw InvalidArgumentException("Invalid key length.");

	HPDF_SetEncryptionMode(_pdf,
		static_cast<HPDF_EncryptMode>(mode),
		static_cast<HPDF_UINT>(keyLength));
}


const Encoder& Document::loadEncoder(const std::string& name)
{
	EncoderContainer::iterator it = _encoders.find(name);
	if (_encoders.end() == it) return it->second;

	Encoder enc(&_pdf, HPDF_GetEncoder(_pdf, name.c_str()), name);
	std::pair<EncoderContainer::iterator, bool> ret = 
		_encoders.insert(EncoderContainer::value_type(name, enc));

	if (ret.second) return ret.first->second;

	throw IllegalStateException("Could not create encoder.");
}


const Encoder& Document::getCurrentEncoder()
{
	HPDF_Encoder enc = HPDF_GetCurrentEncoder(_pdf);
	std::string name = enc->name;
	EncoderContainer::iterator it = _encoders.find(name);
	if (_encoders.end() == it)
	{
		std::pair<EncoderContainer::iterator, bool> ret =
			_encoders.insert(EncoderContainer::value_type(name, Encoder(&_pdf, enc)));

		if (ret.second) return ret.first->second;
	}
	
	return it->second;
}


const Encoder& Document::setCurrentEncoder(const std::string& name)
{
	loadEncoder(name);
	HPDF_SetCurrentEncoder(_pdf, name.c_str());
	return getCurrentEncoder();
}


const Outline& Document::createOutline(const std::string& title, const Outline& parent, const Encoder& encoder)
{
	_outlines.push_back(Outline(&_pdf, HPDF_CreateOutline(_pdf, parent, title.c_str(), encoder)));
	return _outlines.back();
}


void Document::setInfo(Info info, const LocalDateTime& dt)
{
	if (INFO_CREATION_DATE == info || INFO_MOD_DATE == info)
	{
		HPDF_Date hpdfDate;
		hpdfDate.year = dt.year();
		hpdfDate.month = dt.month();
		hpdfDate.day = dt.day();
		hpdfDate.hour = dt.hour();
		hpdfDate.minutes = dt.minute();
		hpdfDate.seconds = dt.second();
		std::string tzd = DateTimeFormatter::tzdISO(dt.tzd());
		StringTokenizer st(tzd, "+-:");
		if (st.count() >= 2)
		{
			hpdfDate.ind = tzd[0];
			hpdfDate.off_hour = NumberParser::parse(st[0]);
			hpdfDate.off_minutes = NumberParser::parse(st[1]);
		}
		else hpdfDate.ind = ' ';

		HPDF_SetInfoDateAttr(_pdf, static_cast<HPDF_InfoType>(info), hpdfDate);
	}
	else
		throw InvalidArgumentException("Can not set document info.");
}


void Document::setInfo(Info info, const std::string& value)
{
	if (INFO_CREATION_DATE == info || INFO_MOD_DATE == info)
		throw InvalidArgumentException("Can not set document date.");
	
	HPDF_SetInfoAttr(_pdf, static_cast<HPDF_InfoType>(info), value.c_str());
}


void Document::setPassword(const std::string& ownerPassword, const std::string& userPassword)
{
	if (ownerPassword.empty())
		throw InvalidArgumentException("Owner password empty.");

	HPDF_SetPassword(_pdf, ownerPassword.c_str(), userPassword.c_str());
}


void Document::reset(bool all)
{
	if (!all) HPDF_FreeDoc(_pdf);
	else
	{
		_pages.clear();
		_fonts.clear();
		_encoders.clear();
		_outlines.clear();
		_images.clear();
		HPDF_FreeDocAll(_pdf);
	}
}


} } // namespace Poco::PDF
