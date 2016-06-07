//
// HTMLForm.cpp
//
// $Id: //poco/1.4/Net/src/HTMLForm.cpp#4 $
//
// Library: Net
// Package: HTML
// Module:  HTMLForm
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/PartSource.h"
#include "Poco/Net/PartHandler.h"
#include "Poco/Net/MultipartWriter.h"
#include "Poco/Net/MultipartReader.h"
#include "Poco/Net/NullPartHandler.h"
#include "Poco/Net/NetException.h"
#include "Poco/NullStream.h"
#include "Poco/CountingStream.h"
#include "Poco/StreamCopier.h"
#include "Poco/URI.h"
#include "Poco/String.h"
#include "Poco/CountingStream.h"
#include "Poco/UTF8String.h"
#include <sstream>


using Poco::NullInputStream;
using Poco::StreamCopier;
using Poco::SyntaxException;
using Poco::URI;
using Poco::icompare;


namespace Poco {
namespace Net {


const std::string HTMLForm::ENCODING_URL           = "application/x-www-form-urlencoded";
const std::string HTMLForm::ENCODING_MULTIPART     = "multipart/form-data";
const int         HTMLForm::UNKNOWN_CONTENT_LENGTH = -1;


class HTMLFormCountingOutputStream: public CountingOutputStream
{
public:
	HTMLFormCountingOutputStream():
		_valid(true)
	{
	}

	bool isValid() const 
	{
		return _valid;
	}
	
	void setValid(bool v)
	{
		_valid = v;
	}

private:
	bool _valid;
};


HTMLForm::HTMLForm():
	_fieldLimit(DFL_FIELD_LIMIT),
	_encoding(ENCODING_URL)
{
}

	
HTMLForm::HTMLForm(const std::string& encoding):
	_fieldLimit(DFL_FIELD_LIMIT),
	_encoding(encoding)
{
}


HTMLForm::HTMLForm(const HTTPRequest& request, std::istream& requestBody, PartHandler& handler):
	_fieldLimit(DFL_FIELD_LIMIT)
{
	load(request, requestBody, handler);
}


HTMLForm::HTMLForm(const HTTPRequest& request, std::istream& requestBody):
	_fieldLimit(DFL_FIELD_LIMIT)
{
	load(request, requestBody);
}


HTMLForm::HTMLForm(const HTTPRequest& request):
	_fieldLimit(DFL_FIELD_LIMIT)
{
	load(request);
}

	
HTMLForm::~HTMLForm()
{
	for (PartVec::iterator it = _parts.begin(); it != _parts.end(); ++it)
	{
		delete it->pSource;
	}
}


void HTMLForm::setEncoding(const std::string& encoding)
{
	_encoding = encoding;
}


void HTMLForm::addPart(const std::string& name, PartSource* pSource)
{
	poco_check_ptr (pSource);

	Part part;
	part.name    = name;
	part.pSource = pSource;
	_parts.push_back(part);
}


void HTMLForm::load(const HTTPRequest& request, std::istream& requestBody, PartHandler& handler)
{
	clear();

	URI uri(request.getURI());
	const std::string& query = uri.getRawQuery();
	if (!query.empty())
	{
		std::istringstream istr(query);
		readUrl(istr);
	}

	if (request.getMethod() == HTTPRequest::HTTP_POST || request.getMethod() == HTTPRequest::HTTP_PUT)
	{
		std::string mediaType;
		NameValueCollection params;
		MessageHeader::splitParameters(request.getContentType(), mediaType, params); 
		_encoding = mediaType;
		if (_encoding == ENCODING_MULTIPART)
		{
			_boundary = params["boundary"];
			readMultipart(requestBody, handler);
		}
		else
		{
			readUrl(requestBody);
		}
	}
}


void HTMLForm::load(const HTTPRequest& request, std::istream& requestBody)
{
	NullPartHandler nah;
	load(request, requestBody, nah);
}


void HTMLForm::load(const HTTPRequest& request)
{
	NullPartHandler nah;
	NullInputStream nis;
	load(request, nis, nah);
}


void HTMLForm::read(std::istream& istr, PartHandler& handler)
{
	if (_encoding == ENCODING_URL)
		readUrl(istr);
	else
		readMultipart(istr, handler);
}


void HTMLForm::read(std::istream& istr)
{
	readUrl(istr);
}


void HTMLForm::read(const std::string& queryString)
{
	std::istringstream istr(queryString);
	readUrl(istr);
}


void HTMLForm::prepareSubmit(HTTPRequest& request)
{
	if (request.getMethod() == HTTPRequest::HTTP_POST || request.getMethod() == HTTPRequest::HTTP_PUT)
	{
		if (_encoding == ENCODING_URL)
		{
			request.setContentType(_encoding);
			request.setChunkedTransferEncoding(false);
			Poco::CountingOutputStream ostr;
			writeUrl(ostr);
			request.setContentLength(ostr.chars());
		}
		else
		{
			_boundary = MultipartWriter::createBoundary();
			std::string ct(_encoding);
			ct.append("; boundary=\"");
			ct.append(_boundary);
			ct.append("\"");
			request.setContentType(ct);
		}
		if (request.getVersion() == HTTPMessage::HTTP_1_0)
		{
			request.setKeepAlive(false);
			request.setChunkedTransferEncoding(false);
		}
		else if (_encoding != ENCODING_URL)
		{
			request.setChunkedTransferEncoding(true);
		}
	}
	else
	{
		std::string uri = request.getURI();
		std::ostringstream ostr;
		writeUrl(ostr);
		uri.append("?");
		uri.append(ostr.str());
		request.setURI(uri);
	}
}


std::streamsize HTMLForm::calculateContentLength()
{
	if (_boundary.empty())
		throw HTMLFormException("Form must be prepared");

	HTMLFormCountingOutputStream c;
	write(c);
	if (c.isValid())
		return c.chars();
	else
		return UNKNOWN_CONTENT_LENGTH;
}


void HTMLForm::write(std::ostream& ostr, const std::string& boundary)
{
	if (_encoding == ENCODING_URL)
	{
		writeUrl(ostr);
	}
	else
	{
		_boundary = boundary;
		writeMultipart(ostr);
	}
}


void HTMLForm::write(std::ostream& ostr)
{
	if (_encoding == ENCODING_URL)
		writeUrl(ostr);
	else
		writeMultipart(ostr);
}


void HTMLForm::readUrl(std::istream& istr)
{
	static const int eof = std::char_traits<char>::eof();

	int fields = 0;
	int ch = istr.get();
	bool isFirst = true;
	while (ch != eof)
	{
		if (_fieldLimit > 0 && fields == _fieldLimit)
			throw HTMLFormException("Too many form fields");
		std::string name;
		std::string value;
		while (ch != eof && ch != '=' && ch != '&')
		{
			if (ch == '+') ch = ' ';
			name += (char) ch;
			ch = istr.get();
		}
		if (ch == '=')
		{
			ch = istr.get();
			while (ch != eof && ch != '&')
			{
				if (ch == '+') ch = ' ';
				value += (char) ch;
				ch = istr.get();
			}
		}
		// remove UTF-8 byte order mark from first name, if present
		if (isFirst)
		{
			UTF8::removeBOM(name);
		}
		std::string decodedName;
		std::string decodedValue;
		URI::decode(name, decodedName);
		URI::decode(value, decodedValue);
		add(decodedName, decodedValue);
		++fields;
		if (ch == '&') ch = istr.get();
		isFirst = false;
	}
}


void HTMLForm::readMultipart(std::istream& istr, PartHandler& handler)
{
	static const int eof = std::char_traits<char>::eof();

	int fields = 0;
	MultipartReader reader(istr, _boundary);
	while (reader.hasNextPart())
	{
		if (_fieldLimit > 0 && fields == _fieldLimit)
			throw HTMLFormException("Too many form fields");
		MessageHeader header;
		reader.nextPart(header);
		std::string disp;
		NameValueCollection params;
		if (header.has("Content-Disposition"))
		{
			std::string cd = header.get("Content-Disposition");
			MessageHeader::splitParameters(cd, disp, params);
		}
		if (params.has("filename"))
		{
			handler.handlePart(header, reader.stream());
			// Ensure that the complete part has been read.
			while (reader.stream().good()) reader.stream().get();
		}
		else
		{
			std::string name = params["name"];
			std::string value;
			std::istream& istr = reader.stream();
			int ch = istr.get();
			while (ch != eof)
			{
				value += (char) ch;
				ch = istr.get();
			}
			add(name, value);
		}
		++fields;
	}
}


void HTMLForm::writeUrl(std::ostream& ostr)
{
	for (NameValueCollection::ConstIterator it = begin(); it != end(); ++it)
	{
		if (it != begin()) ostr << "&";
		std::string name;
		URI::encode(it->first, "!?#/'\",;:$&()[]*+=@", name);
		std::string value;
		URI::encode(it->second, "!?#/'\",;:$&()[]*+=@", value);
		ostr << name << "=" << value;
	}
}


void HTMLForm::writeMultipart(std::ostream& ostr)
{
	HTMLFormCountingOutputStream *costr(dynamic_cast<HTMLFormCountingOutputStream*>(&ostr));

	MultipartWriter writer(ostr, _boundary);
	for (NameValueCollection::ConstIterator it = begin(); it != end(); ++it)
	{
		MessageHeader header;
		std::string disp("form-data; name=\"");
		disp.append(it->first);
		disp.append("\"");
		header.set("Content-Disposition", disp);
		writer.nextPart(header);
		ostr << it->second;
	}	
	for (PartVec::iterator ita = _parts.begin(); ita != _parts.end(); ++ita)
	{
		MessageHeader header(ita->pSource->headers());
		std::string disp("form-data; name=\"");
		disp.append(ita->name);
		disp.append("\"");
		std::string filename = ita->pSource->filename();
		if (!filename.empty())
		{
			disp.append("; filename=\"");
			disp.append(filename);
			disp.append("\"");
		}
		header.set("Content-Disposition", disp);
		header.set("Content-Type", ita->pSource->mediaType());
		writer.nextPart(header);
		if (costr)
		{
			// count only, don't move stream position
			std::streamsize partlen = ita->pSource->getContentLength();
			if (partlen != PartSource::UNKNOWN_CONTENT_LENGTH)
				costr->addChars(static_cast<int>(partlen));
			else
				costr->setValid(false);
		}
		else
			StreamCopier::copyStream(ita->pSource->stream(), ostr);
	}
	writer.close();
	_boundary = writer.boundary();
}


void HTMLForm::setFieldLimit(int limit)
{
	poco_assert (limit >= 0);
	
	_fieldLimit = limit;
}


} } // namespace Poco::Net
