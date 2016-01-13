//
// MessageHeader.cpp
//
// $Id: //poco/1.4/Net/src/MessageHeader.cpp#4 $
//
// Library: Net
// Package: Messages
// Module:  MessageHeader
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MessageHeader.h"
#include "Poco/Net/NetException.h"
#include "Poco/String.h"
#include "Poco/Ascii.h"


namespace Poco {
namespace Net {


MessageHeader::MessageHeader():
	_fieldLimit(DFL_FIELD_LIMIT)
{
}


MessageHeader::MessageHeader(const MessageHeader& messageHeader):
	NameValueCollection(messageHeader),
	_fieldLimit(DFL_FIELD_LIMIT)
{
}


MessageHeader::~MessageHeader()
{
}


MessageHeader& MessageHeader::operator = (const MessageHeader& messageHeader)
{
	NameValueCollection::operator = (messageHeader);
	return *this;
}


void MessageHeader::write(std::ostream& ostr) const
{
	NameValueCollection::ConstIterator it = begin();
	while (it != end())
	{
		ostr << it->first << ": " << it->second << "\r\n";
		++it;
	}
}


void MessageHeader::read(std::istream& istr)
{
	static const int eof = std::char_traits<char>::eof();
	std::streambuf& buf = *istr.rdbuf();

	std::string name;
	std::string value;
	name.reserve(32);
	value.reserve(64);
	int ch = buf.sbumpc();
	int fields = 0;
	while (ch != eof && ch != '\r' && ch != '\n')
	{
		if (_fieldLimit > 0 && fields == _fieldLimit)
			throw MessageException("Too many header fields");
		name.clear();
		value.clear();
		while (ch != eof && ch != ':' && ch != '\n' && name.length() < MAX_NAME_LENGTH) { name += ch; ch = buf.sbumpc(); }
		if (ch == '\n') { ch = buf.sbumpc(); continue; } // ignore invalid header lines
		if (ch != ':') throw MessageException("Field name too long/no colon found");
		if (ch != eof) ch = buf.sbumpc(); // ':'
		while (ch != eof && Poco::Ascii::isSpace(ch) && ch != '\r' && ch != '\n') ch = buf.sbumpc();
		while (ch != eof && ch != '\r' && ch != '\n' && value.length() < MAX_VALUE_LENGTH) { value += ch; ch = buf.sbumpc(); }
		if (ch == '\r') ch = buf.sbumpc();
		if (ch == '\n')
			ch = buf.sbumpc();
		else if (ch != eof)
			throw MessageException("Field value too long/no CRLF found");
		while (ch == ' ' || ch == '\t') // folding
		{
			while (ch != eof && ch != '\r' && ch != '\n' && value.length() < MAX_VALUE_LENGTH) { value += ch; ch = buf.sbumpc(); }
			if (ch == '\r') ch = buf.sbumpc();
			if (ch == '\n')
				ch = buf.sbumpc();
			else if (ch != eof)
				throw MessageException("Folded field value too long/no CRLF found");
		}
		Poco::trimRightInPlace(value);
		add(name, value);
		++fields;
	}
	istr.putback(ch);
}


int MessageHeader::getFieldLimit() const
{
	return _fieldLimit;
}

	
void MessageHeader::setFieldLimit(int limit)
{
	poco_assert (limit >= 0);
	
	_fieldLimit = limit;
}


bool MessageHeader::hasToken(const std::string& fieldName, const std::string& token) const
{
	std::string field = get(fieldName, "");
	std::vector<std::string> tokens;
	splitElements(field, tokens, true);
	for (std::vector<std::string>::const_iterator it = tokens.begin(); it != tokens.end(); ++it)
	{
		if (Poco::icompare(*it, token) == 0)
			return true;
	}
	return false;
}


void MessageHeader::splitElements(const std::string& s, std::vector<std::string>& elements, bool ignoreEmpty)
{
	elements.clear();
	std::string::const_iterator it  = s.begin();
	std::string::const_iterator end = s.end();
	std::string elem;
	elem.reserve(64);
	while (it != end)
	{
		if (*it == '"')
		{
			elem += *it++;
			while (it != end && *it != '"')
			{
				if (*it == '\\')
				{
					++it;
					if (it != end) elem += *it++;
				}
				else elem += *it++;
			}
			if (it != end) elem += *it++;
		}
		else if (*it == '\\')
		{
			++it;
			if (it != end) elem += *it++;
		}
		else if (*it == ',')
		{
			Poco::trimInPlace(elem);
			if (!ignoreEmpty || !elem.empty())
				elements.push_back(elem);
			elem.clear();
			++it;
		}
		else elem += *it++;
	}
	if (!elem.empty())
	{
		Poco::trimInPlace(elem);
		if (!ignoreEmpty || !elem.empty())
			elements.push_back(elem);
	}
}


void MessageHeader::splitParameters(const std::string& s, std::string& value, NameValueCollection& parameters)
{
	value.clear();
	parameters.clear();
	std::string::const_iterator it  = s.begin();
	std::string::const_iterator end = s.end();
	while (it != end && Poco::Ascii::isSpace(*it)) ++it;
	while (it != end && *it != ';') value += *it++;
	Poco::trimRightInPlace(value);
	if (it != end) ++it;
	splitParameters(it, end, parameters);
}


void MessageHeader::splitParameters(const std::string::const_iterator& begin, const std::string::const_iterator& end, NameValueCollection& parameters)
{
	std::string pname;
	std::string pvalue;
	pname.reserve(32);
	pvalue.reserve(64);
	std::string::const_iterator it = begin;
	while (it != end)
	{
		pname.clear();
		pvalue.clear();
		while (it != end && Poco::Ascii::isSpace(*it)) ++it;
		while (it != end && *it != '=' && *it != ';') pname += *it++;
		Poco::trimRightInPlace(pname);
		if (it != end && *it != ';') ++it;
		while (it != end && Poco::Ascii::isSpace(*it)) ++it;
		while (it != end && *it != ';')
		{
			if (*it == '"')
			{
				++it;
				while (it != end && *it != '"')
				{
					if (*it == '\\')
					{
						++it;
						if (it != end) pvalue += *it++;
					}
					else pvalue += *it++;
				}
				if (it != end) ++it;
			}
			else if (*it == '\\')
			{
				++it;
				if (it != end) pvalue += *it++;
			}
			else pvalue += *it++;
		}
		Poco::trimRightInPlace(pvalue);
		if (!pname.empty()) parameters.add(pname, pvalue);
		if (it != end) ++it;
	}
}


void MessageHeader::quote(const std::string& value, std::string& result, bool allowSpace)
{
	bool mustQuote = false;
	for (std::string::const_iterator it = value.begin(); !mustQuote && it != value.end(); ++it)
	{
		if (!Poco::Ascii::isAlphaNumeric(*it) && *it != '.' && *it != '_' && *it != '-' && !(Poco::Ascii::isSpace(*it) && allowSpace))
			mustQuote = true;
	}
	if (mustQuote) result += '"';
	result.append(value);
	if (mustQuote) result += '"';
}


} } // namespace Poco::Net
