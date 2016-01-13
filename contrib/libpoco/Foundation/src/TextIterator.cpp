//
// TextIterator.cpp
//
// $Id: //poco/1.4/Foundation/src/TextIterator.cpp#1 $
//
// Library: Foundation
// Package: Text
// Module:  TextIterator
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/TextIterator.h"
#include "Poco/TextEncoding.h"
#include <algorithm>


namespace Poco {


TextIterator::TextIterator():
	_pEncoding(0)
{
}


TextIterator::TextIterator(const std::string& str, const TextEncoding& encoding):
	_pEncoding(&encoding),
	_it(str.begin()),
	_end(str.end())
{
}


TextIterator::TextIterator(const std::string::const_iterator& begin, const std::string::const_iterator& end, const TextEncoding& encoding):
	_pEncoding(&encoding),
	_it(begin),
	_end(end)
{
}


TextIterator::TextIterator(const std::string& str):
	_pEncoding(0),
	_it(str.end()),
	_end(str.end())
{
}


TextIterator::TextIterator(const std::string::const_iterator& end):
	_pEncoding(0),
	_it(end),
	_end(end)
{
}


TextIterator::~TextIterator()
{
}


TextIterator::TextIterator(const TextIterator& it):
	_pEncoding(it._pEncoding),
	_it(it._it),
	_end(it._end)
{
}


TextIterator& TextIterator::operator = (const TextIterator& it)
{
	if (&it != this)
	{
		_pEncoding = it._pEncoding;
		_it        = it._it;
		_end       = it._end;
	}
	return *this;
}


void TextIterator::swap(TextIterator& it)
{
	std::swap(_pEncoding, it._pEncoding);
	std::swap(_it, it._it);
	std::swap(_end, it._end);
}


int TextIterator::operator * () const
{
	poco_check_ptr (_pEncoding);
	poco_assert (_it != _end);
	std::string::const_iterator it = _it;
	
	unsigned char buffer[TextEncoding::MAX_SEQUENCE_LENGTH];
	unsigned char* p = buffer;

	if (it != _end)
		*p++ = *it++;
	else
		*p++ = 0;

	int read = 1;
	int n = _pEncoding->queryConvert(buffer, 1);

	while (-1 > n && (_end - it) >= -n - read)
	{
		while (read < -n && it != _end)
		{ 
			*p++ = *it++; 
			read++; 
		}
		n = _pEncoding->queryConvert(buffer, read);
	}

	if (-1 > n)
	{
		return -1;
	}
	else
	{
		return n;
	}
}

	
TextIterator& TextIterator::operator ++ ()
{
	poco_check_ptr (_pEncoding);
	poco_assert (_it != _end);
	
	unsigned char buffer[TextEncoding::MAX_SEQUENCE_LENGTH];
	unsigned char* p = buffer;

	if (_it != _end)
		*p++ = *_it++;
	else
		*p++ = 0;

	int read = 1;
	int n = _pEncoding->sequenceLength(buffer, 1);

	while (-1 > n && (_end - _it) >= -n - read)
	{
		while (read < -n && _it != _end)
		{ 
			*p++ = *_it++; 
			read++; 
		}
		n = _pEncoding->sequenceLength(buffer, read);
	}
	while (read < n && _it != _end)
	{ 
		_it++; 
		read++; 
	}

	return *this;
}


TextIterator TextIterator::operator ++ (int)
{
	TextIterator prev(*this);
	operator ++ ();
	return prev;
}


} // namespace Poco
