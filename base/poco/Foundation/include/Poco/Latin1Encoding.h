//
// Latin1Encoding.h
//
// Library: Foundation
// Package: Text
// Module:  Latin1Encoding
//
// Definition of the Latin1Encoding class.
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Latin1Encoding_INCLUDED
#define Foundation_Latin1Encoding_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/TextEncoding.h"


namespace Poco {


class Foundation_API Latin1Encoding: public TextEncoding
	/// ISO Latin-1 (8859-1) text encoding.
{
public:
	Latin1Encoding();
	~Latin1Encoding();
	const char* canonicalName() const;
	bool isA(const std::string& encodingName) const;
	const CharacterMap& characterMap() const;
	int convert(const unsigned char* bytes) const;
	int convert(int ch, unsigned char* bytes, int length) const;
	int queryConvert(const unsigned char* bytes, int length) const;
	int sequenceLength(const unsigned char* bytes, int length) const;
	
private:
	static const char* _names[];
	static const CharacterMap _charMap;
};


} // namespace Poco


#endif // Foundation_Latin1Encoding_INCLUDED
