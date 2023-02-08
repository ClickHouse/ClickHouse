//
// ASCIIEncoding.h
//
// Library: Foundation
// Package: Text
// Module:  ASCIIEncoding
//
// Definition of the ASCIIEncoding class.
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ASCIIEncoding_INCLUDED
#define Foundation_ASCIIEncoding_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/TextEncoding.h"


namespace Poco {


class Foundation_API ASCIIEncoding: public TextEncoding
	/// 7-bit ASCII text encoding.
{
public:
	ASCIIEncoding();
	~ASCIIEncoding();
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


#endif // Foundation_ASCIIEncoding_INCLUDED
