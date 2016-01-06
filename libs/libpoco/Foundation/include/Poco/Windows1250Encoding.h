//
// Windows1250Encoding.h
//
// $Id: //poco/svn/Foundation/include/Poco/Windows1250Encoding.h#2 $
//
// Library: Foundation
// Package: Text
// Module:  Windows1250Encoding
//
// Definition of the Windows1250Encoding class.
//
// Copyright (c) 2005-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Windows1250Encoding_INCLUDED
#define Foundation_Windows1250Encoding_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/TextEncoding.h"


namespace Poco {


class Foundation_API Windows1250Encoding: public TextEncoding
	/// Windows Codepage 1250 text encoding.
	/// Based on: http://msdn.microsoft.com/en-us/goglobal/cc305143
{
public:
	Windows1250Encoding();
	~Windows1250Encoding();
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


#endif // Foundation_Windows1250Encoding_INCLUDED
