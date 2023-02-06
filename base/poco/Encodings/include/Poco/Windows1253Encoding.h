//
// Windows1253Encoding.h
//
// Library: Encodings
// Package: Encodings
// Module:  Windows1253Encoding
//
// Definition of the Windows1252Encoding class.
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: BSL-1.0
//


#ifndef Encodings_Windows1253Encoding_INCLUDED
#define Encodings_Windows1253Encoding_INCLUDED


#include "Poco/DoubleByteEncoding.h"


namespace Poco {


class Encodings_API Windows1253Encoding: public DoubleByteEncoding
	/// windows-1253 Encoding.
	///
	/// This text encoding class has been generated from
	/// http://www.unicode.org/Public/MAPPINGS/VENDORS/MICSFT/WINDOWS/CP1253.TXT.
{
public:
	Windows1253Encoding();
	~Windows1253Encoding();
	
private:
	static const char* _names[];
	static const CharacterMap _charMap;
	static const Mapping _mappingTable[];
	static const Mapping _reverseMappingTable[];
};


} // namespace Poco


#endif // Encodings_Windows1253Encoding_INCLUDED
