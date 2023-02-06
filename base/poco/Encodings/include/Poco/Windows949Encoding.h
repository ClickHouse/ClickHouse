//
// Windows949Encoding.h
//
// Library: Encodings
// Package: Encodings
// Module:  Windows949Encoding
//
// Definition of the Windows1252Encoding class.
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: BSL-1.0
//


#ifndef Encodings_Windows949Encoding_INCLUDED
#define Encodings_Windows949Encoding_INCLUDED


#include "Poco/DoubleByteEncoding.h"


namespace Poco {


class Encodings_API Windows949Encoding: public DoubleByteEncoding
	/// windows-949 Encoding.
	///
	/// This text encoding class has been generated from
	/// http://www.unicode.org/Public/MAPPINGS/VENDORS/MICSFT/WINDOWS/CP949.TXT.
{
public:
	Windows949Encoding();
	~Windows949Encoding();
	
private:
	static const char* _names[];
	static const CharacterMap _charMap;
	static const Mapping _mappingTable[];
	static const Mapping _reverseMappingTable[];
};


} // namespace Poco


#endif // Encodings_Windows949Encoding_INCLUDED
