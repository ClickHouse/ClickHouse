//
// Windows874Encoding.h
//
// Library: Encodings
// Package: Encodings
// Module:  Windows874Encoding
//
// Definition of the Windows1252Encoding class.
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: BSL-1.0
//


#ifndef Encodings_Windows874Encoding_INCLUDED
#define Encodings_Windows874Encoding_INCLUDED


#include "Poco/DoubleByteEncoding.h"


namespace Poco {


class Encodings_API Windows874Encoding: public DoubleByteEncoding
	/// windows-874 Encoding.
	///
	/// This text encoding class has been generated from
	/// http://www.unicode.org/Public/MAPPINGS/VENDORS/MICSFT/WINDOWS/CP874.TXT.
{
public:
	Windows874Encoding();
	~Windows874Encoding();
	
private:
	static const char* _names[];
	static const CharacterMap _charMap;
	static const Mapping _mappingTable[];
	static const Mapping _reverseMappingTable[];
};


} // namespace Poco


#endif // Encodings_Windows874Encoding_INCLUDED
