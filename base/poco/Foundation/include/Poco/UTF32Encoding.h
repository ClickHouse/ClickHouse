//
// UTF32Encoding.h
//
// Library: Foundation
// Package: Text
// Module:  UTF32Encoding
//
// Definition of the UTF32Encoding class.
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UTF32Encoding_INCLUDED
#define Foundation_UTF32Encoding_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/TextEncoding.h"


namespace Poco
{


class Foundation_API UTF32Encoding : public TextEncoding
/// UTF-32 text encoding, as defined in RFC 2781.
///
/// When converting from UTF-32 to Unicode, surrogates are
/// reported as they are - in other words, surrogate pairs
/// are not combined into one Unicode character.
{
public:
    enum ByteOrderType
    {
        BIG_ENDIAN_BYTE_ORDER,
        LITTLE_ENDIAN_BYTE_ORDER,
        NATIVE_BYTE_ORDER
    };

    UTF32Encoding(ByteOrderType byteOrder = NATIVE_BYTE_ORDER);
    /// Creates and initializes the encoding for the given byte order.

    UTF32Encoding(int byteOrderMark);
    /// Creates and initializes the encoding for the byte-order
    /// indicated by the given byte-order mark, which is the Unicode
    /// character 0xFEFF.

    ~UTF32Encoding();

    ByteOrderType getByteOrder() const;
    /// Returns the byte-order currently in use.

    void setByteOrder(ByteOrderType byteOrder);
    /// Sets the byte order.

    void setByteOrder(int byteOrderMark);
    /// Sets the byte order according to the given
    /// byte order mark, which is the Unicode
    /// character 0xFEFF.

    const char * canonicalName() const;
    bool isA(const std::string & encodingName) const;
    const CharacterMap & characterMap() const;
    int convert(const unsigned char * bytes) const;
    int convert(int ch, unsigned char * bytes, int length) const;
    int queryConvert(const unsigned char * bytes, int length) const;
    int sequenceLength(const unsigned char * bytes, int length) const;

protected:
	static int safeToInt(Poco::UInt32 value)
	{
		if (value <= 0x10FFFF)
			return static_cast<int>(value);
		else
			return -1;
	}

private:
    bool _flipBytes;
    static const char * _names[];
    static const CharacterMap _charMap;
};


} // namespace Poco


#endif // Foundation_UTF32Encoding_INCLUDED
