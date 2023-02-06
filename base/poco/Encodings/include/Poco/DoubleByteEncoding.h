//
// DoubleByteEncoding.h
//
// Library: Encodings
// Package: Encodings
// Module:  DoubleByteEncoding
//
// Definition of the DoubleByteEncoding class.
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Encodings_DoubleByteEncoding_INCLUDED
#define Encodings_DoubleByteEncoding_INCLUDED


#include "Poco/Encodings.h"
#include "Poco/TextEncoding.h"


namespace Poco {


class Encodings_API DoubleByteEncoding: public TextEncoding
	/// This abstract class is a base class for various double-byte character
	/// set (DBCS) encodings.
	///
	/// Double-byte encodings are variants of multi-byte encodings
	/// where (Unicode) each code point is represented by one or
	/// two bytes. Unicode code points are restricted to the
	/// Basic Multilingual Plane.
	///
	/// Subclasses must provide encoding names, a static CharacterMap, as well
	/// as static Mapping and reverse Mapping tables, and provide these to the
	/// DoubleByteEncoding constructor.
{
public:
	struct Mapping
	{
		Poco::UInt16 from;
		Poco::UInt16 to;
	};

	// TextEncoding
	const char* canonicalName() const;
	bool isA(const std::string& encodingName) const;
	const CharacterMap& characterMap() const;
	int convert(const unsigned char* bytes) const;
	int convert(int ch, unsigned char* bytes, int length) const;
	int queryConvert(const unsigned char* bytes, int length) const;
	int sequenceLength(const unsigned char* bytes, int length) const;

protected:
	DoubleByteEncoding(const char** names, const TextEncoding::CharacterMap& charMap, const Mapping mappingTable[], std::size_t mappingTableSize, const Mapping reverseMappingTable[], std::size_t reverseMappingTableSize);
		/// Creates a DoubleByteEncoding using the given mapping and reverse-mapping tables.
		///
		/// names must be a static array declared in the derived class,
		/// containing the names of this encoding, declared as:
		///
		///     const char* MyEncoding::_names[] =
		///     {
		///         "myencoding",
		///         "MyEncoding",
		///         NULL
		///     };
		///
		/// The first entry in names must be the canonical name.
		///
		/// charMap must be a static CharacterMap giving information about double-byte
		/// character sequences.
		///
		/// For each mappingTable item, from must be a value in range 0x0100 to
		//  0xFFFF for double-byte mappings, which the most significant (upper) byte
		/// representing the first character in the sequence and the lower byte
		/// representing the second character in the sequence.
		///
		/// For each reverseMappingTable item, from must be Unicode code point from the
		/// Basic Multilingual Plane, and to is a one-byte or two-byte sequence.
		/// As with mappingTable, a one-byte sequence is in range 0x00 to 0xFF, and a
		/// two-byte sequence is in range 0x0100 to 0xFFFF.
		///
		/// Unicode code points are restricted to the Basic Multilingual Plane
		/// (code points 0x0000 to 0xFFFF).
		///
		/// Items in both tables must be sorted by from, in ascending order.

	~DoubleByteEncoding();
		/// Destroys the DoubleByteEncoding.

	int map(Poco::UInt16 encoded) const;
		/// Maps a double-byte encoded character to its Unicode code point.
		///
		/// Returns the Unicode code point, or -1 if the encoded character is bad
		/// and cannot be mapped.

	int reverseMap(int cp) const;
		/// Maps a Unicode code point to its double-byte representation.
		///
		/// Returns -1 if the code point cannot be mapped, otherwise
		/// a value in range 0 to 0xFF for single-byte mappings, or
		/// 0x0100 to 0xFFFF for double-byte mappings.

private:
	DoubleByteEncoding();

	const char** _names;
	const TextEncoding::CharacterMap& _charMap;
	const Mapping* _mappingTable;
	const std::size_t _mappingTableSize;
	const Mapping* _reverseMappingTable;
	const std::size_t _reverseMappingTableSize;
};


} // namespace Poco


#endif // Encodings_DoubleByteEncoding_INCLUDED
