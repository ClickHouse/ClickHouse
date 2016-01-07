//
// TextEncoding.h
//
// $Id: //poco/1.4/Foundation/include/Poco/TextEncoding.h#1 $
//
// Library: Foundation
// Package: Text
// Module:  TextEncoding
//
// Definition of the abstract TextEncoding class.
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_TextEncoding_INCLUDED
#define Foundation_TextEncoding_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/SharedPtr.h"


namespace Poco {


class TextEncodingManager;


class Foundation_API TextEncoding
	/// An abstract base class for implementing text encodings
	/// like UTF-8 or ISO 8859-1. 
	///
	/// Subclasses must override the canonicalName(), isA(),
	/// characterMap() and convert() methods and need to be
	/// thread safe and stateless.
	///
	/// TextEncoding also provides static member functions
	/// for managing mappings from encoding names to
	/// TextEncoding objects.
{
public:
	typedef SharedPtr<TextEncoding> Ptr;
	
	enum
	{
		MAX_SEQUENCE_LENGTH = 6 /// The maximum character byte sequence length supported.
	};
	
	typedef int CharacterMap[256];
		/// The map[b] member gives information about byte sequences
		/// whose first byte is b.
		/// If map[b] is c where c is >= 0, then b by itself encodes the Unicode scalar value c.
		/// If map[b] is -1, then the byte sequence is malformed.
		/// If map[b] is -n, where n >= 2, then b is the first byte of an n-byte
		/// sequence that encodes a single Unicode scalar value. Byte sequences up
		/// to 6 bytes in length are supported.

	virtual ~TextEncoding();
		/// Destroys the encoding.

	virtual const char* canonicalName() const = 0;
		/// Returns the canonical name of this encoding,
		/// e.g. "ISO-8859-1". Encoding name comparisons are case
		/// insensitive.

	virtual bool isA(const std::string& encodingName) const = 0;
		/// Returns true if the given name is one of the names of this encoding.
		/// For example, the "ISO-8859-1" encoding is also known as "Latin-1".
		///
		/// Encoding name comparision are be case insensitive.
			
	virtual const CharacterMap& characterMap() const = 0;
		/// Returns the CharacterMap for the encoding.
		/// The CharacterMap should be kept in a static member. As
		/// characterMap() can be called frequently, it should be
		/// implemented in such a way that it just returns a static
		/// map. If the map is built at runtime, this should be
		/// done in the constructor.
		
	virtual int convert(const unsigned char* bytes) const;
		/// The convert function is used to convert multibyte sequences;
		/// bytes will point to a byte sequence of n bytes where 
		/// sequenceLength(bytes, length) == -n, with length >= n.
		///
		/// The convert function must return the Unicode scalar value
		/// represented by this byte sequence or -1 if the byte sequence is malformed.
		/// The default implementation returns (int) bytes[0].

	virtual	int queryConvert(const unsigned char* bytes, int length) const;
		/// The queryConvert function is used to convert single byte characters 
		/// or multibyte sequences;
		/// bytes will point to a byte sequence of length bytes.
		///
		/// The queryConvert function must return the Unicode scalar value
		/// represented by this byte sequence or -1 if the byte sequence is malformed
		/// or -n where n is number of bytes requested for the sequence, if lenght is 
		/// shorter than the sequence.
		/// The length of the sequence might not be determined by the first byte, 
		/// in which case the conversion becomes an iterative process:
		/// First call with length == 1 might return -2,
		/// Then a second call with lenght == 2 might return -4
		/// Eventually, the third call with length == 4 should return either a 
		/// Unicode scalar value, or -1 if the byte sequence is malformed.
		/// The default implementation returns (int) bytes[0].

	virtual int sequenceLength(const unsigned char* bytes, int length) const;
		/// The sequenceLength function is used to get the lenth of the sequence pointed
		/// by bytes. The length paramater should be greater or equal to the length of 
		/// the sequence.
		///
		/// The sequenceLength function must return the lenght of the sequence
		/// represented by this byte sequence or a negative value -n if length is 
		/// shorter than the sequence, where n is the number of byte requested 
		/// to determine the length of the sequence.
		/// The length of the sequence might not be determined by the first byte, 
		/// in which case the conversion becomes an iterative process as long as the 
		/// result is negative:
		/// First call with length == 1 might return -2,
		/// Then a second call with lenght == 2 might return -4
		/// Eventually, the third call with length == 4 should return 4.
		/// The default implementation returns 1.

	virtual int convert(int ch, unsigned char* bytes, int length) const;
		/// Transform the Unicode character ch into the encoding's 
		/// byte sequence. The method returns the number of bytes
		/// used. The method must not use more than length characters.
		/// Bytes and length can also be null - in this case only the number
		/// of bytes required to represent ch is returned.
		/// If the character cannot be converted, 0 is returned and
		/// the byte sequence remains unchanged.
		/// The default implementation simply returns 0.

	static TextEncoding& byName(const std::string& encodingName);
		/// Returns the TextEncoding object for the given encoding name.
		///
		/// Throws a NotFoundException if the encoding with given name is not available.
		
	static TextEncoding::Ptr find(const std::string& encodingName);
		/// Returns a pointer to the TextEncoding object for the given encodingName,
		/// or NULL if no such TextEncoding object exists.

	static void add(TextEncoding::Ptr encoding);
		/// Adds the given TextEncoding to the table of text encodings,
		/// under the encoding's canonical name.
		///
		/// If an encoding with the given name is already registered,
		/// it is replaced.

	static void add(TextEncoding::Ptr encoding, const std::string& name);
		/// Adds the given TextEncoding to the table of text encodings,
		/// under the given name.
		///
		/// If an encoding with the given name is already registered,
		/// it is replaced.

	static void remove(const std::string& encodingName);
		/// Removes the encoding with the given name from the table
		/// of text encodings.

	static TextEncoding::Ptr global(TextEncoding::Ptr encoding);
		/// Sets global TextEncoding object.
		///
		/// This function sets the global encoding to the argument and returns a
		/// reference of the previous global encoding.

	static TextEncoding& global();
		/// Return the current global TextEncoding object

	static const std::string GLOBAL;
		/// Name of the global TextEncoding, which is the empty string.
		
protected:
	static TextEncodingManager& manager();
		/// Returns the TextEncodingManager.
};


} // namespace Poco


#endif // Foundation_TextEncoding_INCLUDED
