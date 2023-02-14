//
// Checksum.h
//
// Library: Foundation
// Package: Core
// Module:  Checksum
//
// Definition of the Checksum class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Checksum_INCLUDED
#define Foundation_Checksum_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API Checksum
	/// This class calculates CRC-32 or Adler-32 checksums
	/// for arbitrary data.
	///
	/// A cyclic redundancy check (CRC) is a type of hash function, which is used to produce a 
	/// small, fixed-size checksum of a larger block of data, such as a packet of network 
	/// traffic or a computer file. CRC-32 is one of the most commonly used CRC algorithms.
	///
	/// Adler-32 is a checksum algorithm which was invented by Mark Adler. 
	/// It is almost as reliable as a 32-bit cyclic redundancy check for protecting against 
	/// accidental modification of data, such as distortions occurring during a transmission, 
	/// but is significantly faster to calculate in software.
	
{
public:
	enum Type
	{
		TYPE_ADLER32 = 0,
		TYPE_CRC32
	};

	Checksum();
		/// Creates a CRC-32 checksum initialized to 0.

	Checksum(Type t);
		/// Creates the Checksum, using the given type.

	~Checksum();
		/// Destroys the Checksum.

	void update(const char* data, unsigned length);
		/// Updates the checksum with the given data.

	void update(const std::string& data);
		/// Updates the checksum with the given data.

	void update(char data);
		/// Updates the checksum with the given data.

	Poco::UInt32 checksum() const;
		/// Returns the calculated checksum.

	Type type() const;
		/// Which type of checksum are we calulcating

private:
	Type         _type;
	Poco::UInt32 _value;
};


//
// inlines
//
inline void Checksum::update(const std::string& data)
{
	update(data.c_str(), static_cast<unsigned int>(data.size()));
}


inline void Checksum::update(char c)
{
	update(&c, 1);
}


inline Poco::UInt32 Checksum::checksum() const
{
	return _value;
}


inline Checksum::Type Checksum::type() const
{
	return _type;
}


} // namespace Poco


#endif // Foundation_Checksum_INCLUDED
