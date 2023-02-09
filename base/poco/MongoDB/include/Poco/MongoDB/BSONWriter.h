//
// BSONWriter.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  BSONWriter
//
// Definition of the BSONWriter class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_BSONWriter_INCLUDED
#define MongoDB_BSONWriter_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/BinaryWriter.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API BSONWriter
	/// Class for writing BSON using a Poco::BinaryWriter.
{
public:
	BSONWriter(const Poco::BinaryWriter& writer): 
		_writer(writer)
		/// Creates the BSONWriter.
	{
	}

	virtual ~BSONWriter()
		/// Destroys the BSONWriter.
	{
	}

	template<typename T>
	void write(T& t)
		/// Writes the value to the writer. The default implementation uses
		/// the << operator. Special types can write their own version.
	{
		_writer << t;
	}

	void writeCString(const std::string& value);
		/// Writes a cstring to the writer. A cstring is a string
		/// terminated a null character.

private:
	Poco::BinaryWriter _writer;
};


//
// inlines
//
inline void BSONWriter::writeCString(const std::string& value)
{
	_writer.writeRaw(value);
	_writer << (unsigned char) 0x00;
}


} } // namespace Poco::MongoDB


#endif // MongoDB_BSONWriter_INCLUDED
