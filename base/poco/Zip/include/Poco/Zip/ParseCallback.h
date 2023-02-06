//
// ParseCallback.h
//
// Library: Zip
// Package: Zip
// Module:  ParseCallback
//
// Definition of the ParseCallback class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ParseCallback_INCLUDED
#define Zip_ParseCallback_INCLUDED


#include "Poco/Zip/Zip.h"
#include <istream>


namespace Poco {
namespace Zip {


class ZipLocalFileHeader;


class Zip_API ParseCallback
	/// Interface for callbacks to handle ZipData
{
public:
	ParseCallback();
		/// Creates the ParseCallback.

	virtual ~ParseCallback();
		/// Destroys the ParseCallback.

	virtual bool handleZipEntry(std::istream& zipStream, const ZipLocalFileHeader& hdr) = 0;
		/// Handles parsing of the data of a single Zip Entry. zipStream is guaranteed to be at the very first data byte.
		/// Note that a callback class SHOULD consume all data inside a zip file, ie. after
		/// processing the next 4 bytes point the next ZipLocalFileHeader or the ZipDirectory.
		/// If it fails to do so, it must return false, otherwise true.

};


} } // namespace Poco::Zip


#endif // Zip_ParseCallback_INCLUDED
