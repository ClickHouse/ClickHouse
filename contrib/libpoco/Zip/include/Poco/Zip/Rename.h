//
// Rename.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/Rename.h#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  Rename
//
// Definition of the Rename class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_Rename_INCLUDED
#define Zip_Rename_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipOperation.h"
#include "Poco/Zip/ZipLocalFileHeader.h"


namespace Poco {
namespace Zip {


class Zip_API Rename: public ZipOperation
	/// Renames an existing Zip Entry
{
public:
	Rename(const ZipLocalFileHeader& hdr, const std::string& newZipEntryName);
		/// Creates the Rename.

	void execute(Compress& c, std::istream& input);
		/// Performs the rename operation

private:
	const ZipLocalFileHeader _hdr;
	const std::string _newZipEntryName;
};


} } // namespace Poco::Zip


#endif // Zip_Rename_INCLUDED
