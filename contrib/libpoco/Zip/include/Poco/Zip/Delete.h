//
// Delete.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/Delete.h#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  Delete
//
// Definition of the Delete class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_Delete_INCLUDED
#define Zip_Delete_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipOperation.h"
#include "Poco/Zip/ZipLocalFileHeader.h"


namespace Poco {
namespace Zip {


class Zip_API Delete: public ZipOperation
	/// Delete Operation removes an entry from a Zip
{
public:
	Delete(const ZipLocalFileHeader& hdr);
		/// Creates the Delete.

	void execute(Compress& c, std::istream& input);
		/// Throws away the ZipEntry

private:
	const ZipLocalFileHeader _hdr;
};


} } // namespace Poco::Zip


#endif // Zip_Delete_INCLUDED
