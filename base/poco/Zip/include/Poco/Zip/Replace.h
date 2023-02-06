//
// Replace.h
//
// Library: Zip
// Package: Manipulation
// Module:  Replace
//
// Definition of the Replace class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_Replace_INCLUDED
#define Zip_Replace_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/Add.h"
#include "Poco/Zip/Delete.h"
#include "Poco/Zip/ZipOperation.h"


namespace Poco {
namespace Zip {


class Zip_API Replace: public ZipOperation
	/// Operation Replace replaces the content of an existing entry with a new one
{
public:
	Replace(const ZipLocalFileHeader& hdr, const std::string& localPath);
		/// Creates the Replace.

	void execute(Compress& c, std::istream& input);
		/// Performs the replace operation

private:
	Delete _del;
	Add _add;
};


} } // namespace Poco::Zip


#endif // Zip_Replace_INCLUDED
