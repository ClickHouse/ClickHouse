//
// SkipCallback.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/SkipCallback.h#1 $
//
// Library: Zip
// Package: Zip
// Module:  SkipCallback
//
// Definition of the SkipCallback class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_SkipCallback_INCLUDED
#define Zip_SkipCallback_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ParseCallback.h"


namespace Poco {
namespace Zip {


class Zip_API SkipCallback: public ParseCallback
	/// A SkipCallback simply skips over the data
{
public:
	SkipCallback();
		/// Creates the SkipCallback.

	virtual ~SkipCallback();
		/// Destroys the SkipCallback.

	bool handleZipEntry(std::istream& zipStream, const ZipLocalFileHeader& hdr);
};


} } // namespace Poco::Zip


#endif // Zip_SkipCallback_INCLUDED
