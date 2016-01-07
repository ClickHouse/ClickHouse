//
// Replace.cpp
//
// $Id: //poco/1.4/Zip/src/Replace.cpp#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  Replace
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/Replace.h"
#include "Poco/Zip/Compress.h"


namespace Poco {
namespace Zip {


Replace::Replace(const ZipLocalFileHeader& hdr, const std::string& localPath):
	_del(hdr),
	_add(hdr.getFileName(), localPath, hdr.getCompressionMethod(), hdr.getCompressionLevel())
{
}


void Replace::execute(Compress& c, std::istream& input)
{
	_del.execute(c, input);
	_add.execute(c, input);
}


} } // namespace Poco::Zip
