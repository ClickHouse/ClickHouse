//
// Rename.cpp
//
// $Id: //poco/1.4/Zip/src/Rename.cpp#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  Rename
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/Rename.h"
#include "Poco/Zip/Compress.h"


namespace Poco {
namespace Zip {


Rename::Rename(const ZipLocalFileHeader& hdr, const std::string& newZipEntryName):
	_hdr(hdr),
	_newZipEntryName(newZipEntryName)
{
}


void Rename::execute(Compress& c, std::istream& input)
{
	c.addFileRaw(input, _hdr, _newZipEntryName);
}


} } // namespace Poco::Zip
