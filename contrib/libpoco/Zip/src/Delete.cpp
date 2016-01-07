//
// Delete.cpp
//
// $Id: //poco/1.4/Zip/src/Delete.cpp#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  Delete
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/Delete.h"


namespace Poco {
namespace Zip {


Delete::Delete(const ZipLocalFileHeader& hdr):
	_hdr(hdr)
{
}


void Delete::execute(Compress& c, std::istream& input)
{
	// due to absolute positioning in compress we don't need to do anything
}


} } // namespace Poco::Zip
