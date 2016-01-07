//
// Keep.cpp
//
// $Id: //poco/1.4/Zip/src/Keep.cpp#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  Keep
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/Keep.h"
#include "Poco/Zip/Compress.h"
#include "Poco/Buffer.h"
#include "Poco/StreamCopier.h"


namespace Poco {
namespace Zip {


Keep::Keep(const ZipLocalFileHeader& hdr):
	_hdr(hdr)
{
}


void Keep::execute(Compress& c, std::istream& input)
{
	c.addFileRaw(input, _hdr, _hdr.getFileName());
}


} } // namespace Poco::Zip
