//
// SkipCallback.cpp
//
// $Id: //poco/1.4/Zip/src/SkipCallback.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  SkipCallback
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/SkipCallback.h"
#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Zip/ZipUtil.h"


namespace Poco {
namespace Zip {


SkipCallback::SkipCallback()
{
}


SkipCallback::~SkipCallback()
{
}


bool SkipCallback::handleZipEntry(std::istream& zipStream, const ZipLocalFileHeader& hdr)
{
	if (!hdr.searchCRCAndSizesAfterData())
		zipStream.seekg(hdr.getCompressedSize(), std::ios_base::cur);
	else
		ZipUtil::sync(zipStream);
	return true;
}


} } // namespace Poco::Zip
