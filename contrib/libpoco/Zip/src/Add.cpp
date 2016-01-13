//
// Add.cpp
//
// $Id: //poco/1.4/Zip/src/Add.cpp#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  Add
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/Add.h"
#include "Poco/Zip/Compress.h"


namespace Poco {
namespace Zip {


Add::Add(const std::string& zipPath, const std::string& localPath, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl):
	_zipPath(zipPath),
	_localPath(localPath),
	_cm(cm),
	_cl(cl)
{
}


void Add::execute(Compress& c, std::istream& input)
{
	c.addFile(Poco::Path(_localPath), Poco::Path(_zipPath), _cm, _cl);
}


} } // namespace Poco::Zip
