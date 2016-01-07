//
// Add.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/Add.h#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  Add
//
// Definition of the Add class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_Add_INCLUDED
#define Zip_Add_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipOperation.h"
#include "Poco/Zip/ZipCommon.h"


namespace Poco {
namespace Zip {


class Zip_API Add: public ZipOperation
	/// Operation Add adds a new file entry to an existing Zip File
{
public:
	Add(const std::string& zipPath, const std::string& localPath, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl);
		/// Creates the Add.

	void execute(Compress& c, std::istream& input);
		/// Performs the add operation

private:
	const std::string _zipPath;
	const std::string _localPath;
	const ZipCommon::CompressionMethod _cm;
	const ZipCommon::CompressionLevel _cl;
};


} } // namespace Poco::Zip


#endif // Zip_Add_INCLUDED
