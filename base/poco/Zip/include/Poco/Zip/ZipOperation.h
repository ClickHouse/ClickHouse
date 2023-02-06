//
// ZipOperation.h
//
// Library: Zip
// Package: Manipulation
// Module:  ZipOperation
//
// Definition of the ZipOperation class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipOperation_INCLUDED
#define Zip_ZipOperation_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/RefCountedObject.h"
#include "Poco/AutoPtr.h"
#include <ostream>
#include <istream>


namespace Poco {
namespace Zip {


class Compress;


class Zip_API ZipOperation: public Poco::RefCountedObject
	/// Abstract super class for operations on individual zip entries
{
public:
	typedef Poco::AutoPtr<ZipOperation> Ptr;

	ZipOperation();
		/// Creates the ZipOperation.

	virtual void execute(Compress& c, std::istream& input) = 0;
		/// Executes the operation

protected:
	virtual ~ZipOperation();
		/// Destroys the ZipOperation.
};


} } // namespace Poco::Zip


#endif // Zip_ZipOperation_INCLUDED
