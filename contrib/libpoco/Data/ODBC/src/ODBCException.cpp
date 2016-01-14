//
// ODBCException.cpp
//
// $Id: //poco/Main/Data/ODBC/src/ODBCException.cpp#2 $
//
// Library: ODBC
// Package: ODBC
// Module:  ODBCException
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/ODBCException.h"
#include <typeinfo>


namespace Poco {
namespace Data {
namespace ODBC {


POCO_IMPLEMENT_EXCEPTION(ODBCException, Poco::Data::DataException, "Generic ODBC error")
POCO_IMPLEMENT_EXCEPTION(InsufficientStorageException, ODBCException, "Insufficient storage error")
POCO_IMPLEMENT_EXCEPTION(UnknownDataLengthException, ODBCException, "Unknown length of remaining data")
POCO_IMPLEMENT_EXCEPTION(DataTruncatedException, ODBCException, "Variable length character or binary data truncated")


} } } // namespace Poco::Data::ODBC
