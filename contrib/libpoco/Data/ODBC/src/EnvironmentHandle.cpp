//
// EnvironmentHandle.cpp
//
// $Id: //poco/Main/Data/ODBC/src/EnvironmentHandle.cpp#2 $
//
// Library: ODBC
// Package: ODBC
// Module:  EnvironmentHandle
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/EnvironmentHandle.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/ODBCException.h"


namespace Poco {
namespace Data {
namespace ODBC {


EnvironmentHandle::EnvironmentHandle(): _henv(SQL_NULL_HENV)
{
	if (Utility::isError(SQLAllocHandle(SQL_HANDLE_ENV, 
			SQL_NULL_HANDLE, 
			&_henv)) ||
		Utility::isError(SQLSetEnvAttr(_henv, 
			SQL_ATTR_ODBC_VERSION, 
			(SQLPOINTER) SQL_OV_ODBC3, 
			0)))
	{
		throw ODBCException("Could not initialize environment.");
	}
}


EnvironmentHandle::~EnvironmentHandle()
{
	try
	{
		SQLRETURN rc = SQLFreeHandle(SQL_HANDLE_ENV, _henv);
		poco_assert (!Utility::isError(rc));
	}
	catch (...)
	{
		poco_unexpected();
	}
}


} } } // namespace Poco::Data::ODBC
