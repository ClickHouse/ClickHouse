//
// Unicode.cpp
//
// $Id: //poco/Main/Data/ODBC/src/Unicode.cpp#3 $
//
// Library: ODBC
// Package: ODBC
// Module:  Unicode
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/ODBC.h"


#if defined(POCO_ODBC_UNICODE_WINDOWS)
#include "Unicode_WIN32.cpp"
#elif defined(POCO_ODBC_UNICODE_UNIXODBC)
#include "Unicode_UNIXODBC.cpp"
#endif
