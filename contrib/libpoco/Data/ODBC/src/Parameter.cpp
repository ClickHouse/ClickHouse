//
// Parameter.cpp
//
// $Id: //poco/Main/Data/ODBC/src/Parameter.cpp#5 $
//
// Library: ODBC
// Package: ODBC
// Module:  Parameter
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/Parameter.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/Error.h"
#include "Poco/Data/ODBC/ODBCException.h"


namespace Poco {
namespace Data {
namespace ODBC {


Parameter::Parameter(const StatementHandle& rStmt, std::size_t colNum) : 
	_rStmt(rStmt), 
	_number(colNum)
{
	init();
}

	
Parameter::~Parameter()
{
}


void Parameter::init()
{
	if (Utility::isError(SQLDescribeParam(_rStmt, 
		(SQLUSMALLINT) _number + 1, 
		&_dataType,
		&_columnSize,
		&_decimalDigits,
		&_isNullable)))
	{
		throw StatementException(_rStmt);
	}
}


} } } // namespace Poco::Data::ODBC
