//
// Parameter.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/Parameter.h#3 $
//
// Library: ODBC
// Package: ODBC
// Module:  Parameter
//
// Definition of Parameter.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Parameter_INCLUDED
#define Data_ODBC_Parameter_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "Poco/Data/ODBC/Handle.h"
#ifdef POCO_OS_FAMILY_WINDOWS
#include <windows.h>
#endif
#include <sqlext.h>


namespace Poco {
namespace Data {
namespace ODBC {


class ODBC_API Parameter
{
public:
	explicit Parameter(const StatementHandle& rStmt, std::size_t colNum);
		/// Creates the Parameter.
		
	~Parameter();
		/// Destroys the Parameter.

	std::size_t number() const;
		/// Returns the column number.

	std::size_t dataType() const;
		/// Returns the SQL data type.

	std::size_t columnSize() const;
		/// Returns the the size of the column or expression of the corresponding 
		/// parameter marker as defined by the data source.

	std::size_t decimalDigits() const;
		/// Returns the number of decimal digits of the column or expression 
		/// of the corresponding parameter as defined by the data source.

	bool isNullable() const;
		/// Returns true if column allows null values, false otherwise.

private:
	Parameter();

	void init();

	SQLSMALLINT _dataType;
    SQLULEN     _columnSize;
    SQLSMALLINT _decimalDigits;
    SQLSMALLINT _isNullable;

	const StatementHandle& _rStmt;
	std::size_t _number;
};


///
/// inlines
///
inline std::size_t Parameter::number() const
{
	return _number;
}


inline std::size_t Parameter::dataType() const
{
	return _dataType;
}


inline std::size_t Parameter::columnSize() const
{
	return _columnSize;
}


inline std::size_t Parameter::decimalDigits() const
{
	return _decimalDigits;
}


inline bool Parameter::isNullable() const
{
	return SQL_NULLABLE == _isNullable;
}


} } } // namespace Poco::Data::ODBC


#endif
