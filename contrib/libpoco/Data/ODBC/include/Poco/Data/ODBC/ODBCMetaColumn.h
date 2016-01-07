//
// ODBCMetaColumn.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/ODBCMetaColumn.h#3 $
//
// Library: ODBC
// Package: ODBC
// Module:  ODBCMetaColumn
//
// Definition of ODBCMetaColumn.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_ODBCColumn_INCLUDED
#define Data_ODBC_ODBCColumn_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "Poco/Data/ODBC/Error.h"
#include "Poco/Data/ODBC/Handle.h"
#include "Poco/Data/ODBC/ODBCException.h"
#include "Poco/Data/MetaColumn.h"
#ifdef POCO_OS_FAMILY_WINDOWS
#include <windows.h>
#endif
#include <sqlext.h>


namespace Poco {
namespace Data {
namespace ODBC {


class ODBC_API ODBCMetaColumn: public MetaColumn
{
public:
	explicit ODBCMetaColumn(const StatementHandle& rStmt, std::size_t position);
		/// Creates the ODBCMetaColumn.
		
	~ODBCMetaColumn();
		/// Destroys the ODBCMetaColumn.

	std::size_t dataLength() const;
		/// A numeric value that is either the maximum or actual character length of a character 
		/// string or binary data type. It is the maximum character length for a fixed-length data type, 
		/// or the actual character length for a variable-length data type. Its value always excludes the 
		/// null-termination byte that ends the character string. 
		/// This information is returned from the SQL_DESC_LENGTH record field of the IRD.

private:
	ODBCMetaColumn();

	static const int NAME_BUFFER_LENGTH = 2048;

	struct ColumnDescription
	{
		SQLCHAR name[NAME_BUFFER_LENGTH];
		SQLSMALLINT  nameBufferLength;
		SQLSMALLINT  dataType;
		SQLULEN      size;
		SQLSMALLINT  decimalDigits;
		SQLSMALLINT  isNullable;
	};

	void init();
	void getDescription();

	SQLLEN                 _dataLength;
	const StatementHandle& _rStmt;
	ColumnDescription      _columnDesc;
};


///
/// inlines
///
inline std::size_t ODBCMetaColumn::dataLength() const
{
	return _dataLength;
}


} } } // namespace Poco::Data::ODBC


#endif
