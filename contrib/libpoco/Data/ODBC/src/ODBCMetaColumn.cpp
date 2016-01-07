//
// ODBCMetaColumn.cpp
//
// $Id: //poco/Main/Data/ODBC/src/ODBCMetaColumn.cpp#5 $
//
// Library: ODBC
// Package: ODBC
// Module:  ODBCMetaColumn
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/ODBCMetaColumn.h"
#include "Poco/Data/ODBC/Utility.h"


namespace Poco {
namespace Data {
namespace ODBC {


ODBCMetaColumn::ODBCMetaColumn(const StatementHandle& rStmt, std::size_t position) : 
	MetaColumn(position),
	_rStmt(rStmt)
{
	init();
}

	
ODBCMetaColumn::~ODBCMetaColumn()
{
}


void ODBCMetaColumn::getDescription()
{
	std::memset(_columnDesc.name, 0, NAME_BUFFER_LENGTH);
	_columnDesc.nameBufferLength = 0;
	_columnDesc.dataType = 0;
	_columnDesc.size = 0;
	_columnDesc.decimalDigits = 0;
	_columnDesc.isNullable = 0;

	if (Utility::isError(Poco::Data::ODBC::SQLDescribeCol(_rStmt, 
		(SQLUSMALLINT) position() + 1, // ODBC columns are 1-based
		_columnDesc.name, 
		NAME_BUFFER_LENGTH,
		&_columnDesc.nameBufferLength, 
		&_columnDesc.dataType,
		&_columnDesc.size, 
		&_columnDesc.decimalDigits, 
		&_columnDesc.isNullable)))
	{
		throw StatementException(_rStmt);
	}
}


void ODBCMetaColumn::init()
{
	getDescription();

	if (Utility::isError(Poco::Data::ODBC::SQLColAttribute(_rStmt,
			(SQLUSMALLINT) position() + 1, // ODBC columns are 1-based
			SQL_DESC_LENGTH,
			0,
			0,
			0,
			&_dataLength)))
	{
		throw StatementException(_rStmt);
	}

	setName(std::string((char*) _columnDesc.name));
	setLength(_columnDesc.size);
	setPrecision(_columnDesc.decimalDigits);
	setNullable(SQL_NULLABLE == _columnDesc.isNullable);
	switch(_columnDesc.dataType)
	{
	case SQL_BIT:
		setType(MetaColumn::FDT_BOOL); break;

	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
		setType(MetaColumn::FDT_STRING); break;

	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		setType(MetaColumn::FDT_WSTRING); break;
	
	case SQL_TINYINT:
		setType(MetaColumn::FDT_INT8); break;
	
	case SQL_SMALLINT:
		setType(MetaColumn::FDT_INT16); break;
	
	case SQL_INTEGER:
		setType(MetaColumn::FDT_INT32); break;
	
	case SQL_BIGINT:
		setType(MetaColumn::FDT_INT64); break;
	
	case SQL_DOUBLE:
	case SQL_FLOAT:
		setType(MetaColumn::FDT_DOUBLE); break;
	
	case SQL_NUMERIC:
	case SQL_DECIMAL:
		if (0 == _columnDesc.decimalDigits)
			setType(MetaColumn::FDT_INT32);
		else
			setType(MetaColumn::FDT_DOUBLE);
		
		break;
	
	case SQL_REAL:
		setType(MetaColumn::FDT_FLOAT); break;
	
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
	case -98:// IBM DB2 non-standard type
		setType(MetaColumn::FDT_BLOB); break;
	
	case SQL_TYPE_DATE:
		setType(MetaColumn::FDT_DATE); break;
	
	case SQL_TYPE_TIME:
		setType(MetaColumn::FDT_TIME); break;
	
	case SQL_TYPE_TIMESTAMP:
		setType(MetaColumn::FDT_TIMESTAMP); break;
	
	default:
		throw DataFormatException("Unsupported data type.");
	}
}


} } } // namespace Poco::Data::ODBC
