//
// TypeInfo.cpp
//
// $Id: //poco/Main/Data/ODBC/src/TypeInfo.cpp#1 $
//
// Library: ODBC
// Package: ODBC
// Module:  TypeInfo
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/TypeInfo.h"
#include "Poco/Data/ODBC/ODBCException.h"
#include "Poco/Format.h"
#include "Poco/Exception.h"
#include <iostream>

namespace Poco {
namespace Data {
namespace ODBC {


TypeInfo::TypeInfo(SQLHDBC* pHDBC): _pHDBC(pHDBC)
{
	fillCTypes();
	fillSQLTypes();
	if (_pHDBC) fillTypeInfo(*_pHDBC);
}


TypeInfo::~TypeInfo()
{
}


void TypeInfo::fillCTypes()
{
	_cDataTypes.insert(ValueType(SQL_CHAR, SQL_C_CHAR));
	_cDataTypes.insert(ValueType(SQL_VARCHAR, SQL_C_CHAR));
	_cDataTypes.insert(ValueType(SQL_LONGVARCHAR, SQL_C_CHAR));
	_cDataTypes.insert(ValueType(SQL_DECIMAL, SQL_C_DOUBLE));
	_cDataTypes.insert(ValueType(SQL_NUMERIC, SQL_C_DOUBLE));
	_cDataTypes.insert(ValueType(SQL_BIT, SQL_C_BIT));
	_cDataTypes.insert(ValueType(SQL_TINYINT, SQL_C_STINYINT));
	_cDataTypes.insert(ValueType(SQL_SMALLINT, SQL_C_SSHORT));
	_cDataTypes.insert(ValueType(SQL_INTEGER, SQL_C_SLONG));
	_cDataTypes.insert(ValueType(SQL_BIGINT, SQL_C_SBIGINT));
	_cDataTypes.insert(ValueType(SQL_REAL, SQL_C_FLOAT));
	_cDataTypes.insert(ValueType(SQL_FLOAT, SQL_C_DOUBLE));
	_cDataTypes.insert(ValueType(SQL_DOUBLE, SQL_C_DOUBLE));
	_cDataTypes.insert(ValueType(SQL_BINARY, SQL_C_BINARY));
	_cDataTypes.insert(ValueType(SQL_VARBINARY, SQL_C_BINARY));
	_cDataTypes.insert(ValueType(SQL_LONGVARBINARY, SQL_C_BINARY));
	_cDataTypes.insert(ValueType(SQL_TYPE_DATE, SQL_C_TYPE_DATE));
	_cDataTypes.insert(ValueType(SQL_TYPE_TIME, SQL_C_TYPE_TIME));
	_cDataTypes.insert(ValueType(SQL_TYPE_TIMESTAMP, SQL_C_TYPE_TIMESTAMP));
}


void TypeInfo::fillSQLTypes()
{
	_sqlDataTypes.insert(ValueType(SQL_C_CHAR, SQL_LONGVARCHAR));
	_sqlDataTypes.insert(ValueType(SQL_C_BIT, SQL_BIT));
	_sqlDataTypes.insert(ValueType(SQL_C_TINYINT, SQL_TINYINT));
	_sqlDataTypes.insert(ValueType(SQL_C_STINYINT, SQL_TINYINT));
	_sqlDataTypes.insert(ValueType(SQL_C_UTINYINT, SQL_TINYINT));
	_sqlDataTypes.insert(ValueType(SQL_C_SHORT, SQL_SMALLINT));
	_sqlDataTypes.insert(ValueType(SQL_C_SSHORT, SQL_SMALLINT));
	_sqlDataTypes.insert(ValueType(SQL_C_USHORT, SQL_SMALLINT));
	_sqlDataTypes.insert(ValueType(SQL_C_LONG, SQL_INTEGER));
	_sqlDataTypes.insert(ValueType(SQL_C_SLONG, SQL_INTEGER));
	_sqlDataTypes.insert(ValueType(SQL_C_ULONG, SQL_INTEGER));
	_sqlDataTypes.insert(ValueType(SQL_C_SBIGINT, SQL_BIGINT));
	_sqlDataTypes.insert(ValueType(SQL_C_UBIGINT, SQL_BIGINT));
	_sqlDataTypes.insert(ValueType(SQL_C_FLOAT, SQL_REAL));
	_sqlDataTypes.insert(ValueType(SQL_C_DOUBLE, SQL_DOUBLE));
	_sqlDataTypes.insert(ValueType(SQL_C_BINARY, SQL_LONGVARBINARY));
	_sqlDataTypes.insert(ValueType(SQL_C_TYPE_DATE, SQL_TYPE_DATE));
	_sqlDataTypes.insert(ValueType(SQL_C_TYPE_TIME, SQL_TYPE_TIME));
	_sqlDataTypes.insert(ValueType(SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP));
}


void TypeInfo::fillTypeInfo(SQLHDBC pHDBC)
{
	_pHDBC = &pHDBC;

	if (_typeInfo.empty() && _pHDBC)
	{
		const static int stringSize = 512;
		TypeInfoVec().swap(_typeInfo);

		SQLRETURN rc;
		SQLHSTMT hstmt = SQL_NULL_HSTMT;

		rc = SQLAllocHandle(SQL_HANDLE_STMT, *_pHDBC, &hstmt);
		if (!SQL_SUCCEEDED(rc))
			throw StatementException(hstmt, "SQLGetData()");

		rc = SQLGetTypeInfo(hstmt, SQL_ALL_TYPES);
		if (SQL_SUCCEEDED(rc))
		{
			while (SQLFetch(hstmt) != SQL_NO_DATA_FOUND)
			{
				char typeName[stringSize] = { 0 };
				char literalPrefix[stringSize] = { 0 };
				char literalSuffix[stringSize] = { 0 };
				char createParams[stringSize] = { 0 };
				char localTypeName[stringSize] = { 0 };
				
				TypeInfoTup ti("TYPE_NAME", "",
					"DATA_TYPE", 0,
					"COLUMN_SIZE", 0,
					"LITERAL_PREFIX", "",
					"LITERAL_SUFFIX", "",
					"CREATE_PARAMS", "",
					"NULLABLE", 0,
					"CASE_SENSITIVE", 0,
					"SEARCHABLE", 0,
					"UNSIGNED_ATTRIBUTE", 0,
					"FIXED_PREC_SCALE", 0,
					"AUTO_UNIQUE_VALUE", 0,
					"LOCAL_TYPE_NAME", "",
					"MINIMUM_SCALE", 0,
					"MAXIMUM_SCALE", 0,
					"SQL_DATA_TYPE", 0,
					"SQL_DATETIME_SUB", 0,
					"NUM_PREC_RADIX", 0,
					"INTERVAL_PRECISION", 0);

				SQLLEN ind = 0;
				rc = SQLGetData(hstmt, 1, SQL_C_CHAR, typeName, sizeof(typeName), &ind);
				ti.set<0>(typeName);
				rc = SQLGetData(hstmt, 2, SQL_C_SSHORT, &ti.get<1>(), sizeof(SQLSMALLINT), &ind);
				rc = SQLGetData(hstmt, 3, SQL_C_SLONG, &ti.get<2>(), sizeof(SQLINTEGER), &ind);
				rc = SQLGetData(hstmt, 4, SQL_C_CHAR, literalPrefix, sizeof(literalPrefix), &ind);
				ti.set<3>(literalPrefix);
				rc = SQLGetData(hstmt, 5, SQL_C_CHAR, literalSuffix, sizeof(literalSuffix), &ind);
				ti.set<4>(literalSuffix);
				rc = SQLGetData(hstmt, 6, SQL_C_CHAR, createParams, sizeof(createParams), &ind);
				ti.set<5>(createParams);
				rc = SQLGetData(hstmt, 7, SQL_C_SSHORT, &ti.get<6>(), sizeof(SQLSMALLINT), &ind); 
				rc = SQLGetData(hstmt, 8, SQL_C_SSHORT, &ti.get<7>(), sizeof(SQLSMALLINT), &ind); 
				rc = SQLGetData(hstmt, 9, SQL_C_SSHORT, &ti.get<8>(), sizeof(SQLSMALLINT), &ind); 
				rc = SQLGetData(hstmt, 10, SQL_C_SSHORT, &ti.get<9>(), sizeof(SQLSMALLINT), &ind); 
				rc = SQLGetData(hstmt, 11, SQL_C_SSHORT, &ti.get<10>(), sizeof(SQLSMALLINT), &ind);
				rc = SQLGetData(hstmt, 12, SQL_C_SSHORT, &ti.get<11>(), sizeof(SQLSMALLINT), &ind);
				rc = SQLGetData(hstmt, 13, SQL_C_CHAR, localTypeName, sizeof(localTypeName), &ind);
				ti.set<12>(localTypeName);
				rc = SQLGetData(hstmt, 14, SQL_C_SSHORT, &ti.get<13>(), sizeof(SQLSMALLINT), &ind);
				rc = SQLGetData(hstmt, 15, SQL_C_SSHORT, &ti.get<14>(), sizeof(SQLSMALLINT), &ind);
				rc = SQLGetData(hstmt, 16, SQL_C_SSHORT, &ti.get<15>(), sizeof(SQLSMALLINT), &ind);
				rc = SQLGetData(hstmt, 17, SQL_C_SSHORT, &ti.get<16>(), sizeof(SQLSMALLINT), &ind);
				rc = SQLGetData(hstmt, 18, SQL_C_SLONG, &ti.get<17>(), sizeof(SQLINTEGER), &ind);
				rc = SQLGetData(hstmt, 19, SQL_C_SSHORT, &ti.get<18>(), sizeof(SQLSMALLINT), &ind);

				_typeInfo.push_back(ti);
			}
		}

		SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
	}
}


DynamicAny TypeInfo::getInfo(SQLSMALLINT type, const std::string& param) const
{
	TypeInfoVec::const_iterator it = _typeInfo.begin();
	TypeInfoVec::const_iterator end = _typeInfo.end();
	for (; it != end; ++it)
	{
		if (type == it->get<1>())
			return (*it)[param];
	}

	throw NotFoundException(param);
}


bool TypeInfo::tryGetInfo(SQLSMALLINT type, const std::string& param, DynamicAny& result) const
{
	TypeInfoVec::const_iterator it = _typeInfo.begin();
	TypeInfoVec::const_iterator end = _typeInfo.end();
	for (; it != end; ++it)
	{
		if (type == it->get<1>())
		{
			result = (*it)[param];
			return true;
		}
	}
	
	return false;
}


int TypeInfo::cDataType(int sqlDataType) const
{
	DataTypeMap::const_iterator it = _cDataTypes.find(sqlDataType);

	if (_cDataTypes.end() == it)
		throw NotFoundException(format("C data type not found for SQL data type: %d", sqlDataType));

	return it->second;
}


int TypeInfo::sqlDataType(int cDataType) const
{
	DataTypeMap::const_iterator it = _sqlDataTypes.find(cDataType);

	if (_sqlDataTypes.end() == it)
		throw NotFoundException(format("SQL data type not found for C data type: %d", cDataType));

	return it->second;
}


void TypeInfo::print(std::ostream& ostr)
{
	if (_typeInfo.empty())
	{
		ostr << "No data found.";
		return;
	}

	TypeInfoTup::NameVec::const_iterator nIt = (*_typeInfo[0].names()).begin();
	TypeInfoTup::NameVec::const_iterator nItEnd = (*_typeInfo[0].names()).end();
	for (; nIt != nItEnd; ++nIt)
		ostr << *nIt << "\t";

	ostr << std::endl;

	TypeInfoVec::const_iterator it = _typeInfo.begin();
	TypeInfoVec::const_iterator end = _typeInfo.end();

	for (; it != end; ++it)
	{
		ostr << it->get<0>() << "\t" 
			<< it->get<1>() << "\t" 
			<< it->get<2>() << "\t" 
			<< it->get<3>() << "\t" 
			<< it->get<4>() << "\t" 
			<< it->get<5>() << "\t" 
			<< it->get<6>() << "\t" 
			<< it->get<7>() << "\t" 
			<< it->get<8>() << "\t" 
			<< it->get<9>() << "\t" 
			<< it->get<10>() << "\t" 
			<< it->get<11>() << "\t" 
			<< it->get<12>() << "\t" 
			<< it->get<13>() << "\t" 
			<< it->get<14>() << "\t"
			<< it->get<15>() << "\t" 
			<< it->get<16>() << "\t" 
			<< it->get<17>() << "\t" 
			<< it->get<18>() << std::endl;
	}
}


} } } // namespace Poco::Data::ODBC
