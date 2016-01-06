//
// Unicode.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/Unicode.h#4 $
//
// Library: ODBC
// Package: ODBC
// Module:  Unicode
//
// Definition of Unicode.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Unicode_INCLUDED
#define Data_ODBC_Unicode_INCLUDED


#include "Poco/UnicodeConverter.h"
#include "Poco/Buffer.h"
#include "Poco/Exception.h"
#include <cstring>
#ifdef POCO_OS_FAMILY_WINDOWS
	#include <windows.h>
#endif
#ifndef SQL_NOUNICODEMAP
	#define SQL_NOUNICODEMAP
#endif
#include <sqlext.h>
#include <sqltypes.h>
#include <sqlucode.h>


#if defined(POCO_OS_FAMILY_WINDOWS) && defined(POCO_WIN32_UTF8)
	#define POCO_ODBC_UNICODE
	#define POCO_ODBC_UNICODE_WINDOWS
#elif defined(POCO_OS_FAMILY_UNIX) && defined(UNICODE)
	#define POCO_ODBC_UNICODE
	#ifdef POCO_UNIXODBC
		#define POCO_ODBC_UNICODE_UNIXODBC
	#elif defined(POCO_IODBC)
		#error "iODBC Unicode not supported"
	#endif
#endif


namespace Poco {
namespace Data {
namespace ODBC {


#if defined(POCO_PTR_IS_64_BIT) || defined(POCO_OS_FAMILY_UNIX) // mkrivos - POCO_OS_FAMILY_UNIX doesn't compile with SQLPOINTER & SQLColAttribute()
	typedef SQLLEN* NumAttrPtrType;
#else
	typedef SQLPOINTER NumAttrPtrType;
#endif


SQLRETURN ODBC_API SQLColAttribute(SQLHSTMT hstmt,
	SQLUSMALLINT   iCol,
	SQLUSMALLINT   iField,
	SQLPOINTER	   pCharAttr,
	SQLSMALLINT	   cbCharAttrMax,
	SQLSMALLINT*   pcbCharAttr,
	NumAttrPtrType pNumAttr);


SQLRETURN ODBC_API SQLColAttributes(SQLHSTMT hstmt,
	SQLUSMALLINT icol,
	SQLUSMALLINT fDescType,
	SQLPOINTER   rgbDesc,
	SQLSMALLINT  cbDescMax,
	SQLSMALLINT* pcbDesc,
	SQLLEN*      pfDesc);

SQLRETURN ODBC_API SQLConnect(SQLHDBC hdbc,
	SQLCHAR*    szDSN,
	SQLSMALLINT cbDSN,
	SQLCHAR*    szUID,
	SQLSMALLINT cbUID,
	SQLCHAR*    szAuthStr,
	SQLSMALLINT cbAuthStr);

SQLRETURN ODBC_API SQLDescribeCol(SQLHSTMT hstmt,
	SQLUSMALLINT icol,
	SQLCHAR*     szColName,
	SQLSMALLINT  cbColNameMax,
	SQLSMALLINT* pcbColName,
	SQLSMALLINT* pfSqlType,
	SQLULEN*     pcbColDef,
	SQLSMALLINT* pibScale,
	SQLSMALLINT* pfNullable);

SQLRETURN ODBC_API SQLError(SQLHENV henv,
	SQLHDBC      hdbc,
	SQLHSTMT     hstmt,
	SQLCHAR*     szSqlState,
	SQLINTEGER*  pfNativeError,
	SQLCHAR*     szErrorMsg,
	SQLSMALLINT  cbErrorMsgMax,
	SQLSMALLINT* pcbErrorMsg);

SQLRETURN ODBC_API SQLExecDirect(SQLHSTMT hstmt,
	SQLCHAR*   szSqlStr,
	SQLINTEGER cbSqlStr);

SQLRETURN ODBC_API SQLGetConnectAttr(SQLHDBC hdbc,
	SQLINTEGER  fAttribute,
	SQLPOINTER  rgbValue,
	SQLINTEGER  cbValueMax,
	SQLINTEGER* pcbValue);

SQLRETURN ODBC_API SQLGetCursorName(SQLHSTMT hstmt,
	SQLCHAR*     szCursor,
	SQLSMALLINT  cbCursorMax,
	SQLSMALLINT* pcbCursor);

SQLRETURN ODBC_API SQLSetDescField(SQLHDESC DescriptorHandle,
	SQLSMALLINT RecNumber,
	SQLSMALLINT FieldIdentifier,
	SQLPOINTER  Value,
	SQLINTEGER  BufferLength);

SQLRETURN ODBC_API SQLGetDescField(SQLHDESC hdesc,
	SQLSMALLINT iRecord,
	SQLSMALLINT iField,
	SQLPOINTER  rgbValue,
	SQLINTEGER	cbValueMax,
	SQLINTEGER* pcbValue);

SQLRETURN ODBC_API SQLGetDescRec(SQLHDESC hdesc,
	SQLSMALLINT  iRecord,
	SQLCHAR*     szName,
	SQLSMALLINT  cbNameMax,
	SQLSMALLINT* pcbName,
	SQLSMALLINT* pfType,
	SQLSMALLINT* pfSubType,
	SQLLEN*      pLength,
	SQLSMALLINT* pPrecision,
	SQLSMALLINT* pScale,
	SQLSMALLINT* pNullable);

SQLRETURN ODBC_API SQLGetDiagField(SQLSMALLINT fHandleType,
	SQLHANDLE    handle,
	SQLSMALLINT  iRecord,
	SQLSMALLINT  fDiagField,
	SQLPOINTER   rgbDiagInfo,
	SQLSMALLINT  cbDiagInfoMax,
	SQLSMALLINT* pcbDiagInfo);

SQLRETURN ODBC_API SQLGetDiagRec(SQLSMALLINT fHandleType,
	SQLHANDLE    handle,
	SQLSMALLINT  iRecord,
	SQLCHAR*     szSqlState,
	SQLINTEGER*  pfNativeError,
	SQLCHAR*     szErrorMsg,
	SQLSMALLINT  cbErrorMsgMax,
	SQLSMALLINT* pcbErrorMsg);

SQLRETURN ODBC_API SQLPrepare(SQLHSTMT hstmt,
	SQLCHAR*   szSqlStr,
	SQLINTEGER cbSqlStr);

SQLRETURN ODBC_API SQLSetConnectAttr(SQLHDBC hdbc,
	SQLINTEGER fAttribute,
	SQLPOINTER rgbValue,
	SQLINTEGER cbValue);

SQLRETURN ODBC_API SQLSetCursorName(SQLHSTMT hstmt,
	SQLCHAR*    szCursor,
	SQLSMALLINT cbCursor);

SQLRETURN ODBC_API SQLSetStmtAttr(SQLHSTMT hstmt,
	SQLINTEGER fAttribute,
	SQLPOINTER rgbValue,
	SQLINTEGER cbValueMax);

SQLRETURN ODBC_API SQLGetStmtAttr(SQLHSTMT hstmt,
	SQLINTEGER  fAttribute,
	SQLPOINTER  rgbValue,
	SQLINTEGER  cbValueMax,
	SQLINTEGER* pcbValue);

SQLRETURN ODBC_API SQLColumns(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName,
	SQLCHAR*    szColumnName,
	SQLSMALLINT cbColumnName);

SQLRETURN ODBC_API SQLGetConnectOption(SQLHDBC hdbc,
	SQLUSMALLINT fOption,
	SQLPOINTER   pvParam);

SQLRETURN ODBC_API SQLGetInfo(SQLHDBC hdbc,
	SQLUSMALLINT fInfoType,
	SQLPOINTER   rgbInfoValue,
	SQLSMALLINT  cbInfoValueMax,
	SQLSMALLINT* pcbInfoValue);

SQLRETURN ODBC_API SQLGetTypeInfo(SQLHSTMT StatementHandle,
	SQLSMALLINT	DataType);

SQLRETURN ODBC_API SQLSetConnectOption(SQLHDBC hdbc,
	SQLUSMALLINT fOption,
	SQLULEN      vParam);

SQLRETURN ODBC_API SQLSpecialColumns(SQLHSTMT hstmt,
	SQLUSMALLINT fColType,
	SQLCHAR*     szCatalogName,
	SQLSMALLINT  cbCatalogName,
	SQLCHAR*     szSchemaName,
	SQLSMALLINT  cbSchemaName,
	SQLCHAR*     szTableName,
	SQLSMALLINT  cbTableName,
	SQLUSMALLINT fScope,
	SQLUSMALLINT fNullable);

SQLRETURN ODBC_API SQLStatistics(SQLHSTMT hstmt,
	SQLCHAR*     szCatalogName,
	SQLSMALLINT  cbCatalogName,
	SQLCHAR*     szSchemaName,
	SQLSMALLINT  cbSchemaName,
	SQLCHAR*     szTableName,
	SQLSMALLINT  cbTableName,
	SQLUSMALLINT fUnique,
	SQLUSMALLINT fAccuracy);

SQLRETURN ODBC_API SQLTables(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName,
	SQLCHAR*    szTableType,
	SQLSMALLINT cbTableType);

SQLRETURN ODBC_API SQLDataSources(SQLHENV henv,
	SQLUSMALLINT fDirection,
	SQLCHAR*     szDSN,
	SQLSMALLINT  cbDSNMax,
	SQLSMALLINT* pcbDSN,
	SQLCHAR*     szDescription,
	SQLSMALLINT  cbDescriptionMax,
	SQLSMALLINT* pcbDescription);

SQLRETURN ODBC_API SQLDriverConnect(SQLHDBC hdbc,
	SQLHWND      hwnd,
	SQLCHAR*     szConnStrIn,
	SQLSMALLINT  cbConnStrIn,
	SQLCHAR*     szConnStrOut,
	SQLSMALLINT  cbConnStrOutMax,
	SQLSMALLINT* pcbConnStrOut,
	SQLUSMALLINT fDriverCompletion);

SQLRETURN ODBC_API SQLBrowseConnect(SQLHDBC hdbc,
	SQLCHAR*     szConnStrIn,
	SQLSMALLINT  cbConnStrIn,
	SQLCHAR*     szConnStrOut,
	SQLSMALLINT  cbConnStrOutMax,
	SQLSMALLINT* pcbConnStrOut);

SQLRETURN ODBC_API SQLColumnPrivileges(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName,
	SQLCHAR*    szColumnName,
	SQLSMALLINT cbColumnName);

SQLRETURN ODBC_API SQLForeignKeys(SQLHSTMT hstmt,
	SQLCHAR*    szPkCatalogName,
	SQLSMALLINT cbPkCatalogName,
	SQLCHAR*    szPkSchemaName,
	SQLSMALLINT cbPkSchemaName,
	SQLCHAR*    szPkTableName,
	SQLSMALLINT cbPkTableName,
	SQLCHAR*    szFkCatalogName,
	SQLSMALLINT cbFkCatalogName,
	SQLCHAR*    szFkSchemaName,
	SQLSMALLINT cbFkSchemaName,
	SQLCHAR*    szFkTableName,
	SQLSMALLINT cbFkTableName);

SQLRETURN ODBC_API SQLNativeSql(SQLHDBC hdbc,
	SQLCHAR*    szSqlStrIn,
	SQLINTEGER  cbSqlStrIn,
	SQLCHAR*    szSqlStr,
	SQLINTEGER  cbSqlStrMax,
	SQLINTEGER* pcbSqlStr);

SQLRETURN ODBC_API SQLPrimaryKeys(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName);

SQLRETURN ODBC_API SQLProcedureColumns(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szProcName,
	SQLSMALLINT cbProcName,
	SQLCHAR*    szColumnName,
	SQLSMALLINT cbColumnName);

SQLRETURN ODBC_API SQLProcedures(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szProcName,
	SQLSMALLINT cbProcName);

SQLRETURN ODBC_API SQLTablePrivileges(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName);

SQLRETURN ODBC_API SQLDrivers(SQLHENV henv,
	SQLUSMALLINT fDirection,
	SQLCHAR*     szDriverDesc,
	SQLSMALLINT  cbDriverDescMax,
	SQLSMALLINT* pcbDriverDesc,
	SQLCHAR*     szDriverAttributes,
	SQLSMALLINT  cbDrvrAttrMax,
	SQLSMALLINT* pcbDrvrAttr);


///
/// inlines
///

inline bool isString(SQLPOINTER pValue, SQLINTEGER length)
{
	return (SQL_NTS == length || length > 0) && pValue;
}


inline SQLINTEGER stringLength(SQLPOINTER pValue, SQLINTEGER length)
{
	if (SQL_NTS != length) return length;

	return (SQLINTEGER) std::strlen((const char*) pValue);
}


#if !defined(POCO_ODBC_UNICODE)


///
/// inlines
///

inline SQLRETURN SQLColAttribute(SQLHSTMT hstmt,
	SQLUSMALLINT   iCol,
	SQLUSMALLINT   iField,
	SQLPOINTER     pCharAttr,
	SQLSMALLINT    cbCharAttrMax,
	SQLSMALLINT*   pcbCharAttr,
	NumAttrPtrType pNumAttr)
{
	return ::SQLColAttributeA(hstmt,
		iCol,
		iField,
		pCharAttr,
		cbCharAttrMax,
		pcbCharAttr,
		pNumAttr);
}


inline SQLRETURN SQLColAttributes(SQLHSTMT hstmt,
	SQLUSMALLINT icol,
	SQLUSMALLINT fDescType,
	SQLPOINTER   rgbDesc,
	SQLSMALLINT  cbDescMax,
	SQLSMALLINT* pcbDesc,
	SQLLEN*      pfDesc)
{
	return ::SQLColAttributesA(hstmt,
		icol,
		fDescType,
		rgbDesc,
		cbDescMax,
		pcbDesc,
		pfDesc);
}


inline SQLRETURN SQLConnect(SQLHDBC hdbc,
	SQLCHAR*    szDSN,
	SQLSMALLINT cbDSN,
	SQLCHAR*    szUID,
	SQLSMALLINT cbUID,
	SQLCHAR*    szAuthStr,
	SQLSMALLINT cbAuthStr)
{
	return ::SQLConnectA(hdbc,
		szDSN,
		cbDSN,
		szUID,
		cbUID,
		szAuthStr,
		cbAuthStr);
}


inline SQLRETURN SQLDescribeCol(SQLHSTMT hstmt,
	SQLUSMALLINT icol,
	SQLCHAR*     szColName,
	SQLSMALLINT  cbColNameMax,
	SQLSMALLINT* pcbColName,
	SQLSMALLINT* pfSqlType,
	SQLULEN*     pcbColDef,
	SQLSMALLINT* pibScale,
	SQLSMALLINT* pfNullable)
{
	return ::SQLDescribeColA(hstmt,
		icol,
		szColName,
		cbColNameMax,
		pcbColName,
		pfSqlType,
		pcbColDef,
		pibScale,
		pfNullable);
}


inline SQLRETURN SQLError(SQLHENV henv,
	SQLHDBC      hdbc,
	SQLHSTMT     hstmt,
	SQLCHAR*     szSqlState,
	SQLINTEGER*  pfNativeError,
	SQLCHAR*     szErrorMsg,
	SQLSMALLINT  cbErrorMsgMax,
	SQLSMALLINT* pcbErrorMsg)
{
	throw Poco::NotImplementedException("SQLError is obsolete. "
		"Use SQLGetDiagRec instead.");
}


inline SQLRETURN SQLExecDirect(SQLHSTMT hstmt,
	SQLCHAR*   szSqlStr,
	SQLINTEGER cbSqlStr)
{
	return ::SQLExecDirectA(hstmt, szSqlStr, cbSqlStr);
}


inline SQLRETURN SQLGetConnectAttr(SQLHDBC hdbc,
	SQLINTEGER  fAttribute,
	SQLPOINTER  rgbValue,
	SQLINTEGER  cbValueMax,
	SQLINTEGER* pcbValue)
{
	return ::SQLGetConnectAttrA(hdbc,
		fAttribute,
		rgbValue,
		cbValueMax,
		pcbValue);
}


inline SQLRETURN SQLGetCursorName(SQLHSTMT hstmt,
	SQLCHAR*     szCursor,
	SQLSMALLINT  cbCursorMax,
	SQLSMALLINT* pcbCursor)
{
	throw Poco::NotImplementedException("Not implemented");
}


inline SQLRETURN SQLSetDescField(SQLHDESC DescriptorHandle,
	SQLSMALLINT RecNumber,
	SQLSMALLINT FieldIdentifier,
	SQLPOINTER  Value,
	SQLINTEGER  BufferLength)
{
	return ::SQLSetDescField(DescriptorHandle,
		RecNumber,
		FieldIdentifier,
		Value,
		BufferLength);
}


inline SQLRETURN SQLGetDescField(SQLHDESC hdesc,
	SQLSMALLINT iRecord,
	SQLSMALLINT iField,
	SQLPOINTER  rgbValue,
	SQLINTEGER	cbValueMax,
	SQLINTEGER* pcbValue)
{
	return ::SQLGetDescFieldA(hdesc,
		iRecord,
		iField,
		rgbValue,
		cbValueMax,
		pcbValue);
}


inline SQLRETURN SQLGetDescRec(SQLHDESC hdesc,
	SQLSMALLINT  iRecord,
	SQLCHAR*     szName,
	SQLSMALLINT  cbNameMax,
	SQLSMALLINT* pcbName,
	SQLSMALLINT* pfType,
	SQLSMALLINT* pfSubType,
	SQLLEN*      pLength,
	SQLSMALLINT* pPrecision,
	SQLSMALLINT* pScale,
	SQLSMALLINT* pNullable)
{
	return ::SQLGetDescRecA(hdesc,
		iRecord,
		szName,
		cbNameMax,
		pcbName,
		pfType,
		pfSubType,
		pLength,
		pPrecision,
		pScale,
		pNullable);
}


inline SQLRETURN SQLGetDiagField(SQLSMALLINT fHandleType,
	SQLHANDLE    handle,
	SQLSMALLINT  iRecord,
	SQLSMALLINT  fDiagField,
	SQLPOINTER   rgbDiagInfo,
	SQLSMALLINT  cbDiagInfoMax,
	SQLSMALLINT* pcbDiagInfo)
{
	return ::SQLGetDiagFieldA(fHandleType,
		handle,
		iRecord,
		fDiagField,
		rgbDiagInfo,
		cbDiagInfoMax,
		pcbDiagInfo);
}


inline SQLRETURN SQLGetDiagRec(SQLSMALLINT fHandleType,
	SQLHANDLE    handle,
	SQLSMALLINT  iRecord,
	SQLCHAR*     szSqlState,
	SQLINTEGER*  pfNativeError,
	SQLCHAR*     szErrorMsg,
	SQLSMALLINT  cbErrorMsgMax,
	SQLSMALLINT* pcbErrorMsg)
{
	return ::SQLGetDiagRecA(fHandleType,
		handle,
		iRecord,
		szSqlState,
		pfNativeError,
		szErrorMsg,
		cbErrorMsgMax,
		pcbErrorMsg);
}


inline SQLRETURN SQLPrepare(SQLHSTMT hstmt,
	SQLCHAR*   szSqlStr,
	SQLINTEGER cbSqlStr)
{
	return ::SQLPrepareA(hstmt,	szSqlStr, cbSqlStr);
}


inline SQLRETURN SQLSetConnectAttr(SQLHDBC hdbc,
	SQLINTEGER fAttribute,
	SQLPOINTER rgbValue,
	SQLINTEGER cbValue)
{
	return ::SQLSetConnectAttrA(hdbc,	fAttribute,	rgbValue, cbValue);
}


inline SQLRETURN SQLSetCursorName(SQLHSTMT hstmt,
	SQLCHAR*    szCursor,
	SQLSMALLINT cbCursor)
{
	throw Poco::NotImplementedException("Not implemented");
}


inline SQLRETURN SQLSetStmtAttr(SQLHSTMT hstmt,
	SQLINTEGER fAttribute,
	SQLPOINTER rgbValue,
	SQLINTEGER cbValueMax)
{
	return ::SQLSetStmtAttr(hstmt, fAttribute, rgbValue, cbValueMax);
}


inline SQLRETURN SQLGetStmtAttr(SQLHSTMT hstmt,
	SQLINTEGER  fAttribute,
	SQLPOINTER  rgbValue,
	SQLINTEGER  cbValueMax,
	SQLINTEGER* pcbValue)
{
	return ::SQLGetStmtAttrA(hstmt,
		fAttribute,
		rgbValue,
		cbValueMax,
		pcbValue);
}


inline SQLRETURN SQLColumns(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName,
	SQLCHAR*    szColumnName,
	SQLSMALLINT cbColumnName)
{
	return ::SQLColumnsA(hstmt,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szTableName,
		cbTableName,
		szColumnName,
		cbColumnName);
}


inline SQLRETURN SQLGetConnectOption(SQLHDBC hdbc,
	SQLUSMALLINT fOption,
	SQLPOINTER   pvParam)
{
	return ::SQLGetConnectOptionA(hdbc, fOption, pvParam);
}


inline SQLRETURN SQLGetInfo(SQLHDBC hdbc,
	SQLUSMALLINT fInfoType,
	SQLPOINTER   rgbInfoValue,
	SQLSMALLINT  cbInfoValueMax,
	SQLSMALLINT* pcbInfoValue)
{
	return ::SQLGetInfoA(hdbc,
		fInfoType,
		rgbInfoValue,
		cbInfoValueMax,
		pcbInfoValue);
}


inline SQLRETURN SQLGetTypeInfo(SQLHSTMT StatementHandle,
	SQLSMALLINT	DataType)
{
	return ::SQLGetTypeInfoA(StatementHandle, DataType);
}


inline SQLRETURN SQLSetConnectOption(SQLHDBC hdbc,
	SQLUSMALLINT fOption,
	SQLULEN      vParam)
{
	return ::SQLSetConnectOptionA(hdbc, fOption, vParam);
}


inline SQLRETURN SQLSpecialColumns(SQLHSTMT hstmt,
	SQLUSMALLINT fColType,
	SQLCHAR*     szCatalogName,
	SQLSMALLINT  cbCatalogName,
	SQLCHAR*     szSchemaName,
	SQLSMALLINT  cbSchemaName,
	SQLCHAR*     szTableName,
	SQLSMALLINT  cbTableName,
	SQLUSMALLINT fScope,
	SQLUSMALLINT fNullable)
{
	return ::SQLSpecialColumnsA(hstmt,
		fColType,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szTableName,
		cbTableName,
		fScope,
		fNullable);
}


inline SQLRETURN SQLStatistics(SQLHSTMT hstmt,
	SQLCHAR*     szCatalogName,
	SQLSMALLINT  cbCatalogName,
	SQLCHAR*     szSchemaName,
	SQLSMALLINT  cbSchemaName,
	SQLCHAR*     szTableName,
	SQLSMALLINT  cbTableName,
	SQLUSMALLINT fUnique,
	SQLUSMALLINT fAccuracy)
{
	return ::SQLStatisticsA(hstmt,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szTableName,
		cbTableName,
		fUnique,
		fAccuracy);
}


inline SQLRETURN SQLTables(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName,
	SQLCHAR*    szTableType,
	SQLSMALLINT cbTableType)
{
	return ::SQLTablesA(hstmt,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szTableName,
		cbTableName,
		szTableType,
		cbTableType);
}


inline SQLRETURN SQLDataSources(SQLHENV henv,
	SQLUSMALLINT fDirection,
	SQLCHAR*     szDSN,
	SQLSMALLINT  cbDSNMax,
	SQLSMALLINT* pcbDSN,
	SQLCHAR*     szDescription,
	SQLSMALLINT  cbDescriptionMax,
	SQLSMALLINT* pcbDescription)
{
	return ::SQLDataSourcesA(henv,
		fDirection,
		szDSN,
		cbDSNMax,
		pcbDSN,
		szDescription,
		cbDescriptionMax,
		pcbDescription);
}


inline SQLRETURN SQLDriverConnect(SQLHDBC hdbc,
	SQLHWND      hwnd,
	SQLCHAR*     szConnStrIn,
	SQLSMALLINT  cbConnStrIn,
	SQLCHAR*     szConnStrOut,
	SQLSMALLINT  cbConnStrOutMax,
	SQLSMALLINT* pcbConnStrOut,
	SQLUSMALLINT fDriverCompletion)
{
	return ::SQLDriverConnectA(hdbc,
		hwnd,
		szConnStrIn,
		cbConnStrIn,
		szConnStrOut,
		cbConnStrOutMax,
		pcbConnStrOut,
		fDriverCompletion);
}


inline SQLRETURN SQLBrowseConnect(SQLHDBC hdbc,
	SQLCHAR*     szConnStrIn,
	SQLSMALLINT  cbConnStrIn,
	SQLCHAR*     szConnStrOut,
	SQLSMALLINT  cbConnStrOutMax,
	SQLSMALLINT* pcbConnStrOut)
{
	return ::SQLBrowseConnectA(hdbc,
		szConnStrIn,
		cbConnStrIn,
		szConnStrOut,
		cbConnStrOutMax,
		pcbConnStrOut);
}


inline SQLRETURN SQLColumnPrivileges(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName,
	SQLCHAR*    szColumnName,
	SQLSMALLINT cbColumnName)
{
	return ::SQLColumnPrivilegesA(hstmt,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szTableName,
		cbTableName,
		szColumnName,
		cbColumnName);
}


inline SQLRETURN SQLForeignKeys(SQLHSTMT hstmt,
	SQLCHAR*    szPkCatalogName,
	SQLSMALLINT cbPkCatalogName,
	SQLCHAR*    szPkSchemaName,
	SQLSMALLINT cbPkSchemaName,
	SQLCHAR*    szPkTableName,
	SQLSMALLINT cbPkTableName,
	SQLCHAR*    szFkCatalogName,
	SQLSMALLINT cbFkCatalogName,
	SQLCHAR*    szFkSchemaName,
	SQLSMALLINT cbFkSchemaName,
	SQLCHAR*    szFkTableName,
	SQLSMALLINT cbFkTableName)
{
	return ::SQLForeignKeysA(hstmt,
		szPkCatalogName,
		cbPkCatalogName,
		szPkSchemaName,
		cbPkSchemaName,
		szPkTableName,
		cbPkTableName,
		szFkCatalogName,
		cbFkCatalogName,
		szFkSchemaName,
		cbFkSchemaName,
		szFkTableName,
		cbFkTableName);
}


inline SQLRETURN SQLNativeSql(SQLHDBC hdbc,
	SQLCHAR*    szSqlStrIn,
	SQLINTEGER  cbSqlStrIn,
	SQLCHAR*    szSqlStr,
	SQLINTEGER  cbSqlStrMax,
	SQLINTEGER* pcbSqlStr)
{
	return ::SQLNativeSqlA(hdbc,
		szSqlStrIn,
		cbSqlStrIn,
		szSqlStr,
		cbSqlStrMax,
		pcbSqlStr);
}


inline SQLRETURN SQLPrimaryKeys(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName)
{
	return ::SQLPrimaryKeysA(hstmt,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szTableName,
		cbTableName);
}


inline SQLRETURN SQLProcedureColumns(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szProcName,
	SQLSMALLINT cbProcName,
	SQLCHAR*    szColumnName,
	SQLSMALLINT cbColumnName)
{
	return ::SQLProcedureColumnsA(hstmt,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szProcName,
		cbProcName,
		szColumnName,
		cbColumnName);
}


inline SQLRETURN SQLProcedures(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szProcName,
	SQLSMALLINT cbProcName)
{
	return ::SQLProceduresA(hstmt,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szProcName,
		cbProcName);
}


inline SQLRETURN SQLTablePrivileges(SQLHSTMT hstmt,
	SQLCHAR*    szCatalogName,
	SQLSMALLINT cbCatalogName,
	SQLCHAR*    szSchemaName,
	SQLSMALLINT cbSchemaName,
	SQLCHAR*    szTableName,
	SQLSMALLINT cbTableName)
{
	return ::SQLTablePrivilegesA(hstmt,
		szCatalogName,
		cbCatalogName,
		szSchemaName,
		cbSchemaName,
		szTableName,
		cbTableName);
}


inline SQLRETURN SQLDrivers(SQLHENV henv,
	SQLUSMALLINT fDirection,
	SQLCHAR*     szDriverDesc,
	SQLSMALLINT  cbDriverDescMax,
	SQLSMALLINT* pcbDriverDesc,
	SQLCHAR*     szDriverAttributes,
	SQLSMALLINT  cbDrvrAttrMax,
	SQLSMALLINT* pcbDrvrAttr)
{
	return ::SQLDriversA(henv,
		fDirection,
		szDriverDesc,
		cbDriverDescMax,
		pcbDriverDesc,
		szDriverAttributes,
		cbDrvrAttrMax,
		pcbDrvrAttr);
}


#endif // POCO_ODBC_UNICODE


} } } // namespace Poco::Data::ODBC


#endif // ODBC_Unicode_INCLUDED
