#include <sql.h>
#include <stdio.h>
#include <iostream>
#include <stdexcept>
#include <malloc.h>


static void mylog(const char * message)
{
/*	static struct Once
	{
		Once()
		{
			std::string stderr_path = "/tmp/clickhouse-odbc-stderr";
			if (!freopen(stderr_path.c_str(), "a+", stderr))
				throw std::logic_error("Cannot freopen stderr.");
		}
	} once;*/

	std::cerr << message << "\n";
}



struct Environment
{
};

struct Connection
{
};

struct Statement
{
};


RETCODE allocEnv(SQLHENV * out_environment)
{
	if (nullptr == out_environment)
		return SQL_INVALID_HANDLE;

	*out_environment = new Environment;

	return SQL_SUCCESS;
}

RETCODE allocConnect(SQLHENV environment, SQLHDBC * out_connection)
{
	if (nullptr == out_connection)
		return SQL_INVALID_HANDLE;

	*out_connection = new Connection;

	return SQL_SUCCESS;
}

RETCODE allocStmt(SQLHDBC connection, SQLHSTMT * out_statement)
{
	if (nullptr == out_statement)
		return SQL_INVALID_HANDLE;

	*out_statement = new Statement;

	return SQL_SUCCESS;
}


/// Интерфейс библиотеки.
extern "C"
{


RETCODE SQL_API
SQLAllocHandle(SQLSMALLINT handleType,
               SQLHANDLE inputHandle,
               SQLHANDLE *outputHandle)
{
	mylog(__PRETTY_FUNCTION__);

	switch (handleType)
	{
		case SQL_HANDLE_ENV:
			return allocEnv((SQLHENV *)outputHandle);
		case SQL_HANDLE_DBC:
			return allocConnect((SQLHENV)inputHandle, (SQLHDBC *)outputHandle);
		case SQL_HANDLE_STMT:
			return allocStmt((SQLHDBC)inputHandle, (SQLHSTMT *)outputHandle);
		default:
			return SQL_ERROR;
	}
}


RETCODE SQL_API
SQLFreeHandle(SQLSMALLINT handleType, SQLHANDLE handle)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLBindCol(HSTMT StatementHandle,
		   SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
		   PTR TargetValue, SQLLEN BufferLength,
		   SQLLEN *StrLen_or_Ind)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLCancel(HSTMT StatementHandle)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLColumns(HSTMT StatementHandle,
		   SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
		   SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
		   SQLCHAR *TableName, SQLSMALLINT NameLength3,
		   SQLCHAR *ColumnName, SQLSMALLINT NameLength4)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLConnect(HDBC ConnectionHandle,
		   SQLCHAR *ServerName, SQLSMALLINT NameLength1,
		   SQLCHAR *UserName, SQLSMALLINT NameLength2,
		   SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLDriverConnect(HDBC hdbc,
				 HWND hwnd,
				 SQLCHAR FAR * szConnStrIn,
				 SQLSMALLINT cbConnStrIn,
				 SQLCHAR FAR * szConnStrOut,
				 SQLSMALLINT cbConnStrOutMax,
				 SQLSMALLINT FAR * pcbConnStrOut,
				 SQLUSMALLINT fDriverCompletion)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLBrowseConnect(HDBC hdbc,
				 SQLCHAR *szConnStrIn,
				 SQLSMALLINT cbConnStrIn,
				 SQLCHAR *szConnStrOut,
				 SQLSMALLINT cbConnStrOutMax,
				 SQLSMALLINT *pcbConnStrOut)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLDataSources(HENV EnvironmentHandle,
			   SQLUSMALLINT Direction, SQLCHAR *ServerName,
			   SQLSMALLINT BufferLength1, SQLSMALLINT *NameLength1,
			   SQLCHAR *Description, SQLSMALLINT BufferLength2,
			   SQLSMALLINT *NameLength2)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLDescribeCol(HSTMT StatementHandle,
			   SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
			   SQLSMALLINT BufferLength, SQLSMALLINT *NameLength,
			   SQLSMALLINT *DataType, SQLULEN *ColumnSize,
			   SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLDisconnect(HDBC ConnectionHandle)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLExecDirect(HSTMT StatementHandle,
			  SQLCHAR *StatementText, SQLINTEGER TextLength)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLExecute(HSTMT StatementHandle)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLFetch(HSTMT StatementHandle)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLFreeStmt(HSTMT StatementHandle,
			SQLUSMALLINT Option)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLGetCursorName(HSTMT StatementHandle,
				 SQLCHAR *CursorName, SQLSMALLINT BufferLength,
				 SQLSMALLINT *NameLength)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLGetData(HSTMT StatementHandle,
		   SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
		   PTR TargetValue, SQLLEN BufferLength,
		   SQLLEN *StrLen_or_Ind)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLGetFunctions(HDBC ConnectionHandle,
				SQLUSMALLINT FunctionId, SQLUSMALLINT *Supported)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLGetInfo(HDBC ConnectionHandle,
		   SQLUSMALLINT InfoType, PTR InfoValue,
		   SQLSMALLINT BufferLength, SQLSMALLINT *StringLength)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLGetTypeInfo(HSTMT StatementHandle,
			   SQLSMALLINT DataType)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLNumResultCols(HSTMT StatementHandle,
				 SQLSMALLINT *ColumnCount)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLParamData(HSTMT StatementHandle,
			 PTR *Value)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLPrepare(HSTMT StatementHandle,
		   SQLCHAR *StatementText, SQLINTEGER TextLength)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLPutData(HSTMT StatementHandle,
		   PTR Data, SQLLEN StrLen_or_Ind)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLRowCount(HSTMT StatementHandle,
			SQLLEN *RowCount)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetCursorName(HSTMT StatementHandle,
				 SQLCHAR *CursorName, SQLSMALLINT NameLength)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetParam(HSTMT StatementHandle,
			SQLUSMALLINT ParameterNumber, SQLSMALLINT ValueType,
			SQLSMALLINT ParameterType, SQLULEN LengthPrecision,
			SQLSMALLINT ParameterScale, PTR ParameterValue,
			SQLLEN *StrLen_or_Ind)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSpecialColumns(HSTMT StatementHandle,
				  SQLUSMALLINT IdentifierType, SQLCHAR *CatalogName,
				  SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
				  SQLSMALLINT NameLength2, SQLCHAR *TableName,
				  SQLSMALLINT NameLength3, SQLUSMALLINT Scope,
				  SQLUSMALLINT Nullable)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLStatistics(HSTMT StatementHandle,
			  SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
			  SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
			  SQLCHAR *TableName, SQLSMALLINT NameLength3,
			  SQLUSMALLINT Unique, SQLUSMALLINT Reserved)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLTables(HSTMT StatementHandle,
		  SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
		  SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
		  SQLCHAR *TableName, SQLSMALLINT NameLength3,
		  SQLCHAR *TableType, SQLSMALLINT NameLength4)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLColumnPrivileges(HSTMT hstmt,
					SQLCHAR *szCatalogName,
					SQLSMALLINT cbCatalogName,
					SQLCHAR *szSchemaName,
					SQLSMALLINT cbSchemaName,
					SQLCHAR *szTableName,
					SQLSMALLINT cbTableName,
					SQLCHAR *szColumnName,
					SQLSMALLINT cbColumnName)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLDescribeParam(HSTMT hstmt,
				 SQLUSMALLINT ipar,
				 SQLSMALLINT *pfSqlType,
				 SQLULEN *pcbParamDef,
				 SQLSMALLINT *pibScale,
				 SQLSMALLINT *pfNullable)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLExtendedFetch(HSTMT hstmt,
				 SQLUSMALLINT fFetchType,
				 SQLLEN irow,
#if defined(WITH_UNIXODBC) && (SIZEOF_LONG != 8)
				 SQLROWSETSIZE *pcrow,
#else
				 SQLULEN *pcrow,
#endif /* WITH_UNIXODBC */
				 SQLUSMALLINT *rgfRowStatus)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLForeignKeys(HSTMT hstmt,
			   SQLCHAR *szPkCatalogName,
			   SQLSMALLINT cbPkCatalogName,
			   SQLCHAR *szPkSchemaName,
			   SQLSMALLINT cbPkSchemaName,
			   SQLCHAR *szPkTableName,
			   SQLSMALLINT cbPkTableName,
			   SQLCHAR *szFkCatalogName,
			   SQLSMALLINT cbFkCatalogName,
			   SQLCHAR *szFkSchemaName,
			   SQLSMALLINT cbFkSchemaName,
			   SQLCHAR *szFkTableName,
			   SQLSMALLINT cbFkTableName)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLMoreResults(HSTMT hstmt)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLNativeSql(HDBC hdbc,
			 SQLCHAR *szSqlStrIn,
			 SQLINTEGER cbSqlStrIn,
			 SQLCHAR *szSqlStr,
			 SQLINTEGER cbSqlStrMax,
			 SQLINTEGER *pcbSqlStr)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLNumParams(HSTMT hstmt,
			 SQLSMALLINT *pcpar)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLPrimaryKeys(HSTMT hstmt,
			   SQLCHAR *szCatalogName,
			   SQLSMALLINT cbCatalogName,
			   SQLCHAR *szSchemaName,
			   SQLSMALLINT cbSchemaName,
			   SQLCHAR *szTableName,
			   SQLSMALLINT cbTableName)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLProcedureColumns(HSTMT hstmt,
					SQLCHAR *szCatalogName,
					SQLSMALLINT cbCatalogName,
					SQLCHAR *szSchemaName,
					SQLSMALLINT cbSchemaName,
					SQLCHAR *szProcName,
					SQLSMALLINT cbProcName,
					SQLCHAR *szColumnName,
					SQLSMALLINT cbColumnName)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLProcedures(HSTMT hstmt,
			  SQLCHAR *szCatalogName,
			  SQLSMALLINT cbCatalogName,
			  SQLCHAR *szSchemaName,
			  SQLSMALLINT cbSchemaName,
			  SQLCHAR *szProcName,
			  SQLSMALLINT cbProcName)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetPos(HSTMT hstmt,
		  SQLSETPOSIROW irow,
		  SQLUSMALLINT fOption,
		  SQLUSMALLINT fLock)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLTablePrivileges(HSTMT hstmt,
				   SQLCHAR *szCatalogName,
				   SQLSMALLINT cbCatalogName,
				   SQLCHAR *szSchemaName,
				   SQLSMALLINT cbSchemaName,
				   SQLCHAR *szTableName,
				   SQLSMALLINT cbTableName)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLBindParameter(HSTMT hstmt,
				 SQLUSMALLINT ipar,
				 SQLSMALLINT fParamType,
				 SQLSMALLINT fCType,
				 SQLSMALLINT fSqlType,
				 SQLULEN cbColDef,
				 SQLSMALLINT ibScale,
				 PTR rgbValue,
				 SQLLEN cbValueMax,
				 SQLLEN *pcbValue)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}


}
