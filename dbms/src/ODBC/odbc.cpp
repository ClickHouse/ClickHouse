#include <sql.h>
#include <sqlext.h>

#include <stdio.h>
#include <malloc.h>
#include <string.h>

#include <iostream>
#include <stdexcept>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/NumberParser.h>


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


struct StringRef
{
	const char * data = nullptr;
	size_t size = 0;

	StringRef() {}
	StringRef(const char * c_str) { *this = c_str; }
	StringRef & operator= (const char * c_str) { data = c_str; size = strlen(c_str); }

	std::string toString() const { return {data, size}; }

	bool operator== (const char * rhs) const
	{
		return size == strlen(rhs) && 0 == memcmp(data, rhs, strlen(rhs));
	}

	operator bool() const { return data != nullptr; }
};


/// Парсит строку вида key1=value1;key2=value2... TODO Парсинг значений в фигурных скобках.
static const char * nextKeyValuePair(const char * data, const char * end, StringRef & out_key, StringRef & out_value)
{
	if (data >= end)
		return nullptr;

	const char * key_begin = data;
	const char * key_end = reinterpret_cast<const char *>(memchr(key_begin, '=', end - key_begin));
	if (!key_end)
		return nullptr;

	const char * value_begin = key_end + 1;
	const char * value_end;
	if (value_begin >= end)
		value_end = value_begin;
	else
	{
		value_end = reinterpret_cast<const char *>(memchr(value_begin, ';', end - value_begin));
		if (!value_end)
			value_end = end;
	}

	out_key.data = key_begin;
	out_key.size = key_end - key_begin;

	out_value.data = value_begin;
	out_value.size = value_end - value_begin;

	if (value_end < end && *value_end == ';')
		return value_end + 1;
	return value_end;
}


struct Environment
{
};

struct Connection
{
	std::string host = "localhost";
	uint16_t port = 8123;
	std::string user = "default";
	std::string password;
	std::string database = "default";

	Poco::Net::HTTPClientSession session;
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

RETCODE freeEnv(SQLHENV environment)
{
	delete reinterpret_cast<Environment *>(environment);
	return SQL_SUCCESS;
}

RETCODE freeConnect(SQLHENV connection)
{
	delete reinterpret_cast<Connection *>(connection);
	return SQL_SUCCESS;
}

RETCODE freeStmt(SQLHENV statement)
{
	delete reinterpret_cast<Statement *>(statement);
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

	switch (handleType)
	{
		case SQL_HANDLE_ENV:
			return freeEnv((SQLHENV)handle);
		case SQL_HANDLE_DBC:
			return freeConnect((SQLHDBC)handle);
		case SQL_HANDLE_STMT:
			return freeStmt((SQLHDBC)handle);
		default:
			return SQL_ERROR;
	}
}


RETCODE SQL_API
SQLConnect(HDBC ConnectionHandle,
		   SQLCHAR *ServerName, SQLSMALLINT NameLength1,
		   SQLCHAR *UserName, SQLSMALLINT NameLength2,
		   SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{
	mylog(__PRETTY_FUNCTION__);

	std::cerr << ServerName << "\n";

	return SQL_ERROR;
}


RETCODE SQL_API
SQLDriverConnect(HDBC connection_handle,
				 HWND unused_window,
				 SQLCHAR FAR * connection_str_in,
				 SQLSMALLINT connection_str_in_size,
				 SQLCHAR FAR * connection_str_out,
				 SQLSMALLINT connection_str_out_max_size,
				 SQLSMALLINT FAR * connection_str_out_size,
				 SQLUSMALLINT driver_completion)
{
	mylog(__PRETTY_FUNCTION__);

	if (nullptr == connection_handle)
		return SQL_INVALID_HANDLE;

	Connection & connection = *reinterpret_cast<Connection *>(connection_handle);

	if (connection.session.connected())
		return SQL_ERROR;

	if (nullptr == connection_str_in)
		return SQL_ERROR;

	/// Почему-то при использовании isql, сюда передаётся -3. TODO С чего бы это?
	if (connection_str_in_size < 0)
		connection_str_in_size = strlen(reinterpret_cast<const char *>(connection_str_in));

	/// connection_str_in - строка вида DSN=ClickHouse;UID=default;PWD=password

	const char * data = reinterpret_cast<const char *>(connection_str_in);
	const char * end = reinterpret_cast<const char *>(connection_str_in) + connection_str_in_size;

	StringRef current_key;
	StringRef current_value;

	while (data = nextKeyValuePair(data, end, current_key, current_value))
	{
		if (current_key == "UID")
			connection.user = current_value.toString();
		else if (current_key == "PWD")
			connection.password = current_value.toString();
		else if (current_key == "HOST")
			connection.host = current_value.toString();
		else if (current_key == "PORT")
		{
			int int_port = 0;
			if (Poco::NumberParser::tryParse(current_value.toString(), int_port))
				connection.port = int_port;
			else
				return SQL_ERROR;
		}
		else if (current_key == "DATABASE")
			connection.database = current_value.toString();
	}

	connection.session.setHost(connection.host);
	connection.session.setPort(connection.port);
	connection.session.setKeepAlive(true);

	/// TODO Таймаут.
	/// TODO Ловля исключений.

	std::cerr << connection_str_in << "\n";

	return SQL_SUCCESS;
}


RETCODE SQL_API
SQLGetInfo(HDBC connection_handle,
		   SQLUSMALLINT info_type, PTR out_info_value,
		   SQLSMALLINT out_info_value_max_length, SQLSMALLINT * out_info_value_length)
{
	mylog(__PRETTY_FUNCTION__);

	StringRef res;

	switch (info_type)
	{
		case SQL_DRIVER_VER:
			res = "1.0";
			break;
		case SQL_DRIVER_ODBC_VER:
			res = "1.0";
			break;
		case SQL_DRIVER_NAME:
			res = "ClickHouse ODBC Driver";
			break;
		case SQL_DBMS_NAME:
			res = "ClickHouse";
			break;
		case SQL_SERVER_NAME:
			res = "ClickHouse";
			break;
		case SQL_DATA_SOURCE_NAME:
			res = "ClickHouse";
			break;
		default:
			std::cerr << "Unsupported info type: " << info_type << "\n";	/// TODO Унификация трассировки.
			return SQL_ERROR;
	}

	return SQL_SUCCESS;
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


/*
/// Эта функция может быть реализована в driver manager-е.
RETCODE SQL_API
SQLGetFunctions(HDBC ConnectionHandle,
				SQLUSMALLINT FunctionId, SQLUSMALLINT *Supported)
{
	mylog(__PRETTY_FUNCTION__);
	return SQL_ERROR;
}*/


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
