#include <sql.h>
#include <sqlext.h>

#include <malloc.h>

#include "Log.h"
#include "Environment.h"
#include "Connection.h"
#include "Statement.h"
#include "utils.h"


extern "C"
{

RETCODE SQL_API
SQLSetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute,
    SQLPOINTER value, SQLINTEGER value_length)
{
	LOG(__FUNCTION__);

	return doWith<Environment>(environment_handle, [&](Environment & environment)
	{
		LOG("attr: " << attribute);

		switch (attribute)
		{
			case SQL_ATTR_CONNECTION_POOLING:
			case SQL_ATTR_CP_MATCH:
			case SQL_ATTR_OUTPUT_NTS:
				return SQL_SUCCESS;

			default:
				throw std::runtime_error("Unsupported environment attribute.");

			case SQL_ATTR_ODBC_VERSION:
				intptr_t int_value = reinterpret_cast<intptr_t>(value);
				if (int_value != SQL_OV_ODBC2 && int_value != SQL_OV_ODBC3)
					throw std::runtime_error("Unsupported ODBC version.");

				environment.odbc_version = int_value;
				LOG("Set ODBC version to " << int_value);

				return SQL_SUCCESS;
		}
	});
}


RETCODE SQL_API
SQLGetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute,
    SQLPOINTER out_value, SQLINTEGER out_value_max_length, SQLINTEGER * out_value_length)
{
	LOG(__FUNCTION__);

	return doWith<Environment>(environment_handle, [&](Environment & environment)
	{
		LOG("attr: " << attribute);

		switch (attribute)
		{
			case SQL_ATTR_CONNECTION_POOLING:
			case SQL_ATTR_CP_MATCH:
			case SQL_ATTR_OUTPUT_NTS:
			default:
				throw std::runtime_error("Unsupported environment attribute.");

			case SQL_ATTR_ODBC_VERSION:
				*reinterpret_cast<intptr_t*>(out_value) = environment.odbc_version;
				if (out_value_length)
					*out_value_length = sizeof(SQLUINTEGER);

				return SQL_SUCCESS;
		}
	});
}


RETCODE SQL_API
SQLSetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute,
	SQLPOINTER value, SQLINTEGER value_length)
{
	LOG(__FUNCTION__);

	return doWith<Connection>(connection_handle, [&](Connection & connection)
	{
		LOG("attr: " << attribute);

		switch (attribute)
		{
			case SQL_ATTR_CONNECTION_TIMEOUT:
			case SQL_ATTR_LOGIN_TIMEOUT:
			{
				auto timeout = static_cast<SQLUSMALLINT>(reinterpret_cast<intptr_t>(value));
				LOG("Timeout: " << timeout);
				connection.session.setTimeout(Poco::Timespan(timeout));
				return SQL_SUCCESS;
			}

			case SQL_ATTR_ACCESS_MODE:
			case SQL_ATTR_ASYNC_ENABLE:
			case SQL_ATTR_AUTO_IPD:
			case SQL_ATTR_AUTOCOMMIT:
			case SQL_ATTR_CONNECTION_DEAD:
			case SQL_ATTR_CURRENT_CATALOG:
			case SQL_ATTR_METADATA_ID:
			case SQL_ATTR_ODBC_CURSORS:
			case SQL_ATTR_PACKET_SIZE:
			case SQL_ATTR_QUIET_MODE:
			case SQL_ATTR_TRACE:
			case SQL_ATTR_TRACEFILE:
			case SQL_ATTR_TRANSLATE_LIB:
			case SQL_ATTR_TRANSLATE_OPTION:
			case SQL_ATTR_TXN_ISOLATION:
				return SQL_SUCCESS;

			default:
				throw std::runtime_error("Unsupported connection attribute.");
		}
	});
}


RETCODE SQL_API
SQLGetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute,
	SQLPOINTER out_value, SQLINTEGER out_value_max_length, SQLINTEGER * out_value_length)
{
	LOG(__FUNCTION__);

	return doWith<Connection>(connection_handle, [&](Connection & connection)
	{
		LOG("attr: " << attribute);

		switch (attribute)
		{
			case SQL_ATTR_CONNECTION_TIMEOUT:
			case SQL_ATTR_LOGIN_TIMEOUT:
			case SQL_ATTR_ACCESS_MODE:
			case SQL_ATTR_ASYNC_ENABLE:
			case SQL_ATTR_AUTO_IPD:
			case SQL_ATTR_AUTOCOMMIT:
			case SQL_ATTR_CONNECTION_DEAD:
			case SQL_ATTR_CURRENT_CATALOG:
			case SQL_ATTR_METADATA_ID:
			case SQL_ATTR_ODBC_CURSORS:
			case SQL_ATTR_PACKET_SIZE:
			case SQL_ATTR_QUIET_MODE:
			case SQL_ATTR_TRACE:
			case SQL_ATTR_TRACEFILE:
			case SQL_ATTR_TRANSLATE_LIB:
			case SQL_ATTR_TRANSLATE_OPTION:
			case SQL_ATTR_TXN_ISOLATION:
			default:
				throw std::runtime_error("Unsupported connection attribute.");
		}

		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLSetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute,
	SQLPOINTER value, SQLINTEGER value_length)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		LOG("attr: " << attribute);

		switch (attribute)
		{
			case SQL_ATTR_APP_ROW_DESC:
			case SQL_ATTR_APP_PARAM_DESC:
			case SQL_ATTR_IMP_ROW_DESC:
			case SQL_ATTR_IMP_PARAM_DESC:
			case SQL_ATTR_CURSOR_SCROLLABLE:
			case SQL_ATTR_CURSOR_SENSITIVITY:
			case SQL_ATTR_ASYNC_ENABLE:
			case SQL_ATTR_CONCURRENCY:
			case SQL_ATTR_CURSOR_TYPE:
			case SQL_ATTR_ENABLE_AUTO_IPD:
			case SQL_ATTR_FETCH_BOOKMARK_PTR:
			case SQL_ATTR_KEYSET_SIZE:
			case SQL_ATTR_MAX_LENGTH:
			case SQL_ATTR_MAX_ROWS:
			case SQL_ATTR_NOSCAN:
			case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
			case SQL_ATTR_PARAM_BIND_TYPE:
			case SQL_ATTR_PARAM_OPERATION_PTR:
			case SQL_ATTR_PARAM_STATUS_PTR:
			case SQL_ATTR_PARAMS_PROCESSED_PTR:
			case SQL_ATTR_PARAMSET_SIZE:
			case SQL_ATTR_QUERY_TIMEOUT:
			case SQL_ATTR_RETRIEVE_DATA:
			case SQL_ATTR_ROW_BIND_OFFSET_PTR:
			case SQL_ATTR_ROW_BIND_TYPE:
			case SQL_ATTR_ROW_NUMBER:
			case SQL_ATTR_ROW_OPERATION_PTR:
			case SQL_ATTR_ROW_STATUS_PTR:
			case SQL_ATTR_ROWS_FETCHED_PTR:
			case SQL_ATTR_ROW_ARRAY_SIZE:
			case SQL_ATTR_SIMULATE_CURSOR:
			case SQL_ATTR_USE_BOOKMARKS:
				return SQL_SUCCESS;

			default:
				throw std::runtime_error("Unsupported statement attribute.");
		}
	});
}


RETCODE SQL_API
SQLGetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute,
    SQLPOINTER out_value, SQLINTEGER out_value_max_length, SQLINTEGER * out_value_length)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		LOG("attr: " << attribute);

		switch (attribute)
		{
			case SQL_ATTR_APP_ROW_DESC:
			case SQL_ATTR_APP_PARAM_DESC:
			case SQL_ATTR_IMP_ROW_DESC:
			case SQL_ATTR_IMP_PARAM_DESC:
			case SQL_ATTR_CURSOR_SCROLLABLE:
			case SQL_ATTR_CURSOR_SENSITIVITY:
			case SQL_ATTR_ASYNC_ENABLE:
			case SQL_ATTR_CONCURRENCY:
			case SQL_ATTR_CURSOR_TYPE:
			case SQL_ATTR_ENABLE_AUTO_IPD:
			case SQL_ATTR_FETCH_BOOKMARK_PTR:
			case SQL_ATTR_KEYSET_SIZE:
			case SQL_ATTR_MAX_LENGTH:
			case SQL_ATTR_MAX_ROWS:
			case SQL_ATTR_NOSCAN:
			case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
			case SQL_ATTR_PARAM_BIND_TYPE:
			case SQL_ATTR_PARAM_OPERATION_PTR:
			case SQL_ATTR_PARAM_STATUS_PTR:
			case SQL_ATTR_PARAMS_PROCESSED_PTR:
			case SQL_ATTR_PARAMSET_SIZE:
			case SQL_ATTR_QUERY_TIMEOUT:
			case SQL_ATTR_RETRIEVE_DATA:
			case SQL_ATTR_ROW_BIND_OFFSET_PTR:
			case SQL_ATTR_ROW_BIND_TYPE:
			case SQL_ATTR_ROW_NUMBER:
			case SQL_ATTR_ROW_OPERATION_PTR:
			case SQL_ATTR_ROW_STATUS_PTR:
			case SQL_ATTR_ROWS_FETCHED_PTR:
			case SQL_ATTR_ROW_ARRAY_SIZE:
			case SQL_ATTR_SIMULATE_CURSOR:
			case SQL_ATTR_USE_BOOKMARKS:
			default:
				throw std::runtime_error("Unsupported statement attribute.");
		}

		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLGetConnectOption(SQLHDBC connection_handle, UWORD attribute, PTR out_value)
{
	LOG(__FUNCTION__);
	SQLINTEGER value_max_length = 64;
	SQLINTEGER value_length_unused = 0;
	return SQLGetConnectAttr(connection_handle, attribute, out_value, value_max_length, &value_length_unused);
}

RETCODE SQL_API
SQLGetStmtOption(SQLHSTMT statement_handle, UWORD attribute, PTR out_value)
{
	LOG(__FUNCTION__);
	SQLINTEGER value_max_length = 64;
	SQLINTEGER value_length_unused = 0;
	return SQLGetStmtAttr(statement_handle, attribute, out_value, value_max_length, &value_length_unused);
}

RETCODE SQL_API
SQLSetConnectOption(SQLHDBC connection_handle, UWORD attribute, SQLULEN value)
{
	LOG(__FUNCTION__);
	return SQLSetConnectAttr(connection_handle, attribute, reinterpret_cast<void *>(value), sizeof(value));
}

RETCODE SQL_API
SQLSetStmtOption(SQLHSTMT statement_handle, UWORD attribute, SQLULEN value)
{
	LOG(__FUNCTION__);
	return SQLSetConnectAttr(statement_handle, attribute, reinterpret_cast<void *>(value), sizeof(value));
}


}
