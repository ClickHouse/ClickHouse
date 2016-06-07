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

RETCODE
impl_SQLSetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute,
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


RETCODE
impl_SQLGetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute,
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
				fillOutputNumber<SQLUINTEGER>(environment.odbc_version, out_value, out_value_max_length, out_value_length);
				return SQL_SUCCESS;
		}
	});
}


RETCODE
impl_SQLSetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute,
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
				connection.session.setTimeout(Poco::Timespan(timeout, 0));
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


RETCODE
impl_SQLGetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute,
	SQLPOINTER out_value, SQLINTEGER out_value_max_length, SQLINTEGER * out_value_length)
{
	LOG(__FUNCTION__);

	return doWith<Connection>(connection_handle, [&](Connection & connection) -> RETCODE
	{
		LOG("attr: " << attribute);

		const char * name = nullptr;

		switch (attribute)
		{
			CASE_NUM(SQL_ATTR_CONNECTION_DEAD, SQLUINTEGER, SQL_CD_FALSE);
			CASE_FALLTHROUGH(SQL_ATTR_CONNECTION_TIMEOUT)
			CASE_NUM(SQL_ATTR_LOGIN_TIMEOUT, SQLUSMALLINT, connection.session.getTimeout().seconds())

			case SQL_ATTR_ACCESS_MODE:
			case SQL_ATTR_ASYNC_ENABLE:
			case SQL_ATTR_AUTO_IPD:
			case SQL_ATTR_AUTOCOMMIT:
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


RETCODE
impl_SQLSetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute,
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
			case SQL_ATTR_CURSOR_TYPE:			/// Libreoffice Base
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
			case SQL_ATTR_ROW_STATUS_PTR:		/// Libreoffice Base
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


RETCODE
impl_SQLGetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute,
    SQLPOINTER out_value, SQLINTEGER out_value_max_length, SQLINTEGER * out_value_length)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement) -> RETCODE
	{
		LOG("attr: " << attribute);

		const char * name = nullptr;

		switch (attribute)
		{
			CASE_FALLTHROUGH(SQL_ATTR_APP_ROW_DESC)
			CASE_FALLTHROUGH(SQL_ATTR_APP_PARAM_DESC)
			CASE_FALLTHROUGH(SQL_ATTR_IMP_ROW_DESC)
			CASE_NUM(SQL_ATTR_IMP_PARAM_DESC, SQLPOINTER, (void*)1)

			CASE_NUM(SQL_ATTR_CURSOR_SCROLLABLE, SQLULEN, SQL_NONSCROLLABLE);
			CASE_NUM(SQL_ATTR_CURSOR_SENSITIVITY, SQLULEN, SQL_INSENSITIVE);
			CASE_NUM(SQL_ATTR_ASYNC_ENABLE, SQLULEN, SQL_ASYNC_ENABLE_OFF);
			CASE_NUM(SQL_ATTR_CONCURRENCY, SQLULEN, SQL_CONCUR_READ_ONLY);
			CASE_NUM(SQL_ATTR_CURSOR_TYPE, SQLULEN, SQL_CURSOR_FORWARD_ONLY);
			CASE_NUM(SQL_ATTR_ENABLE_AUTO_IPD, SQLULEN, SQL_FALSE);
			CASE_NUM(SQL_ATTR_MAX_LENGTH, SQLULEN, 0);
			CASE_NUM(SQL_ATTR_MAX_ROWS, SQLULEN, 0);
			CASE_NUM(SQL_ATTR_METADATA_ID, SQLULEN, SQL_FALSE);
			CASE_NUM(SQL_ATTR_NOSCAN, SQLULEN, SQL_NOSCAN_ON);
			CASE_NUM(SQL_ATTR_QUERY_TIMEOUT, SQLULEN, 0);
			CASE_NUM(SQL_ATTR_RETRIEVE_DATA, SQLULEN, SQL_RD_ON);
			CASE_NUM(SQL_ATTR_ROW_NUMBER, SQLULEN, statement.result.getNumRows());

			case SQL_ATTR_FETCH_BOOKMARK_PTR:
			case SQL_ATTR_KEYSET_SIZE:
			case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
			case SQL_ATTR_PARAM_BIND_TYPE:
			case SQL_ATTR_PARAM_OPERATION_PTR:
			case SQL_ATTR_PARAM_STATUS_PTR:
			case SQL_ATTR_PARAMS_PROCESSED_PTR:
			case SQL_ATTR_PARAMSET_SIZE:
			case SQL_ATTR_ROW_BIND_OFFSET_PTR:
			case SQL_ATTR_ROW_BIND_TYPE:	/// TODO
			case SQL_ATTR_ROW_OPERATION_PTR:
			case SQL_ATTR_ROW_STATUS_PTR:
			case SQL_ATTR_ROWS_FETCHED_PTR:
			case SQL_ATTR_ROW_ARRAY_SIZE:
			case SQL_ATTR_SIMULATE_CURSOR:
			case SQL_ATTR_USE_BOOKMARKS:	/// Libreoffice Base
			default:
				throw std::runtime_error("Unsupported statement attribute.");
		}

		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLSetEnvAttr(SQLHENV handle, SQLINTEGER attribute,
    SQLPOINTER value, SQLINTEGER value_length)
{
	return impl_SQLSetEnvAttr(handle, attribute, value, value_length);
}

RETCODE SQL_API
SQLSetConnectAttr(SQLHENV handle, SQLINTEGER attribute,
    SQLPOINTER value, SQLINTEGER value_length)
{
	return impl_SQLSetConnectAttr(handle, attribute, value, value_length);
}

RETCODE SQL_API
SQLSetStmtAttr(SQLHENV handle, SQLINTEGER attribute,
    SQLPOINTER value, SQLINTEGER value_length)
{
	return impl_SQLSetStmtAttr(handle, attribute, value, value_length);
}

RETCODE SQL_API
SQLGetEnvAttr(SQLHSTMT handle, SQLINTEGER attribute,
    SQLPOINTER out_value, SQLINTEGER out_value_max_length, SQLINTEGER * out_value_length)
{
	return impl_SQLGetEnvAttr(handle, attribute, out_value, out_value_max_length, out_value_length);
}

RETCODE SQL_API
SQLGetConnectAttr(SQLHSTMT handle, SQLINTEGER attribute,
    SQLPOINTER out_value, SQLINTEGER out_value_max_length, SQLINTEGER * out_value_length)
{
	return impl_SQLGetConnectAttr(handle, attribute, out_value, out_value_max_length, out_value_length);
}

RETCODE SQL_API
SQLGetStmtAttr(SQLHSTMT handle, SQLINTEGER attribute,
    SQLPOINTER out_value, SQLINTEGER out_value_max_length, SQLINTEGER * out_value_length)
{
	return impl_SQLGetStmtAttr(handle, attribute, out_value, out_value_max_length, out_value_length);
}


RETCODE SQL_API
SQLGetConnectOption(SQLHDBC connection_handle, UWORD attribute, PTR out_value)
{
	LOG(__FUNCTION__);
	SQLINTEGER value_max_length = 64;
	SQLINTEGER value_length_unused = 0;
	return impl_SQLGetConnectAttr(connection_handle, attribute, out_value, value_max_length, &value_length_unused);
}

RETCODE SQL_API
SQLGetStmtOption(SQLHSTMT statement_handle, UWORD attribute, PTR out_value)
{
	LOG(__FUNCTION__);
	SQLINTEGER value_max_length = 64;
	SQLINTEGER value_length_unused = 0;
	return impl_SQLGetStmtAttr(statement_handle, attribute, out_value, value_max_length, &value_length_unused);
}

RETCODE SQL_API
SQLSetConnectOption(SQLHDBC connection_handle, UWORD attribute, SQLULEN value)
{
	LOG(__FUNCTION__);
	return impl_SQLSetConnectAttr(connection_handle, attribute, reinterpret_cast<void *>(value), sizeof(value));
}

RETCODE SQL_API
SQLSetStmtOption(SQLHSTMT statement_handle, UWORD attribute, SQLULEN value)
{
	LOG(__FUNCTION__);
	return impl_SQLSetConnectAttr(statement_handle, attribute, reinterpret_cast<void *>(value), sizeof(value));
}


}
