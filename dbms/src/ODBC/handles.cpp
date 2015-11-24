#include <sql.h>
#include <sqlext.h>

#include <malloc.h>

#include "Log.h"
#include "Environment.h"
#include "Connection.h"
#include "Statement.h"
#include "utils.h"


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

	*out_connection = new Connection(*reinterpret_cast<Environment *>(environment));
	return SQL_SUCCESS;
}

RETCODE allocStmt(SQLHDBC connection, SQLHSTMT * out_statement)
{
	if (nullptr == out_statement || nullptr == connection)
		return SQL_INVALID_HANDLE;

	*out_statement = new Statement(*reinterpret_cast<Connection *>(connection));
	return SQL_SUCCESS;
}

template <typename Handle>
RETCODE freeHandle(SQLHANDLE handle_opaque)
{
	delete reinterpret_cast<Handle *>(handle_opaque);
	handle_opaque = nullptr;
	return SQL_SUCCESS;
}


extern "C"
{

RETCODE SQL_API
SQLAllocHandle(SQLSMALLINT handle_type,
               SQLHANDLE input_handle,
               SQLHANDLE * output_handle)
{
	LOG(__FUNCTION__);

	switch (handle_type)
	{
		case SQL_HANDLE_ENV:
			return allocEnv((SQLHENV *)output_handle);
		case SQL_HANDLE_DBC:
			return allocConnect((SQLHENV)input_handle, (SQLHDBC *)output_handle);
		case SQL_HANDLE_STMT:
			return allocStmt((SQLHDBC)input_handle, (SQLHSTMT *)output_handle);
		default:
			return SQL_ERROR;
	}
}

RETCODE SQL_API
SQLAllocEnv(SQLHDBC * output_handle)
{
	LOG(__FUNCTION__);
	return allocEnv(output_handle);
}

RETCODE SQL_API
SQLAllocConnect(SQLHENV input_handle, SQLHDBC * output_handle)
{
	LOG(__FUNCTION__);
	return allocConnect(input_handle, output_handle);
}

RETCODE SQL_API
SQLAllocStmt(SQLHDBC input_handle, SQLHSTMT * output_handle)
{
	LOG(__FUNCTION__);
	return allocStmt(input_handle, output_handle);
}


RETCODE SQL_API
SQLFreeHandle(SQLSMALLINT handleType, SQLHANDLE handle)
{
	LOG(__FUNCTION__);

	switch (handleType)
	{
		case SQL_HANDLE_ENV:
			return freeHandle<Environment>(handle);
		case SQL_HANDLE_DBC:
			return freeHandle<Connection>(handle);
		case SQL_HANDLE_STMT:
			return freeHandle<Statement>(handle);
		default:
			return SQL_ERROR;
	}
}


RETCODE SQL_API
SQLFreeEnv(HENV handle)
{
	LOG(__FUNCTION__);
	return freeHandle<Environment>(handle);
}

RETCODE SQL_API
SQLFreeConnect(HDBC handle)
{
	LOG(__FUNCTION__);
	return freeHandle<Connection>(handle);
}

RETCODE SQL_API
SQLFreeStmt(HSTMT statement_handle,
			SQLUSMALLINT option)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement) -> RETCODE
	{
		std::cerr << "option: " << option << "\n";

		switch (option)
		{
			case SQL_DROP:
				return freeHandle<Statement>(statement_handle);

			case SQL_CLOSE:				/// Закрыть курсор, проигнорировать оставшиеся результаты. Если курсора нет, то noop.
				statement.reset();

			case SQL_UNBIND:
				statement.bindings.clear();
				return SQL_SUCCESS;

			case SQL_RESET_PARAMS:
				return SQL_SUCCESS;

			default:
				return SQL_ERROR;
		}
	});
}

}
