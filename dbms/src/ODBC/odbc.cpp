#include <sql.h>
#include <sqlext.h>

#include <stdio.h>
#include <malloc.h>
#include <string.h>

#include <iostream>
#include <sstream>
#include <stdexcept>

#include "StringRef.h"
#include "Log.h"
#include "DiagnosticRecord.h"
#include "Environment.h"
#include "Connection.h"
#include "Statement.h"
#include "ResultSet.h"
#include "utils.h"


/** Функции из ODBC интерфейса не могут напрямую вызывать другие функции.
  * Потому что будет вызвана не функция из этой библиотеки, а обёртка из driver manager-а,
  *  которая может неправильно работать, будучи вызванной изнутри другой функции.
  * Неправильно - потому что driver manager оборачивает все handle в свои другие,
  *  которые имеют уже другие адреса.
  */

extern "C"
{


RETCODE SQL_API
SQLConnect(HDBC connection_handle,
		   SQLCHAR * server_name, SQLSMALLINT server_name_size,
		   SQLCHAR * user, SQLSMALLINT user_size,
		   SQLCHAR * password, SQLSMALLINT password_size)
{
	LOG(__FUNCTION__);

	return doWith<Connection>(connection_handle, [&](Connection & connection)
	{
		std::string user_str = stringFromSQLChar(user, user_size);
		std::string password_str = stringFromSQLChar(password, password_size);

		connection.init("", 0, user_str, password_str, "");
		return SQL_SUCCESS;
	});
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
	LOG(__FUNCTION__);

	return doWith<Connection>(connection_handle, [&](Connection & connection)
	{
		connection.init(stringFromSQLChar(connection_str_in, connection_str_in_size));
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLPrepare(HSTMT statement_handle,
		   SQLCHAR * statement_text, SQLINTEGER statement_text_size)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		if (!statement.query.empty())
			throw std::runtime_error("Prepare called, but statement query is not empty.");

		statement.query = stringFromSQLChar(statement_text, statement_text_size);
		if (statement.query.empty())
			throw std::runtime_error("Prepare called with empty query.");

		LOG(statement.query);
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLExecute(HSTMT statement_handle)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		statement.sendRequest();
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLExecDirect(HSTMT statement_handle,
			  SQLCHAR * statement_text, SQLINTEGER statement_text_size)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		if (!statement.query.empty())
			throw std::runtime_error("ExecDirect called, but statement query is not empty.");

		statement.query = stringFromSQLChar(statement_text, statement_text_size);
		if (statement.query.empty())
			throw std::runtime_error("ExecDirect called with empty query.");

		LOG(statement.query);
		statement.sendRequest();
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLNumResultCols(HSTMT statement_handle,
				 SQLSMALLINT * column_count)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		*column_count = statement.result.getNumColumns();
		LOG(*column_count);
		return SQL_SUCCESS;
 	});
}


RETCODE SQL_API
SQLColAttribute(HSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
	SQLPOINTER out_string_value, SQLSMALLINT out_string_value_max_size, SQLSMALLINT * out_string_value_size,
	SQLLEN * out_num_value)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement) -> RETCODE
	{
		if (column_number < 1 || column_number > statement.result.getNumColumns())
			throw std::runtime_error("Column number is out of range.");

		size_t column_idx = column_number - 1;

		SQLLEN num_value = 0;
		std::string str_value;

		const ColumnInfo & column_info = statement.result.getColumnInfo(column_idx);
		const TypeInfo & type_info = statement.connection.environment.types_info.at(column_info.type_without_parameters);

		switch (field_identifier)
		{
			case SQL_DESC_AUTO_UNIQUE_VALUE:
				num_value = SQL_FALSE;
				break;
			case SQL_DESC_BASE_COLUMN_NAME:
				break;
			case SQL_DESC_BASE_TABLE_NAME:
				break;
			case SQL_DESC_CASE_SENSITIVE:
				num_value = SQL_TRUE;
				break;
			case SQL_DESC_CATALOG_NAME:
				break;
			case SQL_DESC_CONCISE_TYPE:
				num_value = type_info.sql_type;
				break;
			case SQL_DESC_COUNT:
				num_value = statement.result.getNumColumns();
				break;
			case SQL_DESC_DISPLAY_SIZE:
				num_value = column_info.display_size;
				break;
			case SQL_DESC_FIXED_PREC_SCALE:
				break;
			case SQL_DESC_LABEL:
				str_value = column_info.name;
				break;
			case SQL_DESC_LENGTH:
				break;
			case SQL_DESC_LITERAL_PREFIX:
				break;
			case SQL_DESC_LITERAL_SUFFIX:
				break;
			case SQL_DESC_LOCAL_TYPE_NAME:
				break;
			case SQL_DESC_NAME:
				str_value = column_info.name;
				break;
			case SQL_DESC_NULLABLE:
				num_value = SQL_FALSE;
				break;
			case SQL_DESC_NUM_PREC_RADIX:
				break;
			case SQL_DESC_OCTET_LENGTH:
				break;
			case SQL_DESC_PRECISION:
				break;
			case SQL_DESC_SCALE:
				break;
			case SQL_DESC_SCHEMA_NAME:
				break;
			case SQL_DESC_SEARCHABLE:
				num_value = SQL_SEARCHABLE;
				break;
			case SQL_DESC_TABLE_NAME:
				break;
			case SQL_DESC_TYPE:
				num_value = type_info.sql_type;
				break;
			case SQL_DESC_TYPE_NAME:
				str_value = type_info.sql_type_name;
				break;
			case SQL_DESC_UNNAMED:
				num_value = SQL_NAMED;
				break;
			case SQL_DESC_UNSIGNED:
				num_value = type_info.is_unsigned;
				break;
			case SQL_DESC_UPDATABLE:
				num_value = SQL_FALSE;
				break;
			default:
				return SQL_ERROR;
		}

		if (out_num_value)
			*out_num_value = num_value;

		return fillOutputString(str_value, out_string_value, out_string_value_max_size, out_string_value_size);
	});
}


RETCODE SQL_API
SQLDescribeCol(HSTMT statement_handle,
			   SQLUSMALLINT column_number,
			   SQLCHAR * out_column_name, SQLSMALLINT out_column_name_max_size, SQLSMALLINT * out_column_name_size,
			   SQLSMALLINT * out_type, SQLULEN * out_column_size, SQLSMALLINT * out_decimal_digits, SQLSMALLINT * out_is_nullable)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		if (column_number < 1 || column_number > statement.result.getNumColumns())
			throw std::runtime_error("Column number is out of range.");

		size_t column_idx = column_number - 1;

		const ColumnInfo & column_info = statement.result.getColumnInfo(column_idx);
		const TypeInfo & type_info = statement.connection.environment.types_info.at(column_info.type_without_parameters);

		if (out_type)
			*out_type = type_info.sql_type;
		if (out_column_size)
			*out_column_size = type_info.column_size;
		if (out_decimal_digits)
			*out_decimal_digits = 0;
		if (out_is_nullable)
			*out_is_nullable = SQL_NO_NULLS;

		return fillOutputString(column_info.name, out_column_name, out_column_name_max_size, out_column_name_size);;
	});
}


RETCODE SQL_API
impl_SQLGetData(HSTMT statement_handle,
		   SQLUSMALLINT column_or_param_number, SQLSMALLINT target_type,
		   PTR out_value, SQLLEN out_value_max_size,
		   SQLLEN * out_value_size_or_indicator)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		if (column_or_param_number < 1 || column_or_param_number > statement.result.getNumColumns())
			throw std::runtime_error("Column number is out of range.");

		size_t column_idx = column_or_param_number - 1;

		LOG("column: " << column_idx << ", target_type: " << target_type << ", out_value_max_size: " << out_value_max_size);

		const Field & field = statement.current_row.data[column_idx];

		switch (target_type)
		{
			case SQL_ARD_TYPE:
			case SQL_C_DEFAULT:
				throw std::runtime_error("Unsupported type requested.");

			case SQL_C_CHAR:
			case SQL_C_BINARY:
				return fillOutputString(field.data.data(), field.data.size(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_WCHAR:
			{
				std::string converted;

				converted.resize(field.data.size() * 2 + 1, '\xFF');
				converted[field.data.size() * 2] = '\0';
				for (size_t i = 0, size = field.data.size(); i < size; ++i)
					converted[i * 2] = field.data[i];

				return fillOutputString(converted.data(), converted.size(), out_value, out_value_max_size, out_value_size_or_indicator);
			}

			case SQL_C_TINYINT:
			case SQL_C_STINYINT:
				return fillOutputNumber<int8_t>(field.getInt(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_UTINYINT:
			case SQL_C_BIT:
				return fillOutputNumber<uint8_t>(field.getUInt(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_SHORT:
			case SQL_C_SSHORT:
				return fillOutputNumber<int16_t>(field.getInt(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_USHORT:
				return fillOutputNumber<uint16_t>(field.getUInt(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_LONG:
			case SQL_C_SLONG:
				return fillOutputNumber<int32_t>(field.getInt(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_ULONG:
				return fillOutputNumber<uint32_t>(field.getUInt(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_SBIGINT:
				return fillOutputNumber<int64_t>(field.getInt(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_UBIGINT:
				return fillOutputNumber<uint64_t>(field.getUInt(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_FLOAT:
				return fillOutputNumber<float>(field.getFloat(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_DOUBLE:
				return fillOutputNumber<double>(field.getDouble(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_DATE:
			case SQL_C_TYPE_DATE:
				return fillOutputNumber<SQL_DATE_STRUCT>(field.getDate(), out_value, out_value_max_size, out_value_size_or_indicator);

			case SQL_C_TIMESTAMP:
			case SQL_C_TYPE_TIMESTAMP:
				return fillOutputNumber<SQL_TIMESTAMP_STRUCT>(field.getDateTime(), out_value, out_value_max_size, out_value_size_or_indicator);

			default:
				throw std::runtime_error("Unknown type requested.");
		}
	});
}


RETCODE
impl_SQLFetch(HSTMT statement_handle)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement) -> RETCODE
	{
		if (!statement.fetchRow())
			return SQL_NO_DATA;

		auto res = SQL_SUCCESS;

		for (auto & col_num_binding : statement.bindings)
		{
			auto code = impl_SQLGetData(statement_handle, col_num_binding.first, col_num_binding.second.target_type,
				col_num_binding.second.out_value, col_num_binding.second.out_value_max_size, col_num_binding.second.out_value_size_or_indicator);

			if (code == SQL_SUCCESS_WITH_INFO)
				res = code;
			else if (code != SQL_SUCCESS)
				return code;
		}

		return res;
	});
}


RETCODE SQL_API
SQLFetch(HSTMT statement_handle)
{
	return impl_SQLFetch(statement_handle);
}


RETCODE SQL_API
SQLFetchScroll(HSTMT statement_handle, SQLSMALLINT orientation, SQLLEN offset)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement) -> RETCODE
	{
		if (orientation != SQL_FETCH_NEXT)
			throw std::runtime_error("Fetch type out of range");	/// TODO sqlstate = HY106

		return impl_SQLFetch(statement_handle);
	});
}


RETCODE SQL_API
SQLGetData(HSTMT statement_handle,
		   SQLUSMALLINT column_or_param_number, SQLSMALLINT target_type,
		   PTR out_value, SQLLEN out_value_max_size,
		   SQLLEN * out_value_size_or_indicator)
{
	return impl_SQLGetData(statement_handle,
		column_or_param_number, target_type,
		out_value, out_value_max_size,
		out_value_size_or_indicator);
}


RETCODE SQL_API
SQLBindCol(HSTMT statement_handle,
		   SQLUSMALLINT column_number, SQLSMALLINT target_type,
		   PTR out_value, SQLLEN out_value_max_size, SQLLEN * out_value_size_or_indicator)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		if (column_number < 1 || column_number > statement.result.getNumColumns())
			throw std::runtime_error("Column number is out of range.");

		statement.bindings[column_number] = Binding
		{
			.target_type = target_type,
			.out_value = out_value,
			.out_value_max_size = out_value_max_size,
			.out_value_size_or_indicator = out_value_size_or_indicator
		};

		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLRowCount(HSTMT statement_handle,
			SQLLEN * out_row_count)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		if (out_row_count)
			*out_row_count = statement.result.getNumRows();
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLMoreResults(HSTMT hstmt)
{
	LOG(__FUNCTION__);

	return SQL_NO_DATA;
}


RETCODE SQL_API
SQLDisconnect(HDBC connection_handle)
{
	LOG(__FUNCTION__);

	return doWith<Connection>(connection_handle, [&](Connection & connection)
	{
		connection.session.reset();
		return SQL_SUCCESS;
	});
}


RETCODE
impl_SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle,
    SQLSMALLINT record_number,
	SQLCHAR * out_sqlstate,
	SQLINTEGER * out_native_error_code,
	SQLCHAR * out_mesage, SQLSMALLINT out_message_max_size, SQLSMALLINT * out_message_size)
{
	LOG(__FUNCTION__);

	LOG("handle_type: " << handle_type << ", record_number: " << record_number << ", out_message_max_size: " << out_message_max_size);

	if (nullptr == handle)
		return SQL_INVALID_HANDLE;

	if (record_number <= 0 || out_message_max_size < 0)
		return SQL_ERROR;

	if (record_number > 1)
		return SQL_NO_DATA;

	DiagnosticRecord * diagnostic_record = nullptr;
	switch (handle_type)
	{
		case SQL_HANDLE_ENV:
			diagnostic_record = &reinterpret_cast<Environment *>(handle)->diagnostic_record;
			break;
		case SQL_HANDLE_DBC:
			diagnostic_record = &reinterpret_cast<Connection *>(handle)->diagnostic_record;
			break;
		case SQL_HANDLE_STMT:
			diagnostic_record = &reinterpret_cast<Statement *>(handle)->diagnostic_record;
			break;
		default:
			return SQL_ERROR;
	}

	if (diagnostic_record->native_error_code == 0)
		return SQL_NO_DATA;

	/// Пятибуквенный SQLSTATE и завершающий ноль.
	if (out_sqlstate)
		strncpy(reinterpret_cast<char *>(out_sqlstate), diagnostic_record->sql_state.data(), 6);

	if (out_native_error_code)
		*out_native_error_code = diagnostic_record->native_error_code;

	return fillOutputString(diagnostic_record->message, out_mesage, out_message_max_size, out_message_size);
}


RETCODE SQL_API
SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle,
    SQLSMALLINT record_number,
	SQLCHAR * out_sqlstate,
	SQLINTEGER * out_native_error_code,
	SQLCHAR * out_mesage, SQLSMALLINT out_message_max_size, SQLSMALLINT * out_message_size)
{
	return impl_SQLGetDiagRec(handle_type, handle, record_number, out_sqlstate, out_native_error_code, out_mesage, out_message_max_size, out_message_size);
}


RETCODE SQL_API
SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle,
    SQLSMALLINT record_number,
	SQLSMALLINT field_id,
	SQLPOINTER out_mesage, SQLSMALLINT out_message_max_size, SQLSMALLINT * out_message_size)
{
	LOG(__FUNCTION__);

	return impl_SQLGetDiagRec(
		handle_type,
		handle,
		record_number,
		nullptr,
		nullptr,
		reinterpret_cast<SQLCHAR *>(out_mesage),
		out_message_max_size,
		out_message_size);
}


RETCODE SQL_API
SQLTables(HSTMT statement_handle,
		  SQLCHAR * catalog_name, SQLSMALLINT catalog_name_length,
		  SQLCHAR * schema_name, SQLSMALLINT schema_name_length,
		  SQLCHAR * table_name, SQLSMALLINT table_name_length,
		  SQLCHAR * table_type, SQLSMALLINT table_type_length)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		std::stringstream query;

		query << "SELECT"
				" database AS TABLE_CAT"
				", '' AS TABLE_SCHEM"
				", name AS TABLE_NAME"
				", 'TABLE' AS TABLE_TYPE"
				", '' AS REMARKS"
			" FROM system.tables"
			" WHERE 1";

		if (catalog_name)
			query << " AND TABLE_CAT LIKE '" << stringFromSQLChar(catalog_name, catalog_name_length) << "'";
		if (schema_name)
			query << " AND TABLE_SCHEM LIKE '" << stringFromSQLChar(schema_name, schema_name_length) << "'";
		if (table_name)
			query << " AND TABLE_NAME LIKE '" << stringFromSQLChar(table_name, table_name_length) << "'";
	/*	if (table_type)
			query << " AND TABLE_TYPE = '" << stringFromSQLChar(table_type, table_type_length) << "'";*/

		query << " ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME";

		statement.query = query.str();
		statement.sendRequest();
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLColumns(HSTMT statement_handle,
		   SQLCHAR * catalog_name, SQLSMALLINT catalog_name_length,
		   SQLCHAR * schema_name, SQLSMALLINT schema_name_length,
		   SQLCHAR * table_name, SQLSMALLINT table_name_length,
		   SQLCHAR * column_name, SQLSMALLINT column_name_length)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		std::stringstream query;

		query << "SELECT"
				" database AS TABLE_CAT"
				", '' AS TABLE_SCHEM"
				", table AS TABLE_NAME"
				", name AS COLUMN_NAME"
				", 0 AS DATA_TYPE"	/// TODO
				", 0 AS TYPE_NAME"
				", 0 AS COLUMN_SIZE"
				", 0 AS BUFFER_LENGTH"
				", 0 AS DECIMAL_DIGITS"
				", 0 AS NUM_PREC_RADIX"
				", 0 AS NULLABLE"
				", 0 AS REMARKS"
				", 0 AS COLUMN_DEF"
				", 0 AS SQL_DATA_TYPE "
				", 0 AS SQL_DATETIME_SUB"
				", 0 AS CHAR_OCTET_LENGTH"
				", 0 AS ORDINAL_POSITION"
				", 0 AS IS_NULLABLE"
			" FROM system.columns"
			" WHERE 1";

		if (catalog_name)
			query << " AND TABLE_CAT LIKE '" << stringFromSQLChar(catalog_name, catalog_name_length) << "'";
		if (schema_name)
			query << " AND TABLE_SCHEM LIKE '" << stringFromSQLChar(schema_name, schema_name_length) << "'";
		if (table_name)
			query << " AND TABLE_NAME LIKE '" << stringFromSQLChar(table_name, table_name_length) << "'";
		if (column_name)
			query << " AND COLUMN_NAME LIKE '" << stringFromSQLChar(column_name, column_name_length) << "'";

		query << " ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";

		statement.query = query.str();
		statement.sendRequest();
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLGetTypeInfo(HSTMT statement_handle,
			   SQLSMALLINT type)
{
	LOG(__FUNCTION__);
	LOG("type = " << type);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		std::stringstream query;
		query << "SELECT * FROM (";

		bool first = true;

		auto add_query_for_type = [&](const std::string & name, const TypeInfo & info) mutable
		{
			if (type != SQL_ALL_TYPES && type != info.sql_type)
				return;

			if (!first)
				query << " UNION ALL ";
			first = false;

			query << "SELECT"
				" '" << info.sql_type_name << "' AS TYPE_NAME"
				", toInt16(" << info.sql_type << ") AS DATA_TYPE"
				", toInt32(" << info.column_size << ") AS COLUMN_SIZE"
				", '' AS LITERAL_PREFIX"
				", '' AS LITERAL_SUFFIX"
				", '' AS CREATE_PARAMS"	/// TODO
				", toInt16(" << SQL_NO_NULLS << ") AS NULLABLE"
				", toInt16(" << SQL_TRUE << ") AS CASE_SENSITIVE"
				", toInt16(" << SQL_SEARCHABLE << ") AS SEARCHABLE"
				", toInt16(" << info.is_unsigned << ") AS UNSIGNED_ATTRIBUTE"
				", toInt16(" << SQL_FALSE << ") AS FIXED_PREC_SCALE"
				", toInt16(" << SQL_FALSE << ") AS AUTO_UNIQUE_VALUE"
				", TYPE_NAME AS LOCAL_TYPE_NAME"
				", toInt16(0) AS MINIMUM_SCALE"
				", toInt16(0) AS MAXIMUM_SCALE"
				", DATA_TYPE AS SQL_DATA_TYPE"
				", toInt16(0) AS SQL_DATETIME_SUB"
				", toInt32(10) AS NUM_PREC_RADIX"	/// TODO
				", toInt16(0) AS INTERVAL_PRECISION"
				;
		};

		for (const auto & name_info : statement.connection.environment.types_info)
		{
			add_query_for_type(name_info.first, name_info.second);
		}

		{
			auto info = statement.connection.environment.types_info.at("Date");
			info.sql_type = SQL_DATE;
			add_query_for_type("Date", info);
		}

		{
			auto info = statement.connection.environment.types_info.at("DateTime");
			info.sql_type = SQL_TIMESTAMP;
			add_query_for_type("DateTime", info);
		}

		query << ") ORDER BY DATA_TYPE";

		if (first)
			query.str("SELECT 1 WHERE 0");

		statement.query = query.str();
		statement.sendRequest();
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLNumParams(HSTMT statement_handle,
			 SQLSMALLINT * out_params_count)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement)
	{
		*out_params_count = 0;
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLNativeSql(HDBC connection_handle,
			 SQLCHAR * query, SQLINTEGER query_length,
			 SQLCHAR * out_query, SQLINTEGER out_query_max_length, SQLINTEGER * out_query_length)
{
	LOG(__FUNCTION__);

	return doWith<Connection>(connection_handle, [&](Connection & connection)
	{
		std::string query_str = stringFromSQLChar(query, query_length);
		return fillOutputString(query_str, out_query, out_query_max_length, out_query_length);
	});
}


RETCODE SQL_API
SQLCloseCursor(
	HSTMT statement_handle)
{
	LOG(__FUNCTION__);

	return doWith<Statement>(statement_handle, [&](Statement & statement) -> RETCODE
	{
		statement.reset();
		return SQL_SUCCESS;
	});
}


RETCODE SQL_API
SQLBrowseConnect(HDBC connection_handle,
				 SQLCHAR *szConnStrIn,
				 SQLSMALLINT cbConnStrIn,
				 SQLCHAR *szConnStrOut,
				 SQLSMALLINT cbConnStrOutMax,
				 SQLSMALLINT *pcbConnStrOut)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


/// Не реализовано.


RETCODE SQL_API
SQLCancel(HSTMT StatementHandle)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLDataSources(HENV EnvironmentHandle,
			   SQLUSMALLINT Direction, SQLCHAR *ServerName,
			   SQLSMALLINT BufferLength1, SQLSMALLINT *NameLength1,
			   SQLCHAR *Description, SQLSMALLINT BufferLength2,
			   SQLSMALLINT *NameLength2)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLGetCursorName(HSTMT StatementHandle,
				 SQLCHAR *CursorName, SQLSMALLINT BufferLength,
				 SQLSMALLINT *NameLength)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


/*
/// Эта функция может быть реализована в driver manager-е.
RETCODE SQL_API
SQLGetFunctions(HDBC ConnectionHandle,
				SQLUSMALLINT FunctionId, SQLUSMALLINT *Supported)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}*/


RETCODE SQL_API
SQLParamData(HSTMT StatementHandle,
			 PTR *Value)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLPutData(HSTMT StatementHandle,
		   PTR Data, SQLLEN StrLen_or_Ind)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetCursorName(HSTMT StatementHandle,
				 SQLCHAR *CursorName, SQLSMALLINT NameLength)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetParam(HSTMT StatementHandle,
			SQLUSMALLINT ParameterNumber, SQLSMALLINT ValueType,
			SQLSMALLINT ParameterType, SQLULEN LengthPrecision,
			SQLSMALLINT ParameterScale, PTR ParameterValue,
			SQLLEN *StrLen_or_Ind)
{
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLStatistics(HSTMT StatementHandle,
			  SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
			  SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
			  SQLCHAR *TableName, SQLSMALLINT NameLength3,
			  SQLUSMALLINT Unique, SQLUSMALLINT Reserved)
{
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetPos(HSTMT hstmt,
		  SQLSETPOSIROW irow,
		  SQLUSMALLINT fOption,
		  SQLUSMALLINT fLock)
{
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
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
	LOG(__FUNCTION__);
	return SQL_ERROR;
}

/*
RETCODE SQL_API
SQLBulkOperations(
     SQLHSTMT       StatementHandle,
     SQLUSMALLINT   Operation)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}*/


RETCODE SQL_API
SQLCancelHandle(
      SQLSMALLINT  HandleType,
      SQLHANDLE    Handle)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLCompleteAsync(
      SQLSMALLINT HandleType,
      SQLHANDLE   Handle,
      RETCODE *   AsyncRetCodePtr)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLCopyDesc(
     SQLHDESC     SourceDescHandle,
     SQLHDESC     TargetDescHandle)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLEndTran(
     SQLSMALLINT   HandleType,
     SQLHANDLE     Handle,
     SQLSMALLINT   CompletionType)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLError(SQLHENV hDrvEnv, SQLHDBC hDrvDbc, SQLHSTMT hDrvStmt,
    SQLCHAR *szSqlState, SQLINTEGER *pfNativeError, SQLCHAR *szErrorMsg,
    SQLSMALLINT nErrorMsgMax, SQLSMALLINT *pcbErrorMsg)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLGetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
    SQLSMALLINT FieldIdentifier, SQLPOINTER Value, SQLINTEGER BufferLength,
    SQLINTEGER *StringLength)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLGetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
    SQLCHAR *Name, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength,
    SQLSMALLINT *Type, SQLSMALLINT *SubType, SQLLEN *Length,
    SQLSMALLINT *Precision, SQLSMALLINT *Scale, SQLSMALLINT *Nullable)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLParamOptions(SQLHSTMT hDrvStmt, SQLULEN nRow, SQLULEN *pnRow)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
    SQLSMALLINT FieldIdentifier, SQLPOINTER Value, SQLINTEGER BufferLength)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetDescRec(SQLHDESC hDescriptorHandle, SQLSMALLINT nRecordNumber,
    SQLSMALLINT nType, SQLSMALLINT nSubType, SQLLEN nLength,
    SQLSMALLINT nPrecision, SQLSMALLINT nScale, SQLPOINTER pData,
    SQLLEN *pnStringLength, SQLLEN *pnIndicator)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLSetScrollOptions(
	SQLHSTMT hDrvStmt, SQLUSMALLINT fConcurrency, SQLLEN crowKeyset,
    SQLUSMALLINT crowRowset)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


RETCODE SQL_API
SQLTransact(SQLHENV hDrvEnv, SQLHDBC hDrvDbc, UWORD nType)
{
	LOG(__FUNCTION__);
	return SQL_ERROR;
}


}
