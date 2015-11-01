#pragma once

#include <sqlext.h>
#include <stdio.h>

#include <map>
#include <stdexcept>

/*#include <Poco/UTF8Encoding.h>
#include <Poco/UTF16Encoding.h>
#include <Poco/TextConverter.h>*/

#include "DiagnosticRecord.h"


struct TypeInfo
{
	std::string sql_type_name;
	bool is_unsigned;
	SQLSMALLINT sql_type;
	size_t column_size;
};


struct Environment
{
	Environment()
	{
		std::string stderr_path = "/tmp/clickhouse-odbc-stderr";
		if (!freopen(stderr_path.c_str(), "a+", stderr))
			throw std::logic_error("Cannot freopen stderr.");
	}

	const std::map<std::string, TypeInfo> types_info =
	{
		{"UInt8", 		{ .sql_type_name = "TINYINT", 	.is_unsigned = true, 	.sql_type = SQL_TINYINT, 		.column_size = 3, }},
		{"UInt16", 		{ .sql_type_name = "SMALLINT", 	.is_unsigned = true,  	.sql_type = SQL_SMALLINT, 		.column_size = 5, }},
		{"UInt32", 		{ .sql_type_name = "INT", 		.is_unsigned = true,  	.sql_type = SQL_INTEGER, 		.column_size = 10,}},
		{"UInt64", 		{ .sql_type_name = "BIGINT", 	.is_unsigned = true,  	.sql_type = SQL_BIGINT, 		.column_size = 19,}},
		{"Int8",		{ .sql_type_name = "TINYINT", 	.is_unsigned = false,  	.sql_type = SQL_TINYINT, 		.column_size = 3, }},
		{"Int16", 		{ .sql_type_name = "SMALLINT", 	.is_unsigned = false, 	.sql_type = SQL_SMALLINT, 		.column_size = 5, }},
		{"Int32", 		{ .sql_type_name = "INT", 		.is_unsigned = false, 	.sql_type = SQL_INTEGER, 		.column_size = 10,}},
		{"Int64", 		{ .sql_type_name = "BIGINT", 	.is_unsigned = false,  	.sql_type = SQL_BIGINT, 		.column_size = 20,}},
		{"Float32", 	{ .sql_type_name = "REAL", 		.is_unsigned = false,  	.sql_type = SQL_REAL, 			.column_size = 7, }},
		{"Float64", 	{ .sql_type_name = "DOUBLE", 	.is_unsigned = false,  	.sql_type = SQL_DOUBLE, 		.column_size = 15,}},
		{"String", 		{ .sql_type_name = "TEXT", 		.is_unsigned = true,  	.sql_type = SQL_VARCHAR, 	.column_size = 0xFFFFFF,}},
		{"FixedString", { .sql_type_name = "TEXT", 		.is_unsigned = true,  	.sql_type = SQL_VARCHAR, 	.column_size = 0xFFFFFF,}},
		{"Date", 		{ .sql_type_name = "DATE", 		.is_unsigned = true,  	.sql_type = SQL_TYPE_DATE, 		.column_size = 10, }},
		{"DateTime", 	{ .sql_type_name = "TIMESTAMP",	.is_unsigned = true, 	.sql_type = SQL_TYPE_TIMESTAMP, .column_size = 19, }},
		{"Array", 		{ .sql_type_name = "TEXT", 		.is_unsigned = true,  	.sql_type = SQL_VARCHAR, 	.column_size = 0xFFFFFF,}},
	};

/*	Poco::UTF8Encoding utf8;
	Poco::UTF16Encoding utf16;
	Poco::TextConverter converter_utf8_to_utf16 {utf8, utf16};*/

	int odbc_version = SQL_OV_ODBC3;
	DiagnosticRecord diagnostic_record;
};
