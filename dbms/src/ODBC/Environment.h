#pragma once

#include <sqlext.h>
#include <stdio.h>

#include <map>
#include <stdexcept>

/*#include <Poco/UTF8Encoding.h>
#include <Poco/UTF16Encoding.h>
#include <Poco/TextConverter.h>*/

#include "DiagnosticRecord.h"


struct Environment
{
	Environment()
	{
		std::string stderr_path = "/tmp/clickhouse-odbc-stderr";
		if (!freopen(stderr_path.c_str(), "a+", stderr))
			throw std::logic_error("Cannot freopen stderr.");
	}

	struct TypeInfo
	{
		std::string sql_type_name;
		bool is_unsigned;
	};

	const std::map<std::string, TypeInfo> types_info =
	{
		{"UInt8", 		{ .sql_type_name = "TINYINT", 	.is_unsigned = true, }},
		{"UInt16", 		{ .sql_type_name = "SMALLINT", 	.is_unsigned = true,  }},
		{"UInt32", 		{ .sql_type_name = "INT", 		.is_unsigned = true,  }},
		{"UInt64", 		{ .sql_type_name = "BIGINT", 	.is_unsigned = true,  }},
		{"Int8",		{ .sql_type_name = "TINYINT", 	.is_unsigned = false,  }},
		{"Int16", 		{ .sql_type_name = "SMALLINT", 	.is_unsigned = false,  }},
		{"Int32", 		{ .sql_type_name = "INT", 		.is_unsigned = false,  }},
		{"Int64", 		{ .sql_type_name = "BIGINT", 	.is_unsigned = false,  }},
		{"Float32", 	{ .sql_type_name = "FLOAT", 	.is_unsigned = false,  }},
		{"Float64", 	{ .sql_type_name = "DOUBLE", 	.is_unsigned = false,  }},
		{"String", 		{ .sql_type_name = "TEXT", 		.is_unsigned = true,  }},
		{"FixedString", { .sql_type_name = "TEXT", 		.is_unsigned = true,  }},
		{"Date", 		{ .sql_type_name = "DATE", 		.is_unsigned = true,  }},
		{"DateTime", 	{ .sql_type_name = "DATETIME", 	.is_unsigned = true,  }},
		{"Array", 		{ .sql_type_name = "TEXT", 		.is_unsigned = true,  }},
	};

/*	Poco::UTF8Encoding utf8;
	Poco::UTF16Encoding utf16;
	Poco::TextConverter converter_utf8_to_utf16 {utf8, utf16};*/

	int odbc_version = SQL_OV_ODBC3;
	DiagnosticRecord diagnostic_record;
};
