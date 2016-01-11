//
// MySQLException.cpp
//
// $Id: //poco/1.4/Data/MySQL/src/ResultMetadata.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  ResultMetadata
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/ResultMetadata.h"
#include "Poco/Data/MySQL/MySQLException.h"
#include <cstring>

namespace
{
	class ResultMetadataHandle
		/// Simple exception-safe wrapper
	{
	public:

		explicit ResultMetadataHandle(MYSQL_STMT* stmt)
		{
			h = mysql_stmt_result_metadata(stmt);
		}

		~ResultMetadataHandle()
		{
			if (h)
			{
				mysql_free_result(h);
			}
		}

		operator MYSQL_RES* ()
		{
			return h;
		}

	private:

		MYSQL_RES* h;
	};

	std::size_t fieldSize(const MYSQL_FIELD& field)
		/// Convert field MySQL-type and field MySQL-length to actual field length
	{
		switch (field.type)
		{
		case MYSQL_TYPE_TINY:     return sizeof(char);
		case MYSQL_TYPE_SHORT:    return sizeof(short);
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:     return sizeof(Poco::Int32);
		case MYSQL_TYPE_FLOAT:    return sizeof(float);
		case MYSQL_TYPE_DOUBLE:   return sizeof(double);
		case MYSQL_TYPE_LONGLONG: return sizeof(Poco::Int64);

		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_DATETIME:
			return sizeof(MYSQL_TIME);

		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
			return field.length;

		default:
			throw Poco::Data::MySQL::StatementException("unknown field type");
		}
	}	

	Poco::Data::MetaColumn::ColumnDataType fieldType(const MYSQL_FIELD& field)
		/// Convert field MySQL-type to Poco-type	
	{
		bool unsig = ((field.flags & UNSIGNED_FLAG) == UNSIGNED_FLAG);

		switch (field.type)
		{
		case MYSQL_TYPE_TINY:     
			if (unsig) return Poco::Data::MetaColumn::FDT_UINT8;
			return Poco::Data::MetaColumn::FDT_INT8;

		case MYSQL_TYPE_SHORT:
			if (unsig) return Poco::Data::MetaColumn::FDT_UINT16;
			return Poco::Data::MetaColumn::FDT_INT16;

		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:     
			if (unsig) return Poco::Data::MetaColumn::FDT_UINT32;
			return Poco::Data::MetaColumn::FDT_INT32;

		case MYSQL_TYPE_FLOAT:    
			return Poco::Data::MetaColumn::FDT_FLOAT;

		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DOUBLE:   
			return Poco::Data::MetaColumn::FDT_DOUBLE;

		case MYSQL_TYPE_LONGLONG: 
			if (unsig) return Poco::Data::MetaColumn::FDT_UINT64;
			return Poco::Data::MetaColumn::FDT_INT64;
			
		case MYSQL_TYPE_DATE:
			return Poco::Data::MetaColumn::FDT_DATE;
			
		case MYSQL_TYPE_TIME:
			return Poco::Data::MetaColumn::FDT_TIME;
			
		case MYSQL_TYPE_DATETIME:
			return Poco::Data::MetaColumn::FDT_TIMESTAMP;
			
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_VAR_STRING:
			return Poco::Data::MetaColumn::FDT_STRING;

		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
			return Poco::Data::MetaColumn::FDT_BLOB;
		default:
			return Poco::Data::MetaColumn::FDT_UNKNOWN;
		}
	}
} // namespace


namespace Poco {
namespace Data {
namespace MySQL {

void ResultMetadata::reset()
{
	_columns.resize(0);
	_row.resize(0);
	_buffer.resize(0);
	_lengths.resize(0);
	_isNull.resize(0);
}

void ResultMetadata::init(MYSQL_STMT* stmt)
{
	ResultMetadataHandle h(stmt);

	if (!h)
	{
		// all right, it is normal
		// querys such an "INSERT INTO" just does not have result at all
		reset();
		return;
	}

	std::size_t count = mysql_num_fields(h);
	MYSQL_FIELD* fields = mysql_fetch_fields(h);

	std::size_t commonSize = 0;
	_columns.reserve(count);

	{for (std::size_t i = 0; i < count; i++)
	{
		std::size_t size = fieldSize(fields[i]);
		if (size == 0xFFFFFFFF) size = 0;

		_columns.push_back(MetaColumn(
			i,                               // position
			fields[i].name,                  // name
			fieldType(fields[i]),            // type
			size,                            // length
			0,                               // TODO: precision
			!IS_NOT_NULL(fields[i].flags)    // nullable
			));

		commonSize += _columns[i].length();
	}}

	_buffer.resize(commonSize);
	_row.resize(count);
	_lengths.resize(count);
	_isNull.resize(count);

	std::size_t offset = 0;

	for (std::size_t i = 0; i < count; i++)
	{
		std::memset(&_row[i], 0, sizeof(MYSQL_BIND));
		unsigned int len = static_cast<unsigned int>(_columns[i].length());
		_row[i].buffer_type   = fields[i].type;
		_row[i].buffer_length = len;
		_row[i].buffer        = (len > 0) ? (&_buffer[0] + offset) : 0;
		_row[i].length        = &_lengths[i];
		_row[i].is_null       = &_isNull[i];
		_row[i].is_unsigned   = (fields[i].flags & UNSIGNED_FLAG) > 0;
		
		offset += _row[i].buffer_length;
	}
}

std::size_t ResultMetadata::columnsReturned() const
{
	return static_cast<std::size_t>(_columns.size());
}

const MetaColumn& ResultMetadata::metaColumn(std::size_t pos) const
{
	return _columns[pos];
}

MYSQL_BIND* ResultMetadata::row()
{
	return &_row[0];
}

std::size_t ResultMetadata::length(std::size_t pos) const
{
	return _lengths[pos];
}

const unsigned char* ResultMetadata::rawData(std::size_t pos) const 
{
	return reinterpret_cast<const unsigned char*>(_row[pos].buffer);
}

bool ResultMetadata::isNull(std::size_t pos) const 
{
	return (_isNull[pos] != 0);
}

}}} // namespace Poco::Data::MySQL
