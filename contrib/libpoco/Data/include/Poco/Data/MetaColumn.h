//
// MetaColumn.h
//
// $Id: //poco/Main/Data/include/Poco/Data/MetaColumn.h#5 $
//
// Library: Data
// Package: DataCore
// Module:  MetaColumn
//
// Definition of the MetaColumn class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_MetaColumn_INCLUDED
#define Data_MetaColumn_INCLUDED


#include "Poco/Data/Data.h"
#include <cstddef>


namespace Poco {
namespace Data {


class Data_API MetaColumn
	/// MetaColumn class contains column metadata information.
{
public:
	enum ColumnDataType
	{
		FDT_BOOL,
		FDT_INT8,
		FDT_UINT8,
		FDT_INT16,
		FDT_UINT16,
		FDT_INT32,
		FDT_UINT32,
		FDT_INT64,
		FDT_UINT64,
		FDT_FLOAT,
		FDT_DOUBLE,
		FDT_STRING,
		FDT_WSTRING,
		FDT_BLOB,
		FDT_CLOB,
		FDT_DATE,
		FDT_TIME,
		FDT_TIMESTAMP,
		FDT_UNKNOWN
	};

	MetaColumn();
		/// Creates the MetaColumn.

	explicit MetaColumn(std::size_t position,
		const std::string& name = "",
		ColumnDataType type = FDT_UNKNOWN,
		std::size_t length = 0,
		std::size_t precision = 0,
		bool nullable = false);
		/// Creates the MetaColumn.

	virtual ~MetaColumn();
		/// Destroys the MetaColumn.

	const std::string& name() const;
		/// Returns column name.

	std::size_t length() const;
		/// Returns column maximum length.

	std::size_t precision() const;
		/// Returns column precision.
		/// Valid for floating point fields only
		/// (zero for other data types).

	std::size_t position() const;
		/// Returns column position.

	ColumnDataType type() const;
		/// Returns column type.

	bool isNullable() const;
		/// Returns true if column allows null values, false otherwise.

protected:
	void setName(const std::string& name);
		/// Sets the column name.

	void setLength(std::size_t length);
		/// Sets the column length.

	void setPrecision(std::size_t precision);
		/// Sets the column precision.

	void setType(ColumnDataType type);
		/// Sets the column data type.

	void setNullable(bool nullable);
		/// Sets the column nullability.

private:
	std::string     _name;
	std::size_t     _length;
	std::size_t     _precision;
	std::size_t     _position;
	ColumnDataType  _type;
	bool            _nullable;
};


///
/// inlines
///
inline const std::string& MetaColumn::name() const
{
	return _name;
}


inline std::size_t MetaColumn::length() const
{
	return _length;
}


inline std::size_t MetaColumn::precision() const
{
	return _precision;
}


inline std::size_t MetaColumn::position() const
{
	return _position;
}


inline MetaColumn::ColumnDataType MetaColumn::type() const
{
	return _type;
}


inline bool MetaColumn::isNullable() const
{
	return _nullable;
}


inline void MetaColumn::setName(const std::string& name)
{
	_name = name;
}


inline void MetaColumn::setLength(std::size_t length)
{
	_length = length;
}


inline void MetaColumn::setPrecision(std::size_t precision)
{
	_precision = precision;
}


inline void MetaColumn::setType(ColumnDataType type)
{
	_type = type;
}


inline void MetaColumn::setNullable(bool nullable)
{
	_nullable = nullable;
}


} } // namespace Poco::Data


#endif // Data_MetaColumn_INCLUDED
