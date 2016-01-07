//
// RowFormatter.h
//
// $Id: //poco/Main/Data/include/Poco/Data/SimpleRowFormatter.h#1 $
//
// Library: Data
// Package: DataCore
// Module:  SimpleRowFormatter
//
// Definition of the RowFormatter class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_SimpleRowFormatter_INCLUDED
#define Data_SimpleRowFormatter_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/RowFormatter.h"


namespace Poco {
namespace Data {


class Data_API SimpleRowFormatter: public RowFormatter
	/// A simple row formatting class.
{
public:
	//typedef RowFormatter::NameVec    NameVec;
	//typedef RowFormatter::NameVecPtr NameVecPtr;
	//typedef RowFormatter::ValueVec   ValueVec;

	static const int DEFAULT_COLUMN_WIDTH = 16;
	static const int DEFAULT_SPACING = 1;

	SimpleRowFormatter(std::streamsize columnWidth = DEFAULT_COLUMN_WIDTH, std::streamsize spacing = DEFAULT_SPACING);
		/// Creates the SimpleRowFormatter and sets the column width to specified value.

	SimpleRowFormatter(const SimpleRowFormatter& other);
		/// Creates the copy of the supplied SimpleRowFormatter.

	SimpleRowFormatter& operator = (const SimpleRowFormatter& row);
		/// Assignment operator.

	~SimpleRowFormatter();
		/// Destroys the SimpleRowFormatter.

	void swap(SimpleRowFormatter& other);
		/// Swaps the row formatter with another one.

	std::string& formatNames(const NameVecPtr pNames, std::string& formattedNames);
		/// Formats the row field names.

	std::string& formatValues(const ValueVec& vals, std::string& formattedValues);
		/// Formats the row values.

	int rowCount() const;
		/// Returns row count.

	void setColumnWidth(std::streamsize width);
		/// Sets the column width.

	std::streamsize getColumnWidth() const;
		/// Returns the column width.
		
	std::streamsize getSpacing() const;
		/// Returns the spacing.

private:
	std::streamsize _colWidth;
	std::streamsize _spacing;
	int             _rowCount;
};


///
/// inlines
///
inline int SimpleRowFormatter::rowCount() const
{
	return _rowCount;
}


inline void SimpleRowFormatter::setColumnWidth(std::streamsize columnWidth)
{
	_colWidth = columnWidth;
}


inline std::streamsize SimpleRowFormatter::getColumnWidth() const
{
	return _colWidth;
}


inline std::streamsize SimpleRowFormatter::getSpacing() const
{
	return _spacing;
}


} } // namespace Poco::Data


namespace std
{
	template<>
	inline void swap<Poco::Data::SimpleRowFormatter>(Poco::Data::SimpleRowFormatter& s1, 
		Poco::Data::SimpleRowFormatter& s2)
		/// Full template specalization of std:::swap for SimpleRowFormatter
	{
		s1.swap(s2);
	}
}


#endif // Data_SimpleRowFormatter_INCLUDED
