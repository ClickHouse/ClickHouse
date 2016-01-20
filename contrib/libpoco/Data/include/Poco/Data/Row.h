//
// Row.h
//
// $Id: //poco/Main/Data/include/Poco/Data/Row.h#1 $
//
// Library: Data
// Package: DataCore
// Module:  Row
//
// Definition of the Row class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Row_INCLUDED
#define Data_Row_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/RowFormatter.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/Tuple.h"
#include "Poco/SharedPtr.h"
#include <vector>
#include <string>
#include <ostream>


namespace Poco {
namespace Data {


class RecordSet;


class Data_API Row
	/// Row class provides a data type for RecordSet iteration purposes.
	/// Dereferencing a RowIterator returns Row.
	/// Rows are sortable. The sortability is maintained at all times (i.e. there
	/// is always at least one column specified as a sorting criteria) .
	/// The default and minimal sorting criteria is the first field (position 0).
	/// The default sorting criteria can be replaced with any other field by 
	/// calling replaceSortField() member function.
	/// Additional fields can be added to sorting criteria, in which case the
	/// field precedence corresponds to addition order (i.e. later added fields
	/// have lower sorting precedence).
	/// These features make Row suitable for use with standard sorted 
	/// containers and algorithms. The main constraint is that all the rows from
	/// a set that is being sorted must have the same sorting criteria (i.e., the same
	/// set of fields must be in sorting criteria in the same order). Since rows don't
	/// know about each other, it is the programmer's responsibility to ensure this
	/// constraint is satisfied.
	/// Field names are a shared pointer to a vector of strings. For efficiency sake,
	/// a constructor taking a shared pointer to names vector argument is provided.
	/// The stream operator is provided for Row data type as a free-standing function.
{
public:
	typedef RowFormatter::NameVec    NameVec;
	typedef RowFormatter::NameVecPtr NameVecPtr;
	typedef RowFormatter::ValueVec   ValueVec;

	enum ComparisonType
	{
		COMPARE_AS_EMPTY,
		COMPARE_AS_INTEGER,
		COMPARE_AS_FLOAT,
		COMPARE_AS_STRING
	};

	typedef Tuple<std::size_t, ComparisonType> SortTuple;
	typedef std::vector<SortTuple>             SortMap;
		/// The type for map holding fields used for sorting criteria.
		/// Fields are added sequentially and have precedence that
		/// corresponds to field adding sequence order (rather than field's 
		/// position in the row).
		/// This requirement rules out use of std::map due to its sorted nature.
	typedef SharedPtr<SortMap> SortMapPtr;

	Row();
		/// Creates the Row.

	Row(NameVecPtr pNames,
		const RowFormatter::Ptr& pFormatter = 0);
		/// Creates the Row.

	Row(NameVecPtr pNames,
		const SortMapPtr& pSortMap,
		const RowFormatter::Ptr& pFormatter = 0);
		/// Creates the Row.

	~Row();
		/// Destroys the Row.

	Poco::Dynamic::Var& get(std::size_t col);
		/// Returns the reference to data value at column location.

	Poco::Dynamic::Var& operator [] (std::size_t col);
		/// Returns the reference to data value at column location.

	Poco::Dynamic::Var& operator [] (const std::string& name);
		/// Returns the reference to data value at named column location.

	template <typename T>
	void append(const std::string& name, const T& val)
		/// Appends the value to the row.
	{
		if (!_pNames) _pNames = new NameVec;
		_values.push_back(val);
		_pNames->push_back(name);
		if (1 == _values.size()) addSortField(0);
	}
	
	template <typename T>
	void set(std::size_t pos, const T& val)
		/// Assigns the value to the row.
	{
		try
		{
			_values.at(pos) = val;
		}catch (std::out_of_range&)
		{
			throw RangeException("Invalid column number.");
		}
	}

	template <typename T>
	void set(const std::string& name, const T& val)
		/// Assigns the value to the row.
	{
		NameVec::iterator it = _pNames->begin();
		NameVec::iterator end = _pNames->end();
		for (int i = 0; it != end; ++it, ++i)
		{
			if (*it == name)
				return set(i, val);
		}

		std::ostringstream os;
		os << "Column with name " << name << " not found.";
		throw NotFoundException(os.str());
	}

	std::size_t fieldCount() const;
		/// Returns the number of fields in this row.

	void reset();
		/// Resets the row by clearing all field names and values.

	void separator(const std::string& sep);
		/// Sets the separator.

	void addSortField(std::size_t pos);
		/// Adds the field used for sorting.

	void addSortField(const std::string& name);
		/// Adds the field used for sorting.

	void removeSortField(std::size_t pos);
		/// Removes the field used for sorting.

	void removeSortField(const std::string& name);
		/// Removes the field used for sorting.

	void replaceSortField(std::size_t oldPos, std::size_t newPos);
		/// Replaces the field used for sorting.

	void replaceSortField(const std::string& oldName, const std::string& newName);
		/// Replaces the field used for sorting.

	void resetSort();
		/// Resets the sorting criteria to field 0 only.

	const std::string& namesToString() const;
		/// Converts the column names to string.

	void formatNames() const;
		/// Fomats the column names.

	const std::string& valuesToString() const;
		/// Converts the row values to string and returns the formated string.

	void formatValues() const;
		/// Fomats the row values.

	bool operator == (const Row& other) const;
		/// Equality operator.

	bool operator != (const Row& other) const;
		/// Inequality operator.

	bool operator < (const Row& other) const;
		/// Less-than operator.

	const NameVecPtr names() const;
		/// Returns the shared pointer to names vector.

	const ValueVec& values() const;
		/// Returns the const reference to values vector.

	void setFormatter(const RowFormatter::Ptr& pFormatter = 0);
		/// Sets the formatter for this row and takes the
		/// shared ownership of it.

	const RowFormatter& getFormatter() const;
		/// Returns the reference to the formatter.

	void setSortMap(const SortMapPtr& pSortMap = 0);
		/// Adds the sorting fields entry and takes the
		/// shared ownership of it.

	const SortMapPtr& getSortMap() const;
		/// Returns the reference to the sorting fields.

private:
	void init(const SortMapPtr& pSortMap, const RowFormatter::Ptr& pFormatter);

	void checkEmpty(std::size_t pos, const Poco::Dynamic::Var& val);
		/// Check if row contains only empty values and throws IllegalStateException
		/// if that is the case.

	ValueVec& values();
		/// Returns the reference to values vector.

	std::size_t getPosition(const std::string& name);
	bool isEqualSize(const Row& other) const;
	bool isEqualType(const Row& other) const;

	NameVecPtr                _pNames;
	ValueVec                  _values;
	SortMapPtr                _pSortMap;
	mutable RowFormatter::Ptr _pFormatter;
	mutable std::string       _nameStr;
	mutable std::string       _valueStr;
};


Data_API std::ostream& operator << (std::ostream &os, const Row& row);


///
/// inlines
///
inline std::size_t Row::fieldCount() const
{
	return static_cast<std::size_t>(_values.size());
}


inline void Row::reset()
{
	_pNames->clear();
	_values.clear();
}


inline const Row::NameVecPtr Row::names() const
{
	return _pNames;
}


inline const Row::ValueVec& Row::values() const
{
	return _values;
}


inline Row::ValueVec& Row::values()
{
	return _values;
}


inline Poco::Dynamic::Var& Row::operator [] (std::size_t col)
{
	return get(col);
}


inline Poco::Dynamic::Var& Row::operator [] (const std::string& name)
{
	return get(getPosition(name));
}


inline const RowFormatter& Row::getFormatter() const
{
	return *_pFormatter;
}


inline const Row::SortMapPtr& Row::getSortMap() const
{
	return _pSortMap;
}


inline const std::string& Row::valuesToString() const
{
	return _pFormatter->formatValues(values(), _valueStr);
}


inline void Row::formatValues() const
{
	return _pFormatter->formatValues(values());
}


} } // namespace Poco::Data


#endif // Data_Row_INCLUDED
