//
// Column.h
//
// $Id: //poco/Main/Data/include/Poco/Data/Column.h#5 $
//
// Library: Data
// Package: DataCore
// Module:  Column
//
// Definition of the Column class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Column_INCLUDED
#define Data_Column_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/MetaColumn.h"
#include "Poco/SharedPtr.h"
#include "Poco/RefCountedObject.h"
#include <vector>
#include <list>
#include <deque>


namespace Poco {
namespace Data {


template <class C>
class Column
	/// Column class is column data container.
	/// Data (a pointer to underlying STL container) is assigned to the class 
	/// at construction time. Construction with null pointer is not allowed.
	/// This class owns the data assigned to it and deletes the storage on destruction.
{
public:
	typedef C                                  Container;
	typedef Poco::SharedPtr<C>                 ContainerPtr;
	typedef typename C::const_iterator         Iterator;
	typedef typename C::const_reverse_iterator RIterator;
	typedef typename C::size_type              Size;
	typedef typename C::value_type             Type;

	Column(const MetaColumn& metaColumn, Container* pData): 
		_metaColumn(metaColumn),
		_pData(pData)
		/// Creates the Column.
	{
		if (!_pData)
			throw NullPointerException("Container pointer must point to valid storage.");
	}

	Column(const Column& col): 
		_metaColumn(col._metaColumn), 
		_pData(col._pData)
		/// Creates the Column.
	{
	}

	~Column()
		/// Destroys the Column.
	{
	}

	Column& operator = (const Column& col)
		/// Assignment operator.
	{
		Column tmp(col);
		swap(tmp);
		return *this;
	}

	void swap(Column& other)
		/// Swaps the column with another one.
	{
		using std::swap;
		swap(_metaColumn, other._metaColumn);
		swap(_pData, other._pData);
	}

	Container& data()
		/// Returns reference to contained data.
	{
		return *_pData;
	}

	const Type& value(std::size_t row) const
		/// Returns the field value in specified row.
	{
		try
		{
			return _pData->at(row);
		}
		catch (std::out_of_range& ex)
		{ 
			throw RangeException(ex.what()); 
		}
	}

	const Type& operator [] (std::size_t row) const
		/// Returns the field value in specified row.
	{
		return value(row);
	}

	Size rowCount() const
		/// Returns number of rows.
	{
		return _pData->size();
	}

	void reset()
		/// Clears and shrinks the storage.
	{
		Container().swap(*_pData);
	}

	const std::string& name() const
		/// Returns column name.
	{
		return _metaColumn.name();
	}

	std::size_t length() const
		/// Returns column maximum length.
	{
		return _metaColumn.length();
	}

	std::size_t precision() const
		/// Returns column precision.
		/// Valid for floating point fields only (zero for other data types).
	{
		return _metaColumn.precision();
	}

	std::size_t position() const
		/// Returns column position.
	{
		return _metaColumn.position();
	}

	MetaColumn::ColumnDataType type() const
		/// Returns column type.
	{
		return _metaColumn.type();
	}

	Iterator begin() const
		/// Returns iterator pointing to the beginning of data storage vector.
	{
		return _pData->begin();
	}

	Iterator end() const
		/// Returns iterator pointing to the end of data storage vector.
	{
		return _pData->end();
	}

private:
	Column();

	MetaColumn   _metaColumn;
	ContainerPtr _pData;
};


template <>
class Column<std::vector<bool> >
	/// The std::vector<bool> specialization for the Column class.
	/// 
	/// This specialization is necessary due to the nature of std::vector<bool>.
	/// For details, see the standard library implementation of vector<bool> 
	/// or
	/// S. Meyers: "Effective STL" (Copyright Addison-Wesley 2001),
	/// Item 18: "Avoid using vector<bool>."
	/// 
	/// The workaround employed here is using deque<bool> as an
	/// internal "companion" container kept in sync with the vector<bool>
	/// column data.
{
public:
	typedef std::vector<bool>                 Container;
	typedef Poco::SharedPtr<Container>        ContainerPtr;
	typedef Container::const_iterator         Iterator;
	typedef Container::const_reverse_iterator RIterator;
	typedef Container::size_type              Size;

	Column(const MetaColumn& metaColumn, Container* pData): 
		_metaColumn(metaColumn), 
		_pData(pData)
		/// Creates the Column.
	{
		poco_check_ptr (_pData);
		_deque.assign(_pData->begin(), _pData->end());
	}

	Column(const Column& col): 
		_metaColumn(col._metaColumn), 
		_pData(col._pData)
		/// Creates the Column.
	{
		_deque.assign(_pData->begin(), _pData->end());
	}

	~Column()
		/// Destroys the Column.
	{
	}

	Column& operator = (const Column& col)
		/// Assignment operator.
	{
		Column tmp(col);
		swap(tmp);
		return *this;
	}

	void swap(Column& other)
		/// Swaps the column with another one.
	{
		using std::swap;
		swap(_metaColumn, other._metaColumn);
		swap(_pData, other._pData);
		swap(_deque, other._deque);
	}

	Container& data()
		/// Returns reference to contained data.
	{
		return *_pData;
	}

	const bool& value(std::size_t row) const
		/// Returns the field value in specified row.
	{
		if (_deque.size() < _pData->size())
			_deque.resize(_pData->size());

		try
		{
			return _deque.at(row) = _pData->at(row);
		}
		catch (std::out_of_range& ex)
		{ 
			throw RangeException(ex.what()); 
		}
	}

	const bool& operator [] (std::size_t row) const
		/// Returns the field value in specified row.
	{
		return value(row);
	}

	Size rowCount() const
		/// Returns number of rows.
	{
		return _pData->size();
	}

	void reset()
		/// Clears and shrinks the storage.
	{
		Container().swap(*_pData);
		_deque.clear();
	}

	const std::string& name() const
		/// Returns column name.
	{
		return _metaColumn.name();
	}

	std::size_t length() const
		/// Returns column maximum length.
	{
		return _metaColumn.length();
	}

	std::size_t precision() const
		/// Returns column precision.
		/// Valid for floating point fields only (zero for other data types).
	{
		return _metaColumn.precision();
	}

	std::size_t position() const
		/// Returns column position.
	{
		return _metaColumn.position();
	}

	MetaColumn::ColumnDataType type() const
		/// Returns column type.
	{
		return _metaColumn.type();
	}

	Iterator begin() const
		/// Returns iterator pointing to the beginning of data storage vector.
	{
		return _pData->begin();
	}

	Iterator end() const
		/// Returns iterator pointing to the end of data storage vector.
	{
		return _pData->end();
	}

private:
	Column();

	MetaColumn               _metaColumn;
	ContainerPtr             _pData;
	mutable std::deque<bool> _deque;
};


template <class T>
class Column<std::list<T> >
	/// Column specialization for std::list
{
public:
	typedef std::list<T>                               Container;
	typedef Poco::SharedPtr<Container>                 ContainerPtr;
	typedef typename Container::const_iterator         Iterator;
	typedef typename Container::const_reverse_iterator RIterator;
	typedef typename Container::size_type              Size;

	Column(const MetaColumn& metaColumn, std::list<T>* pData): 
		_metaColumn(metaColumn), 
		_pData(pData)
		/// Creates the Column.
	{
		poco_check_ptr (_pData);
	}

	Column(const Column& col):
		_metaColumn(col._metaColumn),
		_pData(col._pData)
		/// Creates the Column.
	{
	}

	~Column()
		/// Destroys the Column.
	{
	}

	Column& operator = (const Column& col)
		/// Assignment operator.
	{
		Column tmp(col);
		swap(tmp);
		return *this;
	}

	void swap(Column& other)
		/// Swaps the column with another one.
	{
		using std::swap;
		swap(_metaColumn, other._metaColumn);
		swap(_pData, other._pData);
	}

	Container& data()
		/// Returns reference to contained data.
	{
		return *_pData;
	}

	const T& value(std::size_t row) const
		/// Returns the field value in specified row.
		/// This is the std::list specialization and std::list
		/// is not the optimal solution for cases where random 
		/// access is needed.
		/// However, to allow for compatibility with other
		/// containers, this functionality is provided here.
		/// To alleviate the problem, an effort is made
		/// to start iteration from beginning or end,
		/// depending on the position requested.
	{
		if (row <= (std::size_t) (_pData->size() / 2))
		{
			Iterator it = _pData->begin();
			Iterator end = _pData->end();
			for (int i = 0; it != end; ++it, ++i)
				if (i == row) return *it;
		}
		else
		{
			row = _pData->size() - row;
			RIterator it = _pData->rbegin();
			RIterator end = _pData->rend();
			for (int i = 1; it != end; ++it, ++i)
				if (i == row) return *it;
		}

		throw RangeException("Invalid row number."); 
	}

	const T& operator [] (std::size_t row) const
		/// Returns the field value in specified row.
	{
		return value(row);
	}

	Size rowCount() const
		/// Returns number of rows.
	{
		return _pData->size();
	}

	void reset()
		/// Clears the storage.
	{
		_pData->clear();
	}

	const std::string& name() const
		/// Returns column name.
	{
		return _metaColumn.name();
	}

	std::size_t length() const
		/// Returns column maximum length.
	{
		return _metaColumn.length();
	}

	std::size_t precision() const
		/// Returns column precision.
		/// Valid for floating point fields only (zero for other data types).
	{
		return _metaColumn.precision();
	}

	std::size_t position() const
		/// Returns column position.
	{
		return _metaColumn.position();
	}

	MetaColumn::ColumnDataType type() const
		/// Returns column type.
	{
		return _metaColumn.type();
	}

	Iterator begin() const
		/// Returns iterator pointing to the beginning of data storage vector.
	{
		return _pData->begin();
	}

	Iterator end() const
		/// Returns iterator pointing to the end of data storage vector.
	{
		return _pData->end();
	}

private:
	Column();

	MetaColumn   _metaColumn;
	ContainerPtr _pData;
};


template <typename C>
inline void swap(Column<C>& c1, Column<C>& c2)
{
	c1.swap(c2);
}


} } // namespace Poco::Data


#endif // Data_Column_INCLUDED

