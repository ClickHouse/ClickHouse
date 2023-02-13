//
// RowIterator.h
//
// Library: Data
// Package: DataCore
// Module:  RowIterator
//
// Definition of the RowIterator class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_RowIterator_INCLUDED
#define Data_RowIterator_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/Row.h"
#include "Poco/Dynamic/Var.h"
#include <iterator>
#include <algorithm>


namespace Poco {
namespace Data {


class RecordSet;


class Data_API RowIterator
	/// RowIterator class.
{
public:
	typedef std::bidirectional_iterator_tag iterator_category;
	typedef Row                             value_type;
	typedef std::ptrdiff_t                  difference_type;
	typedef Row*                            pointer;
	typedef Row&                            reference;

	static const std::size_t POSITION_END;
		/// End position indicator.

	RowIterator(RecordSet* pRecordSet, bool positionEnd);
		/// Creates the RowIterator and positions it at the end of
		/// the recordset if positionEnd is true. Otherwise, it is
		/// positioned at the beginning.

	RowIterator(const RowIterator& other);
		/// Creates a copy of other RowIterator.

	~RowIterator();
		/// Destroys the RowIterator.

	RowIterator& operator = (const RowIterator& other);
		/// Assigns the other RowIterator.

	bool operator == (const RowIterator& other) const;
		/// Equality operator.

	bool operator != (const RowIterator& other) const;
		/// Inequality operator.

	Row& operator * () const;
		/// Returns reference to the current row.

	Row* operator -> () const;
		/// Returns pointer to the current row.

	const RowIterator& operator ++ () const;
		/// Advances by one position and returns current position.

	RowIterator operator ++ (int) const;
		/// Advances by one position and returns copy of the iterator with 
		/// previous current position.

	const RowIterator& operator -- () const;
		/// Goes back by one position and returns copy of the iterator with 
		/// previous current position.

	RowIterator operator -- (int) const;
		/// Goes back by one position and returns previous current position.

	RowIterator operator + (std::size_t diff) const;
		/// Returns a copy the RowIterator advanced by diff positions.

	RowIterator operator - (std::size_t diff) const;
		/// Returns a copy the RowIterator backed by diff positions.
		/// Throws RangeException if diff is larger than current position.

	void swap(RowIterator& other);
		/// Swaps the RowIterator with another one.

private:
	RowIterator();

	void increment() const;
		/// Increments the iterator position by one. 
		/// Throws RangeException if position is out of range.

	void decrement() const;
		/// Decrements the iterator position by one. 
		/// Throws RangeException if position is out of range.

	void setPosition(std::size_t pos) const;
		/// Sets the iterator position. 
		/// Throws RangeException if position is out of range.

	RecordSet*          _pRecordSet;
	mutable std::size_t _position;
};


///
/// inlines
///


inline bool RowIterator::operator == (const RowIterator& other) const
{
	return _pRecordSet == other._pRecordSet && _position == other._position;
}


inline bool RowIterator::operator != (const RowIterator& other) const
{
	return _pRecordSet != other._pRecordSet || _position != other._position;
}


} } // namespace Poco::Data


namespace std
{
	template<>
	inline void swap<Poco::Data::RowIterator>(Poco::Data::RowIterator& s1, 
		Poco::Data::RowIterator& s2)
		/// Full template specalization of std:::swap for RowIterator
	{
		s1.swap(s2);
	}
}


#endif // Data_RowIterator_INCLUDED
