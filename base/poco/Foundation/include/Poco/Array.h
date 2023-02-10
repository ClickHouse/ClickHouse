//
// Array.h
//
// Library: Foundation
// Package: Core
// Module:  Array
//
// Definition of the Array class
//
// Copyright (c) 2004-2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//
// ------------------------------------------------------------------------------
// (C) Copyright Nicolai M. Josuttis 2001.
// Permission to copy, use, modify, sell and distribute this software
// is granted provided this copyright notice appears in all copies.
// This software is provided "as is" without express or implied
// warranty, and with no claim as to its suitability for any purpose.
// ------------------------------------------------------------------------------


#ifndef Foundation_Array_INCLUDED
#define Foundation_Array_INCLUDED

#include "Poco/Exception.h"
#include "Poco/Bugcheck.h"
#include <algorithm>

namespace Poco {

template<class T, std::size_t N>
class Array 
	/// STL container like C-style array replacement class. 
	/// 
	/// This implementation is based on the idea of Nicolai Josuttis.
	/// His original implementation can be found at http://www.josuttis.com/cppcode/array.html . 
{

public:

	typedef T				value_type;
	typedef T*				iterator;
	typedef const T*		const_iterator;
	typedef T&				reference;
	typedef const T&		const_reference;
	typedef std::size_t		size_type;
	typedef std::ptrdiff_t  difference_type;

	iterator begin()
	{
		return elems;
	}

	const_iterator begin() const
	{
		return elems;
	}

	iterator end()
	{ 
		return elems+N;
	}

	const_iterator end() const
	{
		return elems+N;
	}

	typedef std::reverse_iterator<iterator>			reverse_iterator;
	typedef std::reverse_iterator<const_iterator>   const_reverse_iterator;

	reverse_iterator rbegin()
	{
		return reverse_iterator(end());
	}

	const_reverse_iterator rbegin() const
	{
		return const_reverse_iterator(end());
	}

	reverse_iterator rend()
	{ 
		return reverse_iterator(begin());
	}

	const_reverse_iterator rend() const
	{
		return const_reverse_iterator(begin());
	}

	reference operator[](size_type i) 
		/// Element access without range check. If the index is not small than the given size, the behavior is undefined.
	{ 
		poco_assert( i < N && "out of range" ); 
		return elems[i];
	}

	const_reference operator[](size_type i) const 
		/// Element access without range check. If the index is not small than the given size, the behavior is undefined.
	{	 
		poco_assert( i < N && "out of range" ); 
		return elems[i]; 
	}

	reference at(size_type i)
		/// Element access with range check. Throws Poco::InvalidArgumentException if the index is over range.
	{ 
		if(i>=size())
			throw Poco::InvalidArgumentException("Array::at() range check failed: index is over range");
		return elems[i]; 
	}

	const_reference at(size_type i) const
		/// Element access with range check. Throws Poco::InvalidArgumentException if the index is over range.
	{
		if(i>=size())
			throw Poco::InvalidArgumentException("Array::at() range check failed: index is over range");
		return elems[i]; 
	}

	reference front() 
	{ 
		return elems[0]; 
	}

	const_reference front() const 
	{
		return elems[0];
	}

	reference back() 
	{ 
		return elems[N-1]; 
	}

	const_reference back() const 
	{ 
		return elems[N-1]; 
	}

	static size_type size()
	{
		return N; 
	}

	static bool empty()
	{ 
		return false; 
	}

	static size_type max_size()
	{ 
		return N; 
	}

	enum { static_size = N };

	void swap (Array<T,N>& y) {
		std::swap_ranges(begin(),end(),y.begin());
	}

	const T* data() const
		/// Direct access to data (read-only)
	{ 
		return elems; 
	}

	T* data()
	{ 
		return elems;
	}

	T* c_array(){ 
		/// Use array as C array (direct read/write access to data)
		return elems;
	}

	template <typename Other>
	Array<T,N>& operator= (const Array<Other,N>& rhs)
		/// Assignment with type conversion 
	{
		std::copy(rhs.begin(),rhs.end(), begin());
		return *this;
	}

	void assign (const T& value)
		/// Assign one value to all elements
	{
		std::fill_n(begin(),size(),value);
	}

public:

	T elems[N];	
		/// Fixed-size array of elements of type T, public specifier used to make this class a aggregate.

};

// comparisons
template<class T, std::size_t N>
bool operator== (const Array<T,N>& x, const Array<T,N>& y) 
{
	return std::equal(x.begin(), x.end(), y.begin());
}

template<class T, std::size_t N>
bool operator< (const Array<T,N>& x, const Array<T,N>& y) 
{
	return std::lexicographical_compare(x.begin(),x.end(),y.begin(),y.end());
}

template<class T, std::size_t N>
bool operator!= (const Array<T,N>& x, const Array<T,N>& y) 
{
	return !(x==y);
}

template<class T, std::size_t N>
bool operator> (const Array<T,N>& x, const Array<T,N>& y) 
{
	return y<x;
}

template<class T, std::size_t N>
bool operator<= (const Array<T,N>& x, const Array<T,N>& y) 
{
	return !(y<x);
}

template<class T, std::size_t N>
bool operator>= (const Array<T,N>& x, const Array<T,N>& y) 
{
	return !(x<y);
}

template<class T, std::size_t N>
inline void swap (Array<T,N>& x, Array<T,N>& y) 
	/// global swap()
{
	x.swap(y);
}

}// namespace Poco

#endif // Foundation_Array_INCLUDED

