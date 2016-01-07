//
// Preparation.h
//
// $Id: //poco/Main/Data/include/Poco/Data/Preparation.h#8 $
//
// Library: Data
// Package: DataCore
// Module:  Preparation
//
// Definition of the Preparation class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Preparation_INCLUDED
#define Data_Preparation_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/AbstractPreparation.h"
#include "Poco/Data/TypeHandler.h"
#include <cstddef>
#include <vector>


namespace Poco {
namespace Data {


template<typename T>
class Preparation: public AbstractPreparation
	/// Class for calling the appropriate AbstractPreparator method.
{
public:
	Preparation(AbstractPreparator::Ptr& pPreparator, std::size_t pos, T& val): 
		AbstractPreparation(pPreparator), 
		_pos(pos), 
		_val(val)
		/// Creates the Preparation.
	{
	}

	~Preparation()
		/// Destroys the Preparation.
	{
	}

	void prepare()
		/// Prepares data.
	{
		TypeHandler<T>::prepare(_pos, _val, preparation());
	}

private:
	std::size_t _pos;
	T&          _val;
};


template<typename T>
class Preparation<std::vector<T> >: public AbstractPreparation
	/// Preparation specialization for std::vector.
	/// This specialization is needed for bulk operations to enforce
	/// the whole vector preparation, rather than only individual contained values.
{
public:
	Preparation(AbstractPreparator::Ptr pPreparator, std::size_t pos, std::vector<T>& val = std::vector<T>()): 
		AbstractPreparation(pPreparator), 
		_pos(pos), 
		_val(val)
		/// Creates the Preparation.
	{
	}

	~Preparation()
		/// Destroys the Preparation.
	{
	}

	void prepare()
		/// Prepares data.
	{
		TypeHandler<std::vector<T> >::prepare(_pos, _val, preparation());
	}

private:
	std::size_t     _pos;
	std::vector<T>& _val;
};


template<typename T>
class Preparation<std::deque<T> >: public AbstractPreparation
	/// Preparation specialization for std::deque.
	/// This specialization is needed for bulk operations to enforce
	/// the whole deque preparation, rather than only individual contained values.
{
public:
	Preparation(AbstractPreparator::Ptr pPreparator, std::size_t pos, std::deque<T>& val = std::deque<T>()): 
		AbstractPreparation(pPreparator), 
		_pos(pos), 
		_val(val)
		/// Creates the Preparation.
	{
	}

	~Preparation()
		/// Destroys the Preparation.
	{
	}

	void prepare()
		/// Prepares data.
	{
		TypeHandler<std::deque<T> >::prepare(_pos, _val, preparation());
	}

private:
	std::size_t     _pos;
	std::deque<T>&  _val;
};


template<typename T>
class Preparation<std::list<T> >: public AbstractPreparation
	/// Preparation specialization for std::list.
	/// This specialization is needed for bulk operations to enforce
	/// the whole list preparation, rather than only individual contained values.
{
public:
	Preparation(AbstractPreparator::Ptr pPreparator, std::size_t pos, std::list<T>& val = std::list<T>()): 
		AbstractPreparation(pPreparator), 
		_pos(pos), 
		_val(val)
		/// Creates the Preparation.
	{
	}

	~Preparation()
		/// Destroys the Preparation.
	{
	}

	void prepare()
		/// Prepares data.
	{
		TypeHandler<std::list<T> >::prepare(_pos, _val, preparation());
	}

private:
	std::size_t   _pos;
	std::list<T>& _val;
};


} } // namespace Poco::Data


#endif // Data_Preparation_INCLUDED
