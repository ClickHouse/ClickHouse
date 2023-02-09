//
// BulkBinding.h
//
// Library: Data
// Package: DataCore
// Module:  BulkBinding
//
// Definition of the BulkBinding class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_BulkBinding_INCLUDED
#define Data_BulkBinding_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/AbstractBinding.h"
#include "Poco/Data/DataException.h"
#include "Poco/Data/TypeHandler.h"
#include "Poco/Data/Bulk.h"
#include <vector>
#include <deque>
#include <list>
#include <cstddef>


namespace Poco {
namespace Data {


template <class T>
class BulkBinding: public AbstractBinding
	/// A BulkBinding maps a value to a column. 
	/// Bulk binding support is provided only for std::vector.
{
public:
	BulkBinding(const T& val, Poco::UInt32 bulkSize, const std::string& name = "", Direction direction = PD_IN): 
		AbstractBinding(name, direction, bulkSize), 
		_val(val), 
		_bound(false)
		/// Creates the BulkBinding.
	{
		if (0 == _val.size())
			throw BindingException("Zero size containers not allowed.");
	}

	~BulkBinding()
		/// Destroys the BulkBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return 1;
	}

	std::size_t numOfRowsHandled() const
	{
		return _val.size();
	}

	bool canBind() const
	{
		return !_bound;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		TypeHandler<T>::bind(pos, _val, getBinder(), getDirection());
		_bound = true;
	}

	void reset ()
	{
		_bound = false;
		getBinder()->reset();
	}

private:
	const T& _val;
	bool     _bound;
};


namespace Keywords {


template <typename T> 
AbstractBinding::Ptr use(const std::vector<T>& t, BulkFnType, const std::string& name = "")
	/// Convenience function for a more compact BulkBinding creation for std::vector.
{
	return new BulkBinding<std::vector<T> >(t, static_cast<Poco::UInt32>(t.size()), name);
}


template <typename T> 
AbstractBinding::Ptr in(const std::vector<T>& t, BulkFnType, const std::string& name = "")
	/// Convenience function for a more compact BulkBinding creation for std::vector.
{
	return new BulkBinding<std::vector<T> >(t, static_cast<Poco::UInt32>(t.size()), name);
}


template <typename T> 
AbstractBinding::Ptr use(const std::deque<T>& t, BulkFnType, const std::string& name = "")
	/// Convenience function for a more compact BulkBinding creation for std::deque.
{
	return new BulkBinding<std::deque<T> >(t, static_cast<Poco::UInt32>(t.size()), name);
}


template <typename T> 
AbstractBinding::Ptr in(const std::deque<T>& t, BulkFnType, const std::string& name = "")
	/// Convenience function for a more compact BulkBinding creation for std::deque.
{
	return new BulkBinding<std::deque<T> >(t, static_cast<Poco::UInt32>(t.size()), name);
}


template <typename T> 
AbstractBinding::Ptr use(const std::list<T>& t, BulkFnType, const std::string& name = "")
	/// Convenience function for a more compact BulkBinding creation for std::list.
{
	return new BulkBinding<std::list<T> >(t, static_cast<Poco::UInt32>(t.size()), name);
}


template <typename T> 
AbstractBinding::Ptr in(const std::list<T>& t, BulkFnType, const std::string& name = "")
	/// Convenience function for a more compact BulkBinding creation for std::list.
{
	return new BulkBinding<std::list<T> >(t, static_cast<Poco::UInt32>(t.size()), name);
}


} // namespace Keywords


} } // namespace Poco::Data


#endif // Data_BulkBinding_INCLUDED
