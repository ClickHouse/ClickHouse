//
// Binding.h
//
// Library: Data
// Package: DataCore
// Module:  Binding
//
// Definition of the Binding class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Binding_INCLUDED
#define Data_Binding_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/AbstractBinding.h"
#include "Poco/Data/DataException.h"
#include "Poco/Data/TypeHandler.h"
#include "Poco/SharedPtr.h"
#include "Poco/MetaProgramming.h"
#include "Poco/Bugcheck.h"
#include <vector>
#include <list>
#include <deque>
#include <set>
#include <map>
#include <cstddef>


namespace Poco {
namespace Data {


template <class T>
class Binding: public AbstractBinding
	/// Binding maps a value or multiple values (see Binding specializations for STL containers as
	/// well as type handlers) to database column(s). Values to be bound can be either mapped 
	/// directly (by reference) or a copy can be created, depending on the value of the copy argument.
	/// To pass a reference to a variable, it is recommended to pass it to the intermediate
	/// utility function use(), which will create the proper binding. In cases when a reference 
	/// is passed to binding, the storage it refers to must be valid at the statement execution time.
	/// To pass a copy of a variable, constant or string literal, use utility function bind().
	/// Variables can be passed as either copies or references (i.e. using either use() or bind()).
	/// Constants, however, can only be passed as copies. this is best achieved using bind() utility 
	/// function. An attempt to pass a constant by reference shall result in compile-time error.
{
public:
	typedef T                  ValType;
	typedef SharedPtr<ValType> ValPtr;
	typedef Binding<ValType>   Type;
	typedef SharedPtr<Type>    Ptr;

	explicit Binding(T& val,
		const std::string& name = "", 
		Direction direction = PD_IN): 
		AbstractBinding(name, direction),
		_val(val), 
		_bound(false)
		/// Creates the Binding using the passed reference as bound value.
		/// If copy is true, a copy of the value referred to is created.
	{
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return 1u;
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
		AbstractBinder::Ptr pBinder = getBinder();
		poco_assert_dbg (!pBinder.isNull());
		pBinder->reset();
	}

private:
	const T& _val;
	bool     _bound;
};



template <class T>
class CopyBinding: public AbstractBinding
	/// Binding maps a value or multiple values (see Binding specializations for STL containers as
	/// well as type handlers) to database column(s). Values to be bound can be either mapped 
	/// directly (by reference) or a copy can be created, depending on the value of the copy argument.
	/// To pass a reference to a variable, it is recommended to pass it to the intermediate
	/// utility function use(), which will create the proper binding. In cases when a reference 
	/// is passed to binding, the storage it refers to must be valid at the statement execution time.
	/// To pass a copy of a variable, constant or string literal, use utility function bind().
	/// Variables can be passed as either copies or references (i.e. using either use() or bind()).
{
public:
	typedef T                    ValType;
	typedef SharedPtr<ValType>   ValPtr;
	typedef CopyBinding<ValType> Type;
	typedef SharedPtr<Type>      Ptr;

	explicit CopyBinding(T& val,
		const std::string& name = "", 
		Direction direction = PD_IN): 
		AbstractBinding(name, direction),
		_pVal(new T(val)),
		_bound(false)
		/// Creates the Binding using the passed reference as bound value.
		/// If copy is true, a copy of the value referred to is created.
	{
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return 1;
	}

	bool canBind() const
	{
		return !_bound;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		TypeHandler<T>::bind(pos, *_pVal, getBinder(), getDirection());
		_bound = true;
	}

	void reset ()
	{
		_bound = false;
		AbstractBinder::Ptr pBinder = getBinder();
		poco_assert_dbg (!pBinder.isNull());
		pBinder->reset();
	}

private:
	//typedef typename TypeWrapper<T>::TYPE ValueType;
	ValPtr _pVal;
	bool   _bound;
};


template <>
class Binding<const char*>: public AbstractBinding
	/// Binding const char* specialization wraps char pointer into string.
{
public:
	typedef const char*          ValType;
	typedef SharedPtr<ValType>   ValPtr;
	typedef Binding<const char*> Type;
	typedef SharedPtr<Type>      Ptr;

	explicit Binding(const char* pVal, 
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(pVal ? pVal : throw NullPointerException() ),
		_bound(false)
		/// Creates the Binding by copying the passed string.
	{
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return 1u;
	}

	std::size_t numOfRowsHandled() const
	{
		return 1u;
	}

	bool canBind() const
	{
		return !_bound;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		TypeHandler<std::string>::bind(pos, _val, getBinder(), getDirection());
		_bound = true;
	}

	void reset ()
	{
		_bound = false;
		AbstractBinder::Ptr pBinder = getBinder();
		poco_assert_dbg (!pBinder.isNull());
		pBinder->reset();
	}

private:
	std::string _val;
	bool        _bound;
};



template <>
class CopyBinding<const char*>: public AbstractBinding
	/// Binding const char* specialization wraps char pointer into string.
{
public:
	typedef const char*              ValType;
	typedef SharedPtr<ValType>       ValPtr;
	typedef CopyBinding<const char*> Type;
	typedef SharedPtr<Type>          Ptr;

	explicit CopyBinding(const char* pVal, 
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(pVal ? pVal : throw NullPointerException() ),
		_bound(false)
		/// Creates the Binding by copying the passed string.
	{
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return 1u;
	}

	std::size_t numOfRowsHandled() const
	{
		return 1u;
	}

	bool canBind() const
	{
		return !_bound;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		TypeHandler<std::string>::bind(pos, _val, getBinder(), getDirection());
		_bound = true;
	}

	void reset ()
	{
		_bound = false;
		AbstractBinder::Ptr pBinder = getBinder();
		poco_assert_dbg (!pBinder.isNull());
		pBinder->reset();
	}

private:
	std::string _val;
	bool        _bound;
};


template <class T>
class Binding<std::vector<T> >: public AbstractBinding
	/// Specialization for std::vector.
{
public:
	typedef std::vector<T>                   ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<Binding<ValType> >     Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit Binding(std::vector<T>& val, 
		const std::string& name = "", 
		Direction direction = PD_IN): 
		AbstractBinding(name, direction),
		_val(val), 
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_val.size());
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _val.begin();
		_end   = _val.end();
	}

private:
	const ValType& _val;
	Iterator       _begin;
	Iterator       _end;
};


template <class T>
class CopyBinding<std::vector<T> >: public AbstractBinding
	/// Specialization for std::vector.
{
public:
	
	typedef std::vector<T>                   ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<CopyBinding<ValType> > Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit CopyBinding(std::vector<T>& val, 
		const std::string& name = "", 
		Direction direction = PD_IN): 
		AbstractBinding(name, direction),
		_pVal(new std::vector<T>(val)),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _pVal->size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _pVal->begin();
		_end   = _pVal->end();
	}

private:
	ValPtr   _pVal;
	Iterator _begin;
	Iterator _end;
};


template <>
class Binding<std::vector<bool> >: public AbstractBinding
	/// Specialization for std::vector<bool>.
	/// This specialization is necessary due to the nature of std::vector<bool>.
	/// For details, see the standard library implementation of std::vector<bool> 
	/// or
	/// S. Meyers: "Effective STL" (Copyright Addison-Wesley 2001),
	/// Item 18: "Avoid using vector<bool>."
	/// 
	/// The workaround employed here is using std::deque<bool> as an
	/// internal replacement container. 
	/// 
	/// IMPORTANT:
	/// Only IN binding is supported.
{
public:
	typedef std::vector<bool>            ValType;
	typedef SharedPtr<ValType>           ValPtr;
	typedef SharedPtr<Binding<ValType> > Ptr;
	typedef ValType::const_iterator      Iterator;

	explicit Binding(const std::vector<bool>& val, 
		const std::string& name = "", 
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(val), 
		_deq(_val.begin(), _val.end()),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN != direction)
			throw BindingException("Only IN direction is legal for std:vector<bool> binding.");

		if (numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return 1u;
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_val.size());
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<bool>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;

	}

	void reset()
	{
		_begin = _deq.begin();
		_end   = _deq.end();
	}

private:
	const std::vector<bool>&         _val;
	std::deque<bool>                 _deq;
	std::deque<bool>::const_iterator _begin;
	std::deque<bool>::const_iterator _end;
};


template <>
class CopyBinding<std::vector<bool> >: public AbstractBinding
	/// Specialization for std::vector<bool>.
	/// This specialization is necessary due to the nature of std::vector<bool>.
	/// For details, see the standard library implementation of std::vector<bool> 
	/// or
	/// S. Meyers: "Effective STL" (Copyright Addison-Wesley 2001),
	/// Item 18: "Avoid using vector<bool>."
	/// 
	/// The workaround employed here is using std::deque<bool> as an
	/// internal replacement container. 
	/// 
	/// IMPORTANT:
	/// Only IN binding is supported.
{
public:
	typedef std::vector<bool>                ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<CopyBinding<ValType> > Ptr;
	typedef ValType::const_iterator          Iterator;

	explicit CopyBinding(const std::vector<bool>& val, 
		const std::string& name = "", 
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_deq(val.begin(), val.end()),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN != direction)
			throw BindingException("Only IN direction is legal for std:vector<bool> binding.");

		if (numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");

		reset();
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return 1u;
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_deq.size());
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<bool>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _deq.begin();
		_end   = _deq.end();
	}

private:
	std::deque<bool>                 _deq;
	std::deque<bool>::const_iterator _begin;
	std::deque<bool>::const_iterator _end;
};


template <class T>
class Binding<std::list<T> >: public AbstractBinding
	/// Specialization for std::list.
{
public:
	typedef std::list<T>                     ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<Binding<ValType> >     Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit Binding(std::list<T>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(val), 
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _val.size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _val.begin();
		_end   = _val.end();
	}

private:
	const ValType& _val;
	Iterator       _begin;
	Iterator       _end;
};


template <class T>
class CopyBinding<std::list<T> >: public AbstractBinding
	/// Specialization for std::list.
{
public:
	typedef typename std::list<T>            ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<CopyBinding<ValType> > Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit CopyBinding(ValType& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_pVal(new std::list<T>(val)),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _pVal->size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _pVal->begin();
		_end   = _pVal->end();
	}

private:
	ValPtr   _pVal;
	Iterator _begin;
	Iterator _end;
};


template <class T>
class Binding<std::deque<T> >: public AbstractBinding
	/// Specialization for std::deque.
{
public:
	typedef std::deque<T>                    ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<Binding<ValType> >     Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit Binding(std::deque<T>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(val), 
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _val.size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _val.begin();
		_end   = _val.end();
	}

private:
	const ValType& _val;
	Iterator       _begin;
	Iterator       _end;
};


template <class T>
class CopyBinding<std::deque<T> >: public AbstractBinding
	/// Specialization for std::deque.
{
public:
	typedef std::deque<T>                    ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<CopyBinding<ValType> > Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit CopyBinding(std::deque<T>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_pVal(new std::deque<T>(val)),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _pVal->size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _pVal->begin();
		_end   = _pVal->end();
	}

private:
	ValPtr   _pVal;
	Iterator _begin;
	Iterator _end;
};


template <class T>
class Binding<std::set<T> >: public AbstractBinding
	/// Specialization for std::set.
{
public:
	typedef std::set<T>                      ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<Binding<ValType> >     Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit Binding(std::set<T>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(val), 
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_val.size());
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _val.begin();
		_end   = _val.end();
	}

private:
	const ValType& _val;
	Iterator       _begin;
	Iterator       _end;
};


template <class T>
class CopyBinding<std::set<T> >: public AbstractBinding
	/// Specialization for std::set.
{
public:
	typedef std::set<T>                      ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<CopyBinding<ValType> > Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit CopyBinding(std::set<T>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_pVal(new std::set<T>(val)),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _pVal->size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _pVal->begin();
		_end   = _pVal->end();
	}

private:
	ValPtr   _pVal;
	Iterator _begin;
	Iterator _end;
};


template <class T>
class Binding<std::multiset<T> >: public AbstractBinding
	/// Specialization for std::multiset.
{
public:
	typedef std::multiset<T>                 ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<Binding<ValType> >     Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit Binding(std::multiset<T>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(val), 
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_val.size());
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _val.begin();
		_end   = _val.end();
	}

private:
	const ValType& _val;
	Iterator       _begin;
	Iterator       _end;
};


template <class T>
class CopyBinding<std::multiset<T> >: public AbstractBinding
	/// Specialization for std::multiset.
{
public:
	typedef std::multiset<T>             ValType;
	typedef SharedPtr<ValType>           ValPtr;
	typedef SharedPtr<CopyBinding<ValType> > Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit CopyBinding(std::multiset<T>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_pVal(new std::multiset<T>(val)),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _pVal->size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<T>::bind(pos, *_begin, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _pVal->begin();
		_end   = _pVal->end();
	}

private:
	ValPtr    _pVal;
	Iterator _begin;
	Iterator _end;
};


template <class K, class V>
class Binding<std::map<K, V> >: public AbstractBinding
	/// Specialization for std::map.
{
public:
	typedef std::map<K, V>                   ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<Binding<ValType> >     Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit Binding(std::map<K, V>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(val), 
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<V>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_val.size());
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<V>::bind(pos, _begin->second, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _val.begin();
		_end   = _val.end();
	}

private:
	const ValType& _val;
	Iterator       _begin;
	Iterator       _end;
};


template <class K, class V>
class CopyBinding<std::map<K, V> >: public AbstractBinding
	/// Specialization for std::map.
{
public:
	typedef std::map<K, V>                   ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<CopyBinding<ValType> > Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit CopyBinding(std::map<K, V>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_pVal(new std::map<K, V>(val)),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<V>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _pVal->size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<V>::bind(pos, _begin->second, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _pVal->begin();
		_end   = _pVal->end();
	}

private:
	ValPtr   _pVal;
	Iterator _begin;
	Iterator _end;
};


template <class K, class V>
class Binding<std::multimap<K, V> >: public AbstractBinding
	/// Specialization for std::multimap.
{
public:
	typedef std::multimap<K, V>              ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<Binding<ValType> >     Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit Binding(std::multimap<K, V>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_val(val), 
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~Binding()
		/// Destroys the Binding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<V>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_val.size());
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<V>::bind(pos, _begin->second, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _val.begin();
		_end   = _val.end();
	}

private:
	const ValType& _val;
	Iterator       _begin;
	Iterator       _end;
};


template <class K, class V>
class CopyBinding<std::multimap<K, V> >: public AbstractBinding
	/// Specialization for std::multimap.
{
public:
	typedef std::multimap<K, V>              ValType;
	typedef SharedPtr<ValType>               ValPtr;
	typedef SharedPtr<CopyBinding<ValType> > Ptr;
	typedef typename ValType::const_iterator Iterator;

	explicit CopyBinding(std::multimap<K, V>& val,
		const std::string& name = "",
		Direction direction = PD_IN): 
		AbstractBinding(name, direction), 
		_pVal(new std::multimap<K, V>(val)),
		_begin(), 
		_end()
		/// Creates the Binding.
	{
		if (PD_IN == direction && numOfRowsHandled() == 0)
			throw BindingException("It is illegal to bind to an empty data collection");
		reset();
	}

	~CopyBinding()
		/// Destroys the CopyBinding.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<V>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _pVal->size();
	}

	bool canBind() const
	{
		return _begin != _end;
	}

	void bind(std::size_t pos)
	{
		poco_assert_dbg(!getBinder().isNull());
		poco_assert_dbg(canBind());
		TypeHandler<V>::bind(pos, _begin->second, getBinder(), getDirection());
		++_begin;
	}

	void reset()
	{
		_begin = _pVal->begin();
		_end   = _pVal->end();
	}

private:
	ValPtr   _pVal;
	Iterator _begin;
	Iterator _end;
};


namespace Keywords {


template <typename T> 
inline AbstractBinding::Ptr use(T& t, const std::string& name = "")
	/// Convenience function for a more compact Binding creation.
{
	// If this fails to compile, a const ref was passed to use().
	// This can be resolved by either (a) using bind (which will copy the value),
	// or (b) if the const ref is guaranteed to exist when execute is called 
	// (which can be much later!), by using the "useRef" keyword instead
	poco_static_assert (!IsConst<T>::VALUE);
	return new Binding<T>(t, name, AbstractBinding::PD_IN);
}


inline AbstractBinding::Ptr use(const NullData& t, const std::string& name = "")
	/// NullData overload.
{
	return new Binding<NullData>(const_cast<NullData&>(t), name, AbstractBinding::PD_IN);
}


template <typename T> 
inline AbstractBinding::Ptr useRef(T& t, const std::string& name = "")
	/// Convenience function for a more compact Binding creation.
{
	return new Binding<T>(t, name, AbstractBinding::PD_IN);
}


template <typename T> 
inline AbstractBinding::Ptr in(T& t, const std::string& name = "")
	/// Convenience function for a more compact Binding creation.
{
	return use(t, name);
}


inline AbstractBinding::Ptr in(const NullData& t, const std::string& name = "")
	/// NullData overload.
{
	return use(t, name);
}


template <typename T> 
inline AbstractBinding::Ptr out(T& t)
	/// Convenience function for a more compact Binding creation.
{
	poco_static_assert (!IsConst<T>::VALUE);
	return new Binding<T>(t, "", AbstractBinding::PD_OUT);
}


template <typename T> 
inline AbstractBinding::Ptr io(T& t)
	/// Convenience function for a more compact Binding creation.
{
	poco_static_assert (!IsConst<T>::VALUE);
	return new Binding<T>(t, "", AbstractBinding::PD_IN_OUT);
}


inline AbstractBindingVec& use(AbstractBindingVec& bv)
	/// Convenience dummy function (for syntax purposes only).
{
	return bv;
}


inline AbstractBindingVec& in(AbstractBindingVec& bv)
	/// Convenience dummy function (for syntax purposes only).
{
	return bv;
}


inline AbstractBindingVec& out(AbstractBindingVec& bv)
	/// Convenience dummy function (for syntax purposes only).
{
	return bv;
}


inline AbstractBindingVec& io(AbstractBindingVec& bv)
	/// Convenience dummy function (for syntax purposes only).
{
	return bv;
}


template <typename T> 
inline AbstractBinding::Ptr bind(T t, const std::string& name)
	/// Convenience function for a more compact Binding creation.
	/// This funtion differs from use() in its value copy semantics.
{
	return new CopyBinding<T>(t, name, AbstractBinding::PD_IN);
}


template <typename T> 
inline AbstractBinding::Ptr bind(T t)
	/// Convenience function for a more compact Binding creation.
	/// This funtion differs from use() in its value copy semantics.
{
	return Poco::Data::Keywords::bind(t, "");
}


} // namespace Keywords


} } // namespace Poco::Data


#endif // Data_Binding_INCLUDED
