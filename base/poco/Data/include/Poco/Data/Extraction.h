//
// Extraction.h
//
// Library: Data
// Package: DataCore
// Module:  Extraction
//
// Definition of the Extraction class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Extraction_INCLUDED
#define Data_Extraction_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/AbstractExtraction.h"
#include "Poco/Data/Preparation.h"
#include "Poco/Data/TypeHandler.h"
#include "Poco/Data/Column.h"
#include "Poco/Data/Position.h"
#include "Poco/Data/DataException.h"
#include <set>
#include <vector>
#include <list>
#include <deque>
#include <map>
#include <cstddef>


namespace Poco {
namespace Data {


template <class T>
class Extraction: public AbstractExtraction
	/// Concrete Data Type specific extraction of values from a query result set.
{
public:
	typedef T                   ValType;
	typedef SharedPtr<ValType>  ValPtr;
	typedef Extraction<ValType> Type;
	typedef SharedPtr<Type>     Ptr;

	Extraction(T& result, const Position& pos = Position(0)):
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(), 
		_extracted(false),
		_null(false)
		/// Creates an Extraction object at specified position.
		/// Uses an empty object T as default value.
	{
	}

	Extraction(T& result, const T& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def), 
		_extracted(false),
		_null(false)
		/// Creates an Extraction object at specified position.
		/// Uses the provided def object as default value.
	{
	}

	~Extraction()
		/// Destroys the Extraction object.
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _extracted ? 1u : 0;
	}

	std::size_t numOfRowsAllowed() const
	{
		return 1u;
	}

	bool isNull(std::size_t row = 0) const
	{
		return _null;
	}

	std::size_t extract(std::size_t pos)
	{
		if (_extracted) throw ExtractException("value already extracted");
		_extracted = true;
		AbstractExtractor::Ptr pExt = getExtractor();
		TypeHandler<T>::extract(pos, _rResult, _default, pExt);
		_null = isValueNull<T>(_rResult, pExt->isNull(pos));
		
		return 1u;
	}

	void reset()
	{
		_extracted = false;
	}

	bool canExtract() const
	{
		return !_extracted;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<T>(pPrep, pos, _rResult);
	}

private:
	T&   _rResult;
	T    _default;
	bool _extracted;
	bool _null;
};


template <class T>
class Extraction<std::vector<T> >: public AbstractExtraction
	/// Vector Data Type specialization for extraction of values from a query result set.
{
public:
	
	typedef std::vector<T>      ValType;
	typedef SharedPtr<ValType>  ValPtr;
	typedef Extraction<ValType> Type;
	typedef SharedPtr<Type>     Ptr;

	Extraction(std::vector<T>& result, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default()
	{
		_rResult.clear();
	}

	Extraction(std::vector<T>& result, const T& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def)
	{
		_rResult.clear();
	}

	virtual ~Extraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_rResult.size());
	}

	std::size_t numOfRowsAllowed() const
	{
		return getLimit();
	}

	bool isNull(std::size_t row) const
	{
		try
		{
			return _nulls.at(row);
		}
		catch (std::out_of_range& ex)
		{ 
			throw RangeException(ex.what()); 
		}
	}

	std::size_t extract(std::size_t pos)
	{
		AbstractExtractor::Ptr pExt = getExtractor();
		_rResult.push_back(_default);
		TypeHandler<T>::extract(pos, _rResult.back(), _default, pExt);
		_nulls.push_back(isValueNull(_rResult.back(), pExt->isNull(pos)));
		return 1u;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<T>(pPrep, pos, _default);
	}

	void reset()
	{
		_nulls.clear();
	}

protected:

	const std::vector<T>& result() const
	{
		return _rResult;
	}

private:
	std::vector<T>&  _rResult;
	T                _default;
	std::deque<bool> _nulls;
};


template <>
class Extraction<std::vector<bool> >: public AbstractExtraction
	/// Vector bool specialization for extraction of values from a query result set.
{
public:
	typedef std::vector<bool>   ValType;
	typedef SharedPtr<ValType>  ValPtr;
	typedef Extraction<ValType> Type;
	typedef SharedPtr<Type>     Ptr;

	Extraction(std::vector<bool>& result, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default()
	{
		_rResult.clear();
	}

	Extraction(std::vector<bool>& result, const bool& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def)
	{
		_rResult.clear();
	}

	virtual ~Extraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<bool>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_rResult.size());
	}

	std::size_t numOfRowsAllowed() const
	{
		return getLimit();
	}

	bool isNull(std::size_t row) const
	{
		try
		{
			return _nulls.at(row);
		}
		catch (std::out_of_range& ex)
		{ 
			throw RangeException(ex.what()); 
		}
	}

	std::size_t extract(std::size_t pos)
	{
		AbstractExtractor::Ptr pExt = getExtractor();

		bool tmp = _default;
		TypeHandler<bool>::extract(pos, tmp, _default, pExt);
		_rResult.push_back(tmp);
		_nulls.push_back(pExt->isNull(pos));
		return 1u;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<bool>(pPrep, pos, _default);
	}

	void reset()
	{
		_nulls.clear();
	}

protected:

	const std::vector<bool>& result() const
	{
		return _rResult;
	}

private:
	std::vector<bool>& _rResult;
	bool               _default;
	std::deque<bool>   _nulls;
};


template <class T>
class Extraction<std::list<T> >: public AbstractExtraction
	/// List Data Type specialization for extraction of values from a query result set.
{
public:
	typedef std::list<T>        ValType;
	typedef SharedPtr<ValType>  ValPtr;
	typedef Extraction<ValType> Type;
	typedef SharedPtr<Type>     Ptr;

	Extraction(std::list<T>& result, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default()
	{
		_rResult.clear();
	}

	Extraction(std::list<T>& result, const T& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def)
	{
		_rResult.clear();
	}

	virtual ~Extraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _rResult.size();
	}

	std::size_t numOfRowsAllowed() const
	{
		return getLimit();
	}

	bool isNull(std::size_t row) const
	{
		try
		{
			return _nulls.at(row);
		}
		catch (std::out_of_range& ex)
		{ 
			throw RangeException(ex.what()); 
		}
	}

	std::size_t extract(std::size_t pos)
	{
		AbstractExtractor::Ptr pExt = getExtractor();
		_rResult.push_back(_default);
		TypeHandler<T>::extract(pos, _rResult.back(), _default, pExt);
		_nulls.push_back(isValueNull(_rResult.back(), pExt->isNull(pos)));
		return 1u;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<T>(pPrep, pos, _default);
	}

	void reset()
	{
		_nulls.clear();
	}

protected:

	const std::list<T>& result() const
	{
		return _rResult;
	}

private:
	std::list<T>&    _rResult;
	T                _default;
	std::deque<bool> _nulls;
};


template <class T>
class Extraction<std::deque<T> >: public AbstractExtraction
	/// Deque Data Type specialization for extraction of values from a query result set.
{
public:
	typedef std::deque<T>       ValType;
	typedef SharedPtr<ValType>  ValPtr;
	typedef Extraction<ValType> Type;
	typedef SharedPtr<Type>     Ptr;

	Extraction(std::deque<T>& result, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default()
	{
		_rResult.clear();
	}

	Extraction(std::deque<T>& result, const T& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def)
	{
		_rResult.clear();
	}

	virtual ~Extraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return _rResult.size();
	}

	std::size_t numOfRowsAllowed() const
	{
		return getLimit();
	}

	bool isNull(std::size_t row) const
	{
		try
		{
			return _nulls.at(row);
		}
		catch (std::out_of_range& ex)
		{ 
			throw RangeException(ex.what()); 
		}
	}

	std::size_t extract(std::size_t pos)
	{
		AbstractExtractor::Ptr pExt = getExtractor();
		_rResult.push_back(_default);
		TypeHandler<T>::extract(pos, _rResult.back(), _default, pExt);
		_nulls.push_back(isValueNull(_rResult.back(), pExt->isNull(pos)));
		return 1u;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<T>(pPrep, pos, _default);
	}

	void reset()
	{
		_nulls.clear();
	}

protected:

	const std::deque<T>& result() const
	{
		return _rResult;
	}

private:
	std::deque<T>&   _rResult;
	T                _default;
	std::deque<bool> _nulls;
};


template <class C>
class InternalExtraction: public Extraction<C>
	/// Container Data Type specialization extension for extraction of values from a query result set.
	///
	/// This class is intended for PocoData internal use - it is used by StatementImpl 
	/// to automaticaly create internal Extraction in cases when statement returns data and no external storage
	/// was supplied. It is later used by RecordSet to retrieve the fetched data after statement execution.
	/// It takes ownership of the Column pointer supplied as constructor argument. Column object, in turn
	/// owns the data container pointer.
	///
	/// InternalExtraction objects can not be copied or assigned.
{
public:
	typedef typename C::value_type ValType;
	typedef SharedPtr<ValType>     ValPtr;
	typedef Extraction<ValType>    Type;
	typedef SharedPtr<Type>        Ptr;


	InternalExtraction(C& result, Column<C>* pColumn, const Position& pos = Position(0)): 
		Extraction<C>(result, ValType(), pos), 
		_pColumn(pColumn)
		/// Creates InternalExtraction.
	{
	}

	~InternalExtraction()
		/// Destroys InternalExtraction.
	{
		delete _pColumn;
	}

	void reset()
	{
		Extraction<C>::reset();
		_pColumn->reset();
	}	

	const ValType& value(int index) const
	{
		try
		{ 
			return Extraction<C>::result().at(index); 
		}
		catch (std::out_of_range& ex)
		{ 
			throw RangeException(ex.what()); 
		}
	}

	bool isNull(std::size_t row) const
	{
		return Extraction<C>::isNull(row);
	}

	const Column<C>& column() const
	{
		return *_pColumn;
	}

private:
	InternalExtraction();
	InternalExtraction(const InternalExtraction&);
	InternalExtraction& operator = (const InternalExtraction&);

	Column<C>* _pColumn;
};


template <class T>
class Extraction<std::set<T> >: public AbstractExtraction
	/// Set Data Type specialization for extraction of values from a query result set.
{
public:
	typedef std::set<T>                ValType;
	typedef SharedPtr<ValType>         ValPtr;
	typedef Extraction<ValType>        Type;
	typedef SharedPtr<Type>            Ptr;
	typedef typename ValType::iterator Iterator;

	Extraction(std::set<T>& result, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default()
	{
		_rResult.clear();
	}

	Extraction(std::set<T>& result, const T& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def)
	{
		_rResult.clear();
	}

	~Extraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_rResult.size());
	}

	std::size_t numOfRowsAllowed() const
	{
		return getLimit();
	}

	std::size_t extract(std::size_t pos)
	{
		T tmp;
		TypeHandler<T>::extract(pos, tmp, _default, getExtractor());
		_rResult.insert(tmp);
		return 1u;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<T>(pPrep, pos, _default);
	}

private:
	std::set<T>& _rResult;
	T            _default;
};


template <class T>
class Extraction<std::multiset<T> >: public AbstractExtraction
	/// Multiset Data Type specialization for extraction of values from a query result set.
{
public:
	typedef std::multiset<T>    ValType;
	typedef SharedPtr<ValType>  ValPtr;
	typedef Extraction<ValType> Type;
	typedef SharedPtr<Type>     Ptr;

	Extraction(std::multiset<T>& result, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default()
	{
		_rResult.clear();
	}

	Extraction(std::multiset<T>& result, const T& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def)
	{
		_rResult.clear();
	}

	~Extraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<T>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_rResult.size());
	}

	std::size_t numOfRowsAllowed() const
	{
		return getLimit();
	}

	std::size_t extract(std::size_t pos)
	{
		T tmp;
		TypeHandler<T>::extract(pos, tmp, _default, getExtractor());
		_rResult.insert(tmp);
		return 1u;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<T>(pPrep, pos, _default);
	}

private:
	std::multiset<T>& _rResult;
	T                 _default;
};


template <class K, class V>
class Extraction<std::map<K, V> >: public AbstractExtraction
	/// Map Data Type specialization for extraction of values from a query result set.
{
public:
	typedef std::map<K, V>      ValType;
	typedef SharedPtr<ValType>  ValPtr;
	typedef Extraction<ValType> Type;
	typedef SharedPtr<Type>     Ptr;

	Extraction(std::map<K, V>& result, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default()
	{
		_rResult.clear();
	}

	Extraction(std::map<K, V>& result, const V& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def)
	{
		_rResult.clear();
	}

	~Extraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<V>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_rResult.size());
	}

	std::size_t numOfRowsAllowed() const
	{
		return getLimit();
	}

	std::size_t extract(std::size_t pos)
	{
		V tmp;
		TypeHandler<V>::extract(pos, tmp, _default, getExtractor());
		_rResult.insert(std::make_pair(tmp(), tmp));
		return 1u;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<V>(pPrep, pos, _default);
	}

private:
	std::map<K, V>& _rResult;
	V               _default;
};


template <class K, class V>
class Extraction<std::multimap<K, V> >: public AbstractExtraction
	/// Multimap Data Type specialization for extraction of values from a query result set.
{
public:
	typedef std::multimap<K, V> ValType;
	typedef SharedPtr<ValType>  ValPtr;
	typedef Extraction<ValType> Type;
	typedef SharedPtr<Type>     Ptr;

	Extraction(std::multimap<K, V>& result, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default()
	{
		_rResult.clear();
	}

	Extraction(std::multimap<K, V>& result, const V& def, const Position& pos = Position(0)): 
		AbstractExtraction(Limit::LIMIT_UNLIMITED, pos.value()),
		_rResult(result), 
		_default(def)
	{
		_rResult.clear();
	}

	~Extraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<V>::size();
	}

	std::size_t numOfRowsHandled() const
	{
		return static_cast<std::size_t>(_rResult.size());
	}

	std::size_t numOfRowsAllowed() const
	{
		return getLimit();
	}

	std::size_t extract(std::size_t pos)
	{
		V tmp;
		TypeHandler<V>::extract(pos, tmp, _default, getExtractor());
		_rResult.insert(std::make_pair(tmp(), tmp));
		return 1u;
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t pos)
	{
		return new Preparation<V>(pPrep, pos, _default);
	}

private:
	std::multimap<K, V>& _rResult;
	V                    _default;
};


namespace Keywords {


template <typename T> 
inline AbstractExtraction::Ptr into(T& t)
	/// Convenience function to allow for a more compact creation of an extraction object.
{
	return new Extraction<T>(t);
}


template <typename T> 
inline AbstractExtraction::Ptr into(T& t, const Position& pos)
	/// Convenience function to allow for a more compact creation of an extraction object
	/// with multiple recordset support.
{
	return new Extraction<T>(t, pos);
}


template <typename T> 
inline AbstractExtraction::Ptr into(T& t, const Position& pos, const T& def)
	/// Convenience function to allow for a more compact creation of an extraction object 
	/// with multiple recordset support and the given default
{
	return new Extraction<T>(t, def, pos);
}


inline AbstractExtractionVecVec& into(AbstractExtractionVecVec& evv)
	/// Convenience dummy function (for syntax purposes only).
{
	return evv;
}


inline AbstractExtractionVec& into(AbstractExtractionVec& ev)
	/// Convenience dummy function (for syntax purposes only).
{
	return ev;
}


} // namespace Keywords


} } // namespace Poco::Data


#endif // Data_Extraction_INCLUDED
