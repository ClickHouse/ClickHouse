//
// BulkExtraction.h
//
// Library: Data
// Package: DataCore
// Module:  BulkExtraction
//
// Definition of the BulkExtraction class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_BulkExtraction_INCLUDED
#define Data_BulkExtraction_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/AbstractExtraction.h"
#include "Poco/Data/Bulk.h"
#include "Poco/Data/Preparation.h"
#include <vector>


namespace Poco {
namespace Data {


template <class C>
class BulkExtraction: public AbstractExtraction
	/// Specialization for bulk extraction of values from a query result set.
	/// Bulk extraction support is provided only for following STL containers:
	/// - std::vector
	/// - std::deque
	/// - std::list
{
public:
	typedef C                       ValType;
	typedef typename C::value_type  CValType;
	typedef SharedPtr<ValType>      ValPtr;
	typedef BulkExtraction<ValType> Type;
	typedef SharedPtr<Type>         Ptr;

	BulkExtraction(C& result, Poco::UInt32 limit, const Position& pos = Position(0)): 
		AbstractExtraction(limit, pos.value(), true),
		_rResult(result), 
		_default()
	{
		if (static_cast<Poco::UInt32>(result.size()) != limit)
			result.resize(limit);
	}

	BulkExtraction(C& result, const CValType& def, Poco::UInt32 limit, const Position& pos = Position(0)): 
		AbstractExtraction(limit, pos.value(), true),
		_rResult(result), 
		_default(def)
	{
		if (static_cast<Poco::UInt32>(result.size()) != limit)
			result.resize(limit);
	}

	virtual ~BulkExtraction()
	{
	}

	std::size_t numOfColumnsHandled() const
	{
		return TypeHandler<C>::size();
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

	std::size_t extract(std::size_t col)
	{
		AbstractExtractor::Ptr pExt = getExtractor();
		TypeHandler<C>::extract(col, _rResult, _default, pExt);
		typename C::iterator it = _rResult.begin();
		typename C::iterator end = _rResult.end();
		for (int row = 0; it !=end; ++it, ++row)
		{
			_nulls.push_back(isValueNull(*it, pExt->isNull(col, row)));
		}

		return _rResult.size();
	}

	virtual void reset()
	{
	}

	AbstractPreparation::Ptr createPreparation(AbstractPreparator::Ptr& pPrep, std::size_t col)
	{
		Poco::UInt32 limit = getLimit();
		if (limit != _rResult.size()) _rResult.resize(limit);
		pPrep->setLength(limit);
		pPrep->setBulk(true);
		return new Preparation<C>(pPrep, col, _rResult);
	}

protected:
	const C& result() const
	{
		return _rResult;
	}

private:
	C&               _rResult;
	CValType         _default;
	std::deque<bool> _nulls;
};


template <class C>
class InternalBulkExtraction: public BulkExtraction<C>
	/// Container Data Type specialization extension for extraction of values from a query result set.
	///
	/// This class is intended for PocoData internal use - it is used by StatementImpl 
	/// to automaticaly create internal BulkExtraction in cases when statement returns data and no external storage
	/// was supplied. It is later used by RecordSet to retrieve the fetched data after statement execution.
	/// It takes ownership of the Column pointer supplied as constructor argument. Column object, in turn
	/// owns the data container pointer.
	///
	/// InternalBulkExtraction objects can not be copied or assigned.
{
public:
	typedef C                               ValType;
	typedef typename C::value_type          CValType;
	typedef SharedPtr<ValType>              ValPtr;
	typedef InternalBulkExtraction<ValType> Type;
	typedef SharedPtr<Type>                 Ptr;

	InternalBulkExtraction(C& result,
		Column<C>* pColumn,
		Poco::UInt32 limit,
		const Position& pos = Position(0)): 
		BulkExtraction<C>(result, CValType(), limit, pos), 
		_pColumn(pColumn)
		/// Creates InternalBulkExtraction.
	{
	}

	~InternalBulkExtraction()
		/// Destroys InternalBulkExtraction.
	{
		delete _pColumn;
	}

	void reset()
	{
		_pColumn->reset();
	}	

	const CValType& value(int index) const
	{
		try
		{ 
			return BulkExtraction<C>::result().at(index); 
		}
		catch (std::out_of_range& ex)
		{ 
			throw RangeException(ex.what()); 
		}
	}

	bool isNull(std::size_t row) const
	{
		return BulkExtraction<C>::isNull(row);
	}

	const Column<C>& column() const
	{
		return *_pColumn;
	}

private:
	InternalBulkExtraction();
	InternalBulkExtraction(const InternalBulkExtraction&);
	InternalBulkExtraction& operator = (const InternalBulkExtraction&);

	Column<C>* _pColumn;
};


namespace Keywords {


template <typename T> 
AbstractExtraction::Ptr into(std::vector<T>& t, const Bulk& bulk, const Position& pos = Position(0))
	/// Convenience function to allow for a more compact creation of an extraction object
	/// with std::vector bulk extraction support.
{
	return new BulkExtraction<std::vector<T> >(t, bulk.size(), pos);
}


template <typename T> 
AbstractExtraction::Ptr into(std::vector<T>& t, BulkFnType, const Position& pos = Position(0))
	/// Convenience function to allow for a more compact creation of an extraction object
	/// with std::vector bulk extraction support.
{
	Poco::UInt32 size = static_cast<Poco::UInt32>(t.size());
	if (0 == size) throw InvalidArgumentException("Zero length not allowed.");
	return new BulkExtraction<std::vector<T> >(t, size, pos);
}


template <typename T> 
AbstractExtraction::Ptr into(std::deque<T>& t, const Bulk& bulk, const Position& pos = Position(0))
	/// Convenience function to allow for a more compact creation of an extraction object
	/// with std::deque bulk extraction support.
{
	return new BulkExtraction<std::deque<T> >(t, bulk.size(), pos);
}


template <typename T> 
AbstractExtraction::Ptr into(std::deque<T>& t, BulkFnType, const Position& pos = Position(0))
	/// Convenience function to allow for a more compact creation of an extraction object
	/// with std::deque bulk extraction support.
{
	Poco::UInt32 size = static_cast<Poco::UInt32>(t.size());
	if (0 == size) throw InvalidArgumentException("Zero length not allowed.");
	return new BulkExtraction<std::deque<T> >(t, size, pos);
}


template <typename T> 
AbstractExtraction::Ptr into(std::list<T>& t, const Bulk& bulk, const Position& pos = Position(0))
	/// Convenience function to allow for a more compact creation of an extraction object
	/// with std::list bulk extraction support.
{
	return new BulkExtraction<std::list<T> >(t, bulk.size(), pos);
}


template <typename T> 
AbstractExtraction::Ptr into(std::list<T>& t, BulkFnType, const Position& pos = Position(0))
	/// Convenience function to allow for a more compact creation of an extraction object
	/// with std::list bulk extraction support.
{
	Poco::UInt32 size = static_cast<Poco::UInt32>(t.size());
	if (0 == size) throw InvalidArgumentException("Zero length not allowed.");
	return new BulkExtraction<std::list<T> >(t, size, pos);
}


} // namespace Keywords


} } // namespace Poco::Data


#endif // Data_BulkExtraction_INCLUDED
