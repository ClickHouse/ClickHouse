//
// NamedTuple.h
//
// Library: Foundation
// Package: Core
// Module:  NamedTuple
//
// Definition of the NamedTuple class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NamedTuple_INCLUDED
#define Foundation_NamedTuple_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Tuple.h"
#include "Poco/TypeList.h"
#include "Poco/DynamicAny.h"
#include "Poco/SharedPtr.h"
#include "Poco/Format.h"


namespace Poco {


template<class T0, 
	class T1 = NullTypeList,
	class T2 = NullTypeList,
	class T3 = NullTypeList,
	class T4 = NullTypeList,
	class T5 = NullTypeList,
	class T6 = NullTypeList,
	class T7 = NullTypeList, 
	class T8 = NullTypeList,
	class T9 = NullTypeList,
	class T10 = NullTypeList,
	class T11 = NullTypeList,
	class T12 = NullTypeList,
	class T13 = NullTypeList,
	class T14 = NullTypeList,
	class T15 = NullTypeList,
	class T16 = NullTypeList,
	class T17 = NullTypeList,
	class T18 = NullTypeList,
	class T19 = NullTypeList>
struct NamedTuple: public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::Type Type;
	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15), 
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16), 
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17), 
		typename TypeWrapper<T18>::CONSTTYPE& t18 = POCO_TYPEWRAPPER_DEFAULTVALUE(T18), 
		typename TypeWrapper<T19>::CONSTTYPE& t19 = POCO_TYPEWRAPPER_DEFAULTVALUE(T19)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18,t19), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15), 
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16), 
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17), 
		typename TypeWrapper<T18>::CONSTTYPE& t18 = POCO_TYPEWRAPPER_DEFAULTVALUE(T18), 
		typename TypeWrapper<T19>::CONSTTYPE& t19 = POCO_TYPEWRAPPER_DEFAULTVALUE(T19)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18,t19)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11),
		const std::string& n12 = "M",
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12),
		const std::string& n13 = "N",
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13),
		const std::string& n14 = "O",
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14),
		const std::string& n15 = "P",
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15),
		const std::string& n16 = "Q",
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16),
		const std::string& n17 = "R",
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17),
		const std::string& n18 = "S",
		typename TypeWrapper<T18>::CONSTTYPE& t18 = POCO_TYPEWRAPPER_DEFAULTVALUE(T18),
		const std::string& n19 = "T",
		typename TypeWrapper<T19>::CONSTTYPE& t19 = POCO_TYPEWRAPPER_DEFAULTVALUE(T19)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18,t19), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17,n18,n19);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					case 12: return TupleType::template get<12>();
					case 13: return TupleType::template get<13>();
					case 14: return TupleType::template get<14>();
					case 15: return TupleType::template get<15>();
					case 16: return TupleType::template get<16>();
					case 17: return TupleType::template get<17>();
					case 18: return TupleType::template get<18>();
					case 19: return TupleType::template get<19>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L",
		const std::string& n12 = "M",
		const std::string& n13 = "N",
		const std::string& n14 = "O",
		const std::string& n15 = "P",
		const std::string& n16 = "Q",
		const std::string& n17 = "R",
		const std::string& n18 = "S",
		const std::string& n19 = "T")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
			_pNames->push_back(n12); 
			_pNames->push_back(n13); 
			_pNames->push_back(n14); 
			_pNames->push_back(n15); 
			_pNames->push_back(n16); 
			_pNames->push_back(n17); 
			_pNames->push_back(n18); 
			_pNames->push_back(n19); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10,
	class T11,
	class T12,
	class T13,
	class T14,
	class T15,
	class T16,
	class T17,
	class T18>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::Type Type;
	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15), 
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16), 
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17), 
		typename TypeWrapper<T18>::CONSTTYPE& t18 = POCO_TYPEWRAPPER_DEFAULTVALUE(T18)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15), 
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16), 
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17), 
		typename TypeWrapper<T18>::CONSTTYPE& t18 = POCO_TYPEWRAPPER_DEFAULTVALUE(T18)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11),
		const std::string& n12 = "M",
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12),
		const std::string& n13 = "N",
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13),
		const std::string& n14 = "O",
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14),
		const std::string& n15 = "P",
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15),
		const std::string& n16 = "Q",
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16),
		const std::string& n17 = "R",
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17),
		const std::string& n18 = "S",
		typename TypeWrapper<T18>::CONSTTYPE& t18 = POCO_TYPEWRAPPER_DEFAULTVALUE(T18)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17,n18);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					case 12: return TupleType::template get<12>();
					case 13: return TupleType::template get<13>();
					case 14: return TupleType::template get<14>();
					case 15: return TupleType::template get<15>();
					case 16: return TupleType::template get<16>();
					case 17: return TupleType::template get<17>();
					case 18: return TupleType::template get<18>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L",
		const std::string& n12 = "M",
		const std::string& n13 = "N",
		const std::string& n14 = "O",
		const std::string& n15 = "P",
		const std::string& n16 = "Q",
		const std::string& n17 = "R",
		const std::string& n18 = "S")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
			_pNames->push_back(n12); 
			_pNames->push_back(n13); 
			_pNames->push_back(n14); 
			_pNames->push_back(n15); 
			_pNames->push_back(n16); 
			_pNames->push_back(n17); 
			_pNames->push_back(n18); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10,
	class T11,
	class T12,
	class T13,
	class T14,
	class T15,
	class T16,
	class T17>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::Type Type;
	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15), 
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16), 
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15), 
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16), 
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11),
		const std::string& n12 = "M",
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12),
		const std::string& n13 = "N",
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13),
		const std::string& n14 = "O",
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14),
		const std::string& n15 = "P",
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15),
		const std::string& n16 = "Q",
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16),
		const std::string& n17 = "R",
		typename TypeWrapper<T17>::CONSTTYPE& t17 = POCO_TYPEWRAPPER_DEFAULTVALUE(T17)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					case 12: return TupleType::template get<12>();
					case 13: return TupleType::template get<13>();
					case 14: return TupleType::template get<14>();
					case 15: return TupleType::template get<15>();
					case 16: return TupleType::template get<16>();
					case 17: return TupleType::template get<17>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L",
		const std::string& n12 = "M",
		const std::string& n13 = "N",
		const std::string& n14 = "O",
		const std::string& n15 = "P",
		const std::string& n16 = "Q",
		const std::string& n17 = "R")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
			_pNames->push_back(n12); 
			_pNames->push_back(n13); 
			_pNames->push_back(n14); 
			_pNames->push_back(n15); 
			_pNames->push_back(n16); 
			_pNames->push_back(n17); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10,
	class T11,
	class T12,
	class T13,
	class T14,
	class T15,
	class T16>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::Type Type;
	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15), 
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15), 
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11),
		const std::string& n12 = "M",
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12),
		const std::string& n13 = "N",
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13),
		const std::string& n14 = "O",
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14),
		const std::string& n15 = "P",
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15),
		const std::string& n16 = "Q",
		typename TypeWrapper<T16>::CONSTTYPE& t16 = POCO_TYPEWRAPPER_DEFAULTVALUE(T16)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					case 12: return TupleType::template get<12>();
					case 13: return TupleType::template get<13>();
					case 14: return TupleType::template get<14>();
					case 15: return TupleType::template get<15>();
					case 16: return TupleType::template get<16>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L",
		const std::string& n12 = "M",
		const std::string& n13 = "N",
		const std::string& n14 = "O",
		const std::string& n15 = "P",
		const std::string& n16 = "Q")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
			_pNames->push_back(n12); 
			_pNames->push_back(n13); 
			_pNames->push_back(n14); 
			_pNames->push_back(n15); 
			_pNames->push_back(n16); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10,
	class T11,
	class T12,
	class T13,
	class T14,
	class T15>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14), 
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11),
		const std::string& n12 = "M",
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12),
		const std::string& n13 = "N",
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13),
		const std::string& n14 = "O",
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14),
		const std::string& n15 = "P",
		typename TypeWrapper<T15>::CONSTTYPE& t15 = POCO_TYPEWRAPPER_DEFAULTVALUE(T15)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					case 12: return TupleType::template get<12>();
					case 13: return TupleType::template get<13>();
					case 14: return TupleType::template get<14>();
					case 15: return TupleType::template get<15>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L",
		const std::string& n12 = "M",
		const std::string& n13 = "N",
		const std::string& n14 = "O",
		const std::string& n15 = "P")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
			_pNames->push_back(n12); 
			_pNames->push_back(n13); 
			_pNames->push_back(n14); 
			_pNames->push_back(n15); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10,
	class T11,
	class T12,
	class T13,
	class T14>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13), 
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11),
		const std::string& n12 = "M",
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12),
		const std::string& n13 = "N",
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13),
		const std::string& n14 = "O",
		typename TypeWrapper<T14>::CONSTTYPE& t14 = POCO_TYPEWRAPPER_DEFAULTVALUE(T14)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					case 12: return TupleType::template get<12>();
					case 13: return TupleType::template get<13>();
					case 14: return TupleType::template get<14>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L",
		const std::string& n12 = "M",
		const std::string& n13 = "N",
		const std::string& n14 = "O")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
			_pNames->push_back(n12); 
			_pNames->push_back(n13); 
			_pNames->push_back(n14); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10,
	class T11,
	class T12,
	class T13>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12), 
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11),
		const std::string& n12 = "M",
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12),
		const std::string& n13 = "N",
		typename TypeWrapper<T13>::CONSTTYPE& t13 = POCO_TYPEWRAPPER_DEFAULTVALUE(T13)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					case 12: return TupleType::template get<12>();
					case 13: return TupleType::template get<13>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L",
		const std::string& n12 = "M",
		const std::string& n13 = "N")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
			_pNames->push_back(n12); 
			_pNames->push_back(n13); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10,
	class T11,
	class T12>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11), 
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11),
		const std::string& n12 = "M",
		typename TypeWrapper<T12>::CONSTTYPE& t12 = POCO_TYPEWRAPPER_DEFAULTVALUE(T12)): 
	TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12), _pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					case 12: return TupleType::template get<12>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L",
		const std::string& n12 = "M")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
			_pNames->push_back(n12); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10,
	class T11>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10), 
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10),
		const std::string& n11 = "L",
		typename TypeWrapper<T11>::CONSTTYPE& t11 = POCO_TYPEWRAPPER_DEFAULTVALUE(T11)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					case 11: return TupleType::template get<11>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K",
		const std::string& n11 = "L")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
			_pNames->push_back(n11); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9,
	class T10>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9), 
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9),
		const std::string& n10 = "K",
		typename TypeWrapper<T10>::CONSTTYPE& t10 = POCO_TYPEWRAPPER_DEFAULTVALUE(T10)): 
	TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10), _pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					case 10: return TupleType::template get<10>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J",
		const std::string& n10 = "K")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
			_pNames->push_back(n10); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8,
	class T9>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8), 
		const std::string& n9 = "J",
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8,t9), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8,n9);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					case 9: return TupleType::template get<9>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I",
		const std::string& n9 = "J")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
			_pNames->push_back(n9); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7, 
	class T8>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		const std::string& n8 = "I",
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7,t8), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7,n8);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					case 8: return TupleType::template get<8>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H",
		const std::string& n8 = "I")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
			_pNames->push_back(n8);
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,T7,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6,T7>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6,T7> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6,T7>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6), 
		const std::string& n7 = "H",
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7)): 
		TupleType(t0,t1,t2,t3,t4,t5,t6,t7), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6,n7);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					case 7: return TupleType::template get<7>(); 
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G",
		const std::string& n7 = "H")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
			_pNames->push_back(n7);
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6>
struct NamedTuple<T0,T1,T2,T3,T4,T5,T6,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5,T6>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5,T6> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5,T6>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6)): 
	TupleType(t0,t1,t2,t3,t4,t5,t6), _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5), 
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6)): 
	TupleType(t0,t1,t2,t3,t4,t5,t6)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		const std::string& n6 = "G",
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6)): 
	TupleType(t0,t1,t2,t3,t4,t5,t6), _pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5,n6);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					case 6: return TupleType::template get<6>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F", 
		const std::string& n6 = "G")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
			_pNames->push_back(n6);
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4,
	class T5>
struct NamedTuple<T0,T1,T2,T3,T4,T5,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4,T5>
{
	typedef Tuple<T0,T1,T2,T3,T4,T5> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4,T5>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5)): 
	TupleType(t0,t1,t2,t3,t4,t5), _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5)): 
	TupleType(t0,t1,t2,t3,t4,t5)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4), 
		const std::string& n5 = "F",
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5)): 
	TupleType(t0,t1,t2,t3,t4,t5), _pNames(0) 
	{
		init(n0,n1,n2,n3,n4,n5);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					case 5: return TupleType::template get<5>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E",
		const std::string& n5 = "F")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
			_pNames->push_back(n5);
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3,
	class T4>
struct NamedTuple<T0,T1,T2,T3,T4,NullTypeList>:
	public Tuple<T0,T1,T2,T3,T4>
{
	typedef Tuple<T0,T1,T2,T3,T4> TupleType;
	typedef typename Tuple<T0,T1,T2,T3,T4>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4)): 
		TupleType(t0,t1,t2,t3,t4), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4)): 
		TupleType(t0,t1,t2,t3,t4)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		const std::string& n4 = "E",
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4)): 
		TupleType(t0,t1,t2,t3,t4), 
		_pNames(0) 
	{
		init(n0,n1,n2,n3,n4);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					case 4: return TupleType::template get<4>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D",
		const std::string& n4 = "E")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
			_pNames->push_back(n4);
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2,
	class T3>
struct NamedTuple<T0,T1,T2,T3,NullTypeList>:
	public Tuple<T0,T1,T2,T3>
{
	typedef Tuple<T0,T1,T2,T3> TupleType;
	typedef typename Tuple<T0,T1,T2,T3>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3)): 
		TupleType(t0,t1,t2,t3), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3)): 
		TupleType(t0,t1,t2,t3)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2), 
		const std::string& n3 = "D",
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3)): 
	TupleType(t0,t1,t2,t3), _pNames(0) 
	{
		init(n0,n1,n2,n3);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					case 3: return TupleType::template get<3>(); 
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C",
		const std::string& n3 = "D")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
			_pNames->push_back(n3); 
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1,
	class T2>
struct NamedTuple<T0,T1,T2,NullTypeList>:
	public Tuple<T0,T1,T2>
{
	typedef Tuple<T0,T1,T2> TupleType;
	typedef typename Tuple<T0,T1,T2>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1), 
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2)): 
		TupleType(t0,t1,t2), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2)): 
		TupleType(t0,t1,t2)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		const std::string& n2 = "C",
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2)): 
		TupleType(t0,t1,t2), 
		_pNames(0) 
	{
		init(n0,n1,n2);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					case 2: return TupleType::template get<2>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B",
		const std::string& n2 = "C")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
			_pNames->push_back(n2);
		}
	}

	NameVecPtr _pNames;
};


template<class T0, 
	class T1>
struct NamedTuple<T0,T1,NullTypeList>:
	public Tuple<T0,T1>
{
	typedef Tuple<T0,T1> TupleType;
	typedef typename Tuple<T0,T1>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1)): 
		TupleType(t0,t1), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0,
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1)): 
		TupleType(t0,t1)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0,
		typename TypeWrapper<T0>::CONSTTYPE& t0, 
		const std::string& n1 = "B",
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1)): 
		TupleType(t0,t1), 
		_pNames(0) 
	{
		init(n0,n1);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					case 1: return TupleType::template get<1>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A",
		const std::string& n1 = "B")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
			_pNames->push_back(n1);
		}
	}

	NameVecPtr _pNames;
};


template<class T0>
struct NamedTuple<T0,NullTypeList>:
	public Tuple<T0>
{
	typedef Tuple<T0> TupleType;
	typedef typename Tuple<T0>::Type Type;

	typedef std::vector<std::string> NameVec; 
	typedef SharedPtr<NameVec> NameVecPtr; 

	NamedTuple(): _pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames) 
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(typename TypeWrapper<T0>::CONSTTYPE& t0): 
		TupleType(t0), 
		_pNames(0)
	{
		init();
	}

	NamedTuple(const NameVecPtr& rNames, 
		typename TypeWrapper<T0>::CONSTTYPE& t0): 
		TupleType(t0)
	{
		if (rNames->size() != TupleType::length)
			throw InvalidArgumentException("Wrong names vector length."); 

		_pNames = rNames;
	}

	NamedTuple(const std::string& n0, typename TypeWrapper<T0>::CONSTTYPE& t0): 
		TupleType(t0), 
		_pNames(0) 
	{
		init(n0);
	}

	const DynamicAny get(const std::string& name) const
	{
		NameVec::const_iterator it = _pNames->begin(); 
		NameVec::const_iterator itEnd = _pNames->end();

		for(std::size_t counter = 0; it != itEnd; ++it, ++counter)
		{
			if (name == *it)
			{
				switch (counter)
				{ 
					case 0: return TupleType::template get<0>();
					default: throw RangeException();
				}
			}
		} 

		throw NotFoundException("Name not found: " + name);
	}

	const DynamicAny operator [] (const std::string& name) const
	{
		return get(name);
	}

	template<int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const 
	{
		return TupleType::template get<N>();
	}

	template<int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return TupleType::template get<N>();
	}

	template<int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		return TupleType::template set<N>(val);
	}

	const NameVecPtr& names()
	{
		return _pNames; 
	}

	void setName(std::size_t index, const std::string& name)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		(*_pNames)[index] = name;
	}

	const std::string& getName(std::size_t index)
	{
		if (index >= _pNames->size())
			throw InvalidArgumentException(format("Invalid index: %z", index));

		return (*_pNames)[index];
	}

	bool operator == (const NamedTuple& other) const
	{
		return TupleType(*this) == TupleType(other) && _pNames == other._pNames;
	}

	bool operator != (const NamedTuple& other) const 
	{
		return !(*this == other);
	}

	bool operator < (const NamedTuple& other) const
	{
		TupleType th(*this);
		TupleType oth(other); 

		return (th < oth && _pNames == other._pNames) || 
			(th == oth && _pNames < other._pNames) ||
			(th < oth && _pNames < other._pNames);
	}

private:
	void init(const std::string& n0 = "A")
	{ 
		if (!_pNames)
		{
			_pNames = new NameVec;
			_pNames->push_back(n0);
		}
	}

	NameVecPtr _pNames;
};


} // namespace Poco


#endif // Foundation_Tuple_INCLUDED
