//
// Tuple.h
//
// Library: Foundation
// Package: Core
// Module:  Tuple
//
// Definition of the Tuple class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Tuple_INCLUDED
#define Foundation_Tuple_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/TypeList.h"


namespace Poco {


#if defined(_MSC_VER) 
#define POCO_TYPEWRAPPER_DEFAULTVALUE(T) TypeWrapper<T>::TYPE()
#else
#define POCO_TYPEWRAPPER_DEFAULTVALUE(T) typename TypeWrapper<T>::TYPE()
#endif


template <class T0,
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
struct Tuple
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0,
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t8, typename TypeListType<T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t9, typename TypeListType<T10,T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t10, typename TypeListType<T11,T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t11, typename TypeListType<T12,T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t12, typename TypeListType<T13,T14,T15,T16,T17,T18,T19>::HeadType
			(t13, typename TypeListType<T14,T15,T16,T17,T18,T19>::HeadType
			(t14, typename TypeListType<T15,T16,T17,T18,T19>::HeadType
			(t15, typename TypeListType<T16,T17,T18,T19>::HeadType
			(t16, typename TypeListType<T17,T18,T19>::HeadType
			(t17, typename TypeListType<T18,T19>::HeadType
			(t18, typename TypeListType<T19>::HeadType
			(t19, NullTypeList()))))))))))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t8, typename TypeListType<T9,T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t9, typename TypeListType<T10,T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t10, typename TypeListType<T11,T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t11, typename TypeListType<T12,T13,T14,T15,T16,T17,T18>::HeadType
			(t12, typename TypeListType<T13,T14,T15,T16,T17,T18>::HeadType
			(t13, typename TypeListType<T14,T15,T16,T17,T18>::HeadType
			(t14, typename TypeListType<T15,T16,T17,T18>::HeadType
			(t15, typename TypeListType<T16,T17,T18>::HeadType
			(t16, typename TypeListType<T17,T18>::HeadType
			(t17, typename TypeListType<T18>::HeadType
			(t18, NullTypeList())))))))))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t8, typename TypeListType<T9,T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t9, typename TypeListType<T10,T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t10, typename TypeListType<T11,T12,T13,T14,T15,T16,T17>::HeadType
			(t11, typename TypeListType<T12,T13,T14,T15,T16,T17>::HeadType
			(t12, typename TypeListType<T13,T14,T15,T16,T17>::HeadType
			(t13, typename TypeListType<T14,T15,T16,T17>::HeadType
			(t14, typename TypeListType<T15,T16,T17>::HeadType
			(t15, typename TypeListType<T16,T17>::HeadType
			(t16, typename TypeListType<T17>::HeadType
			(t17, NullTypeList()))))))))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t8, typename TypeListType<T9,T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t9, typename TypeListType<T10,T11,T12,T13,T14,T15,T16>::HeadType
			(t10, typename TypeListType<T11,T12,T13,T14,T15,T16>::HeadType
			(t11, typename TypeListType<T12,T13,T14,T15,T16>::HeadType
			(t12, typename TypeListType<T13,T14,T15,T16>::HeadType
			(t13, typename TypeListType<T14,T15,T16>::HeadType
			(t14, typename TypeListType<T15,T16>::HeadType
			(t15, typename TypeListType<T16>::HeadType
			(t16, NullTypeList())))))))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t8, typename TypeListType<T9,T10,T11,T12,T13,T14,T15>::HeadType
			(t9, typename TypeListType<T10,T11,T12,T13,T14,T15>::HeadType
			(t10, typename TypeListType<T11,T12,T13,T14,T15>::HeadType
			(t11, typename TypeListType<T12,T13,T14,T15>::HeadType
			(t12, typename TypeListType<T13,T14,T15>::HeadType
			(t13, typename TypeListType<T14,T15>::HeadType
			(t14, typename TypeListType<T15>::HeadType
			(t15, NullTypeList()))))))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11,T12,T13,T14>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11,T12,T13,T14>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11,T12,T13,T14>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11,T12,T13,T14>::HeadType
			(t8, typename TypeListType<T9,T10,T11,T12,T13,T14>::HeadType
			(t9, typename TypeListType<T10,T11,T12,T13,T14>::HeadType
			(t10, typename TypeListType<T11,T12,T13,T14>::HeadType
			(t11, typename TypeListType<T12,T13,T14>::HeadType
			(t12, typename TypeListType<T13,T14>::HeadType
			(t13, typename TypeListType<T14>::HeadType
			(t14, NullTypeList())))))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11,T12,T13>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11,T12,T13>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11,T12,T13>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11,T12,T13>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11,T12,T13>::HeadType
			(t8, typename TypeListType<T9,T10,T11,T12,T13>::HeadType
			(t9, typename TypeListType<T10,T11,T12,T13>::HeadType
			(t10, typename TypeListType<T11,T12,T13>::HeadType
			(t11, typename TypeListType<T12,T13>::HeadType
			(t12, typename TypeListType<T13>::HeadType
			(t13, NullTypeList()))))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11,T12>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11,T12>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11,T12>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11,T12>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11,T12>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11,T12>::HeadType
			(t8, typename TypeListType<T9,T10,T11,T12>::HeadType
			(t9, typename TypeListType<T10,T11,T12>::HeadType
			(t10, typename TypeListType<T11,T12>::HeadType
			(t11, typename TypeListType<T12>::HeadType
			(t12, NullTypeList())))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10,T11>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10,T11>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10,T11>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10,T11>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10,T11>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10,T11>::HeadType
			(t7, typename TypeListType<T8,T9,T10,T11>::HeadType
			(t8, typename TypeListType<T9,T10,T11>::HeadType
			(t9, typename TypeListType<T10,T11>::HeadType
			(t10, typename TypeListType<T11>::HeadType
			(t11, NullTypeList()))))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
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
struct Tuple<T0, T1,T2,T3,T4,T5,T6,T7,T8,T9,T10, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
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
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9,T10>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9,T10>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9,T10>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9,T10>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9,T10>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9,T10>::HeadType
			(t6, typename TypeListType<T7,T8,T9,T10>::HeadType
			(t7, typename TypeListType<T8,T9,T10>::HeadType
			(t8, typename TypeListType<T9,T10>::HeadType
			(t9, typename TypeListType<T10>::HeadType
			(t10, NullTypeList())))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7,
	class T8,
	class T9>
struct Tuple<T0, T1,T2,T3,T4,T5,T6,T7,T8,T9, NullTypeList>
{
	typedef typename TypeListType<T0, T1,T2,T3,T4,T5,T6,T7,T8,T9>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2),
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8),
		typename TypeWrapper<T9>::CONSTTYPE& t9 = POCO_TYPEWRAPPER_DEFAULTVALUE(T9)):
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8,T9>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8,T9>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8,T9>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8,T9>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8,T9>::HeadType
			(t5, typename TypeListType<T6,T7,T8,T9>::HeadType
			(t6, typename TypeListType<T7,T8,T9>::HeadType
			(t7, typename TypeListType<T8,T9>::HeadType
			(t8, typename TypeListType<T9>::HeadType
			(t9, NullTypeList()))))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7,
	class T8>
struct Tuple<T0, T1,T2,T3,T4,T5,T6,T7,T8, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7,T8>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2),
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7),
		typename TypeWrapper<T8>::CONSTTYPE& t8 = POCO_TYPEWRAPPER_DEFAULTVALUE(T8)):
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7,T8>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7,T8>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7,T8>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7,T8>::HeadType
			(t4, typename TypeListType<T5,T6,T7,T8>::HeadType
			(t5, typename TypeListType<T6,T7,T8>::HeadType
			(t6, typename TypeListType<T7,T8>::HeadType
			(t7, typename TypeListType<T8>::HeadType
			(t8, NullTypeList())))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6,
	class T7>
struct Tuple<T0, T1,T2,T3,T4,T5,T6,T7, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6,T7>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2),
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6),
		typename TypeWrapper<T7>::CONSTTYPE& t7 = POCO_TYPEWRAPPER_DEFAULTVALUE(T7)):
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6,T7>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6,T7>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6,T7>::HeadType
			(t3, typename TypeListType<T4,T5,T6,T7>::HeadType
			(t4, typename TypeListType<T5,T6,T7>::HeadType
			(t5, typename TypeListType<T6,T7>::HeadType
			(t6, typename TypeListType<T7>::HeadType
			(t7, NullTypeList()))))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1,
	class T2,
	class T3,
	class T4,
	class T5,
	class T6>
struct Tuple<T0, T1,T2,T3,T4,T5,T6, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5,T6>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2),
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5),
		typename TypeWrapper<T6>::CONSTTYPE& t6 = POCO_TYPEWRAPPER_DEFAULTVALUE(T6)):
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5,T6>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5,T6>::HeadType
			(t2, typename TypeListType<T3,T4,T5,T6>::HeadType
			(t3, typename TypeListType<T4,T5,T6>::HeadType
			(t4, typename TypeListType<T5,T6>::HeadType
			(t5, typename TypeListType<T6>::HeadType
			(t6, NullTypeList())))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1,
	class T2,
	class T3,
	class T4,
	class T5>
struct Tuple<T0, T1,T2,T3,T4,T5, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4,T5>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2),
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4),
		typename TypeWrapper<T5>::CONSTTYPE& t5 = POCO_TYPEWRAPPER_DEFAULTVALUE(T5)):
		_data(t0, typename TypeListType<T1,T2,T3,T4,T5>::HeadType
			(t1, typename TypeListType<T2,T3,T4,T5>::HeadType
			(t2, typename TypeListType<T3,T4,T5>::HeadType
			(t3, typename TypeListType<T4,T5>::HeadType
			(t4, typename TypeListType<T5>::HeadType
			(t5, NullTypeList()))))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1,
	class T2,
	class T3,
	class T4>
struct Tuple<T0, T1,T2,T3,T4, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3,T4>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2),
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3),
		typename TypeWrapper<T4>::CONSTTYPE& t4 = POCO_TYPEWRAPPER_DEFAULTVALUE(T4)):
		_data(t0, typename TypeListType<T1,T2,T3,T4>::HeadType
			(t1, typename TypeListType<T2,T3,T4>::HeadType
			(t2, typename TypeListType<T3,T4>::HeadType
			(t3, typename TypeListType<T4>::HeadType
			(t4, NullTypeList())))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1,
	class T2,
	class T3>
struct Tuple<T0, T1,T2,T3, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2,T3>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2),
		typename TypeWrapper<T3>::CONSTTYPE& t3 = POCO_TYPEWRAPPER_DEFAULTVALUE(T3)):
		_data(t0, typename TypeListType<T1,T2,T3>::HeadType
			(t1, typename TypeListType<T2,T3>::HeadType
			(t2, typename TypeListType<T3>::HeadType
			(t3, NullTypeList()))))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1,
	class T2>
struct Tuple<T0, T1,T2, NullTypeList>
{
	typedef typename TypeListType<T0,T1,T2>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1),
		typename TypeWrapper<T2>::CONSTTYPE& t2 = POCO_TYPEWRAPPER_DEFAULTVALUE(T2)):
		_data(t0, typename TypeListType<T1,T2>::HeadType
			(t1, typename TypeListType<T2>::HeadType
			(t2, NullTypeList())))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0,
	class T1>
struct Tuple<T0, T1, NullTypeList>
{
	typedef typename TypeListType<T0,T1>::HeadType Type;

	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0, 
		typename TypeWrapper<T1>::CONSTTYPE& t1 = POCO_TYPEWRAPPER_DEFAULTVALUE(T1)):
		_data(t0, typename TypeListType<T1>::HeadType(t1, NullTypeList()))
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


template <class T0>
struct Tuple<T0, NullTypeList>
{
	typedef TypeList<T0, NullTypeList> Type;
	
	enum TupleLengthType
	{
		length = Type::length
	};

	Tuple():_data()
	{
	}

	Tuple(typename TypeWrapper<T0>::CONSTTYPE& t0):
		_data(t0, NullTypeList())
	{
	}

	template <int N>
	typename TypeGetter<N, Type>::ConstHeadType& get() const
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	typename TypeGetter<N, Type>::HeadType& get()
	{
		return Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data);
	}

	template <int N>
	void set(typename TypeGetter<N, Type>::ConstHeadType& val)
	{
		Getter<N>::template get<typename TypeGetter<N, Type>::HeadType, typename Type::HeadType, typename Type::TailType>(_data) = val;
	}

	bool operator == (const Tuple& other) const
	{
		return _data == other._data;
	}

	bool operator != (const Tuple& other) const
	{
		return !(_data == other._data);
	}

	bool operator < (const Tuple& other) const
	{
		return _data < other._data;
	}

private:
	Type _data;
};


} // namespace Poco


#endif // Foundation_Tuple_INCLUDED
