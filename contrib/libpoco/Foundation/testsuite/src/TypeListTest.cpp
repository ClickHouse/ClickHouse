//
// TypeListTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TypeListTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TypeListTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Tuple.h"
#include "Poco/TypeList.h"
#include "Poco/Void.h"
#include <iostream>

GCC_DIAG_OFF(unused-but-set-variable)
#if defined(_MSC_VER)
#	pragma warning(disable:4800) // forcing value to bool 'true' or 'false' on MSVC 71
#endif

using Poco::TypeList;
using Poco::Tuple;
using Poco::NullTypeList;
using Poco::TypeListType;
using Poco::TypeGetter;
using Poco::TypeLocator;
using Poco::TypeAppender;
using Poco::TypeOneEraser;
using Poco::TypeAllEraser;
using Poco::TypeDuplicateEraser;
using Poco::TypeOneReplacer;
using Poco::TypeAllReplacer;
using Poco::Int8;
using Poco::UInt8;
using Poco::Int16;
using Poco::UInt16;
using Poco::Int32;
using Poco::UInt32;
using Poco::Int8;
using Poco::UInt8;
using Poco::Int16;
using Poco::UInt16;
using Poco::Int32;
using Poco::UInt32;
using Poco::Void;


TypeListTest::TypeListTest(const std::string& name): CppUnit::TestCase(name)
{
}


TypeListTest::~TypeListTest()
{
}


void TypeListTest::testTypeList()
{
	typedef TypeListType<Int8,
		UInt8,
		Int16,
		UInt16,
		Int32,
		UInt32,
		float,
		double,
		Int8,
		UInt8,
		Int16,
		UInt16,
		Int32,
		UInt32,
		float>::HeadType Type15;

	Tuple<TypeGetter<0, Type15>::HeadType,
		TypeGetter<1, Type15>::HeadType,
		TypeGetter<2, Type15>::HeadType,
		TypeGetter<3, Type15>::HeadType,
		TypeGetter<4, Type15>::HeadType,
		TypeGetter<5, Type15>::HeadType,
		TypeGetter<6, Type15>::HeadType,
		TypeGetter<7, Type15>::HeadType,
		TypeGetter<8, Type15>::HeadType,
		TypeGetter<9, Type15>::HeadType> tuple;

	static TypeLocator<Type15, Int8> pos0;
	static TypeLocator<Type15, UInt8> pos1;
	static TypeLocator<Type15, Int16> pos2;
	static TypeLocator<Type15, UInt16> pos3;
	static TypeLocator<Type15, Int32> pos4;
	static TypeLocator<Type15, UInt32> pos5;
	static TypeLocator<Type15, float> pos6;
	static TypeLocator<Type15, double> pos7;
	static TypeLocator<Type15, Int8> pos8;
	static TypeLocator<Type15, std::string> posUnknown;

	assert (pos0.value == 0);
	assert (pos1.value == 1);
	assert (pos2.value == 2);
	assert (pos3.value == 3);
	assert (pos4.value == 4);
	assert (pos5.value == 5);
	assert (pos6.value == 6);
	assert (pos7.value == 7);
	assert (pos8.value == 0);
	assert (posUnknown.value == -1);

	tuple.set<TypeLocator<Type15, Int32>::value >(-123);
	assert (-123 == tuple.get<4>());

	assert (typeid(TypeGetter<0, Type15>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<0, Type15>::ConstHeadType) == typeid(const Int8));
	assert (typeid(TypeGetter<1, Type15>::HeadType) == typeid(UInt8));
	assert (typeid(TypeGetter<1, Type15>::ConstHeadType) == typeid(const UInt8));
	assert (typeid(TypeGetter<2, Type15>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<2, Type15>::ConstHeadType) == typeid(const Int16));
	assert (typeid(TypeGetter<3, Type15>::HeadType) == typeid(UInt16));
	assert (typeid(TypeGetter<3, Type15>::ConstHeadType) == typeid(const UInt16));
	assert (typeid(TypeGetter<4, Type15>::HeadType) == typeid(Int32));
	assert (typeid(TypeGetter<4, Type15>::ConstHeadType) == typeid(const Int32));
	assert (typeid(TypeGetter<5, Type15>::HeadType) == typeid(UInt32));
	assert (typeid(TypeGetter<5, Type15>::ConstHeadType) == typeid(const UInt32));
	assert (typeid(TypeGetter<6, Type15>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<6, Type15>::ConstHeadType) == typeid(const float));
	assert (typeid(TypeGetter<7, Type15>::HeadType) == typeid(double));
	assert (typeid(TypeGetter<7, Type15>::ConstHeadType) == typeid(const double));
	assert (typeid(TypeGetter<8, Type15>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<8, Type15>::ConstHeadType) == typeid(const Int8));
	assert (typeid(TypeGetter<9, Type15>::HeadType) == typeid(UInt8));
	assert (typeid(TypeGetter<9, Type15>::ConstHeadType) == typeid(const UInt8));
	assert (typeid(TypeGetter<10, Type15>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<10, Type15>::ConstHeadType) == typeid(const Int16));
	assert (typeid(TypeGetter<11, Type15>::HeadType) == typeid(UInt16));
	assert (typeid(TypeGetter<11, Type15>::ConstHeadType) == typeid(const UInt16));
	assert (typeid(TypeGetter<12, Type15>::HeadType) == typeid(Int32));
	assert (typeid(TypeGetter<12, Type15>::ConstHeadType) == typeid(const Int32));
	assert (typeid(TypeGetter<13, Type15>::HeadType) == typeid(UInt32));
	assert (typeid(TypeGetter<13, Type15>::ConstHeadType) == typeid(const UInt32));
	assert (typeid(TypeGetter<14, Type15>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<14, Type15>::ConstHeadType) == typeid(const float));

	typedef TypeListType<Int8>::HeadType Type1;
	assert (1 == Type1::length);
	typedef TypeListType<Int16, Int32>::HeadType Type2;
	assert (2 == Type2::length);
	typedef TypeAppender<Type1, Type2>::HeadType Type3;
	assert (3 == Type3::length);

	assert (typeid(TypeGetter<0, Type3>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<1, Type3>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<2, Type3>::HeadType) == typeid(Int32));

	static TypeLocator<Type3, Int8> posNo1;
	static TypeLocator<Type3, Int16> posNo2;
	static TypeLocator<Type3, Int32> posNo3;

	assert (posNo1.value == 0);
	assert (posNo2.value == 1);
	assert (posNo3.value == 2);

	typedef TypeOneEraser<Type3, Int8>::HeadType TypeEraser1;
	assert (2 == TypeEraser1::length);
	assert (typeid(TypeGetter<0, TypeEraser1>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<1, TypeEraser1>::HeadType) == typeid(Int32));

	typedef TypeOneEraser<Type3, Int16>::HeadType TypeEraser2;
	assert (2 == TypeEraser2::length);
	assert (typeid(TypeGetter<0, TypeEraser2>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<1, TypeEraser2>::HeadType) == typeid(Int32));

	typedef TypeOneEraser<Type3, Int32>::HeadType TypeEraser3;
	assert (2 == TypeEraser3::length);
	assert (typeid(TypeGetter<0, TypeEraser3>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<1, TypeEraser3>::HeadType) == typeid(Int16));

	typedef TypeListType<Int8,Int16,Int8,Int16,Int8>::HeadType Type5;
	typedef TypeAllEraser<Type5, Int8>::HeadType TypeAllEraser3;
	assert (2 == TypeAllEraser3::length);
	assert (typeid(TypeGetter<0, TypeAllEraser3>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<1, TypeAllEraser3>::HeadType) == typeid(Int16));

	typedef TypeDuplicateEraser<Type5>::HeadType TypeDuplicateEraser1;
	assert (2 == TypeDuplicateEraser1::length);
	assert (typeid(TypeGetter<0, TypeDuplicateEraser1>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<1, TypeDuplicateEraser1>::HeadType) == typeid(Int16));

	typedef TypeOneReplacer<Type5, Int8, Int32>::HeadType TypeOneReplacer1;
	assert (5 == TypeOneReplacer1::length);
	assert (typeid(TypeGetter<0, TypeOneReplacer1>::HeadType) == typeid(Int32));
	assert (typeid(TypeGetter<1, TypeOneReplacer1>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<2, TypeOneReplacer1>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<3, TypeOneReplacer1>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<4, TypeOneReplacer1>::HeadType) == typeid(Int8));

	typedef TypeAllReplacer<Type5, Int8, Int32>::HeadType TypeAllReplacer1;
	assert (5 == TypeAllReplacer1::length);
	assert (typeid(TypeGetter<0, TypeAllReplacer1>::HeadType) == typeid(Int32));
	assert (typeid(TypeGetter<1, TypeAllReplacer1>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<2, TypeAllReplacer1>::HeadType) == typeid(Int32));
	assert (typeid(TypeGetter<3, TypeAllReplacer1>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<4, TypeAllReplacer1>::HeadType) == typeid(Int32));

	typedef TypeListType<Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void,Void>::HeadType TypeVoid;
	assert (typeid(TypeGetter<0, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<1, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<2, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<3, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<4, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<5, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<6, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<7, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<8, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<9, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<10, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<11, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<12, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<13, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<14, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<15, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<16, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<17, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<18, TypeVoid>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<19, TypeVoid>::HeadType) == typeid(Void));
	

	typedef TypeOneReplacer<TypeVoid, Void, Int8>::HeadType TypeFirstReplacer;
	assert (typeid(TypeGetter<0, TypeFirstReplacer>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<1, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<2, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<3, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<4, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<5, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<6, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<7, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<8, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<9, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<10, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<11, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<12, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<13, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<14, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<15, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<16, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<17, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<18, TypeFirstReplacer>::HeadType) == typeid(Void));
	assert (typeid(TypeGetter<19, TypeFirstReplacer>::HeadType) == typeid(Void));

	typedef TypeOneReplacer<TypeFirstReplacer, Void, Int16>::HeadType TypeSecondReplacer;
	assert (typeid(TypeGetter<0, TypeSecondReplacer>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<1, TypeSecondReplacer>::HeadType) == typeid(Int16));

	typedef TypeOneReplacer<TypeSecondReplacer, Void, Int32>::HeadType TypeThirdReplacer;
	assert (typeid(TypeGetter<0, TypeThirdReplacer>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<1, TypeThirdReplacer>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<2, TypeThirdReplacer>::HeadType) == typeid(Int32));


	typedef TypeAllReplacer<TypeThirdReplacer, Void, float>::HeadType TypeFourthReplacer;
	assert (typeid(TypeGetter<0, TypeFourthReplacer>::HeadType) == typeid(Int8));
	assert (typeid(TypeGetter<1, TypeFourthReplacer>::HeadType) == typeid(Int16));
	assert (typeid(TypeGetter<2, TypeFourthReplacer>::HeadType) == typeid(Int32));
	assert (typeid(TypeGetter<3, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<4, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<5, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<6, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<7, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<8, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<9, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<10, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<11, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<12, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<13, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<14, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<15, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<16, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<17, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<18, TypeFourthReplacer>::HeadType) == typeid(float));
	assert (typeid(TypeGetter<19, TypeFourthReplacer>::HeadType) == typeid(float));
}


void TypeListTest::setUp()
{
}


void TypeListTest::tearDown()
{
}


CppUnit::Test* TypeListTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TypeListTest");

	CppUnit_addTest(pSuite, TypeListTest, testTypeList);

	return pSuite;
}
