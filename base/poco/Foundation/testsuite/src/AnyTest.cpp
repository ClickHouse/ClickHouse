//
// AnyTest.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "AnyTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Exception.h"
#include "Poco/Any.h"
#include "Poco/Bugcheck.h"
#include <vector>


#if defined(_MSC_VER) && _MSC_VER < 1400
	#pragma warning(disable:4800)//forcing value to bool 'true' or 'false'
#endif


using namespace Poco;


class SomeClass
{
public:
	int i;
	std::string str;
	SomeClass(int h, std::string s): i (h), str(s)
	{
	};
	bool operator==(const SomeClass& other) const
	{
		return i == other.i && str == other.str;
	}
};


AnyTest::AnyTest(const std::string& name): CppUnit::TestCase(name)
{
}


AnyTest::~AnyTest()
{
}


void AnyTest::testDefaultCtor()
{
	const Any value;
	
	assert (value.empty());
	assert (0 == AnyCast<int>(&value));
	assert (value.type() == typeid(void));
}


void AnyTest::testConvertingCtor()
{
	std::string text = "test message";
	Any value = text;
	
	assert (!value.empty());
	assert (value.type() == typeid(std::string));
	assert (0 == AnyCast<int>(&value));
	assert (0 != AnyCast<std::string>(&value));
	assert (AnyCast<std::string>(value) == text);
	assert (AnyCast<std::string>(&value) != &text);
}


void AnyTest::testCopyCtor()
{
	std::string text = "test message";
	Any original = text, copy = original;
	
	assert (!copy.empty());
	assert (original.type() == copy.type());
	assert (AnyCast<std::string>(original) == AnyCast<std::string>(copy));
	assert (text == AnyCast<std::string>(copy));
	assert (AnyCast<std::string>(&original) != AnyCast<std::string>(&copy));
}


void AnyTest::testCopyAssign()
{
	std::string text = "test message";
	Any original = text, copy;
	Any* assignResult = &(copy = original);
	
	assert (!copy.empty());
	assert (original.type() == copy.type());
	assert (AnyCast<std::string>(original) == AnyCast<std::string>(copy));
	assert (text == AnyCast<std::string>(copy));
	assert (AnyCast<std::string>(&original) != AnyCast<std::string>(&copy));
	assert (assignResult == &copy);

	// test self assignment
	Any& ref = original;
	original = ref;
	assert (AnyCast<std::string>(original) == AnyCast<std::string>(copy));
	original = original;
	assert (AnyCast<std::string>(original) == AnyCast<std::string>(copy));
}


void AnyTest::testConvertingAssign()
{
	std::string text = "test message";
	Any value;
	Any* assignResult = &(value = text);
	
	assert (!value.empty());
	assert (value.type() == typeid(std::string));
	assert (0 == AnyCast<int>(&value));
	assert (0 != AnyCast<std::string>(&value));
	assert (AnyCast<std::string>(value) == text);
	assert (AnyCast<std::string>(&value) != &text);
	assert (assignResult == &value);
}


void AnyTest::testCastToReference()
{
	Any a(137);
	const Any b(a);
	
	int&                ra    = AnyCast<int &>(a);
	int const&          ra_c  = AnyCast<int const &>(a);
	int volatile&       ra_v  = AnyCast<int volatile &>(a);
	int const volatile& ra_cv = AnyCast<int const volatile&>(a);
	
	// cv references to same obj
	assert (&ra == &ra_c && &ra == &ra_v && &ra == &ra_cv);
	
	int const &          rb_c  = AnyCast<int const &>(b);
	int const volatile & rb_cv = AnyCast<int const volatile &>(b);

	assert (&rb_c == &rb_cv); // cv references to copied const obj
	assert (&ra != &rb_c); // copies hold different objects
	
	++ra;
	int incremented = AnyCast<int>(a);
	assert (incremented == 138); // increment by reference changes value

	try
	{
		AnyCast<char &>(a);
		failmsg ("AnyCast to incorrect reference type");
	}
	catch (BadCastException&) { }

	try
	{
		AnyCast<const char &>(b),
		failmsg ("AnyCast to incorrect const reference type");
	}
	catch (BadCastException&) { }
}


void AnyTest::testBadCast()
{
	std::string text = "test message";
	Any value = text;
	
	try
	{
		AnyCast<const char *>(value);
		fail ("must throw");
	}
	catch (BadCastException&) { }
}


void AnyTest::testSwap()
{
	std::string text = "test message";
	Any original = text, swapped;
	std::string* originalPtr = AnyCast<std::string>(&original);
	Any* swapResult = &original.swap(swapped);
	
	assert (original.empty());
	assert (!swapped.empty());
	assert (swapped.type() == typeid(std::string));
	assert (text == AnyCast<std::string>(swapped));
	assert (0 != originalPtr);
#ifdef POCO_NO_SOO // pointers only match when heap-allocated
	assert (originalPtr == AnyCast<std::string>(&swapped));
#endif
	assert (swapResult == &original);
}


void AnyTest::testEmptyCopy()
{
	const Any null;
	Any copied = null, assigned;
	assigned = null;
	
	assert (null.empty());
	assert (copied.empty());
	assert (assigned.empty());
}


void AnyTest::testInt()
{
	Any e;
	assert (e.empty());

	Any a = 13;
	assert (a.type() == typeid(int));
	int* i = AnyCast<int>(&a);
	assert (*i == 13);
	Any b = a;
	assert (b.type() == typeid(int));
	int *cpyI = AnyCast<int>(&b);
	assert (*cpyI == *i);
	*cpyI = 20;
	assert (*cpyI != *i);
	std::string* s = AnyCast<std::string>(&a);
	assert (s == NULL);

	int POCO_UNUSED tmp = AnyCast<int>(a);
	const Any c = a;
	tmp = AnyCast<int>(a);
}


void AnyTest::testComplexType()
{
	SomeClass str(13,std::string("hello"));
	Any a = str;
	Any b = a;
	assert (a.type() == typeid(SomeClass));
	assert (b.type() == typeid(SomeClass));
	SomeClass str2 = AnyCast<SomeClass>(a);
	assert (str == str2);
	const SomeClass& strCRef = RefAnyCast<SomeClass>(a);
	assert (str == strCRef);
	SomeClass& strRef = RefAnyCast<SomeClass>(a);
	assert (str == strRef);
}


void AnyTest::testVector()
{
	std::vector<int> tmp;
	tmp.push_back(1);
	tmp.push_back(2);
	tmp.push_back(3);
	Any a = tmp;
	assert (a.type() == typeid(std::vector<int>));
	std::vector<int> tmp2 = AnyCast<std::vector<int> >(a);
	assert (tmp2.size() == 3);
	const std::vector<int>& vecCRef = RefAnyCast<std::vector<int> >(a);
	std::vector<int>& vecRef = RefAnyCast<std::vector<int> >(a);
	
	assert (vecRef[0] == 1);
	assert (vecRef[1] == 2);
	assert (vecRef[2] == 3);
	vecRef[0] = 0;
	assert (vecRef[0] == vecCRef[0]);
}


void AnyTest::setUp()
{
}


void AnyTest::tearDown()
{
}


CppUnit::Test* AnyTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("AnyTest");

	CppUnit_addTest(pSuite, AnyTest, testConvertingCtor);
	CppUnit_addTest(pSuite, AnyTest, testDefaultCtor);
	CppUnit_addTest(pSuite, AnyTest, testCopyCtor);
	CppUnit_addTest(pSuite, AnyTest, testCopyAssign);
	CppUnit_addTest(pSuite, AnyTest, testConvertingAssign);
	CppUnit_addTest(pSuite, AnyTest, testBadCast);
	CppUnit_addTest(pSuite, AnyTest, testSwap);
	CppUnit_addTest(pSuite, AnyTest, testEmptyCopy);
	CppUnit_addTest(pSuite, AnyTest, testCastToReference);
	CppUnit_addTest(pSuite, AnyTest, testInt);
	CppUnit_addTest(pSuite, AnyTest, testComplexType);
	CppUnit_addTest(pSuite, AnyTest, testVector);

	return pSuite;
}
