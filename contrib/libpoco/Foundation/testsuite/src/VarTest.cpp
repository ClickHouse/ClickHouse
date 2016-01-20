//
// VarTest.cpp
//
// $Id: //poco/svn/Foundation/testsuite/src/VarTest.cpp#2 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "VarTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Exception.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/Bugcheck.h"
#include "Poco/Dynamic/Struct.h"
#include "Poco/Dynamic/Pair.h"
#include <map>
#include <utility>


GCC_DIAG_OFF(unused-variable)
#if defined(_MSC_VER) && _MSC_VER < 1400
	#pragma warning(disable:4800)//forcing value to bool 'true' or 'false'
#endif


using namespace Poco;
using namespace Poco::Dynamic;


class Dummy
{
public:
	Dummy(): _val(0)
	{
	}

	Dummy(int val): _val(val)
	{
	}

	operator int () const
	{
		return _val;
	}

	bool operator == (int i)
	{
		return i == _val;
	}

private:
	int _val;
};


VarTest::VarTest(const std::string& name): CppUnit::TestCase(name)
{
}


VarTest::~VarTest()
{
}


void VarTest::testInt8()
{
	Poco::Int8 src = 32;
	Var a1 = src;
	
	assert (a1.type() == typeid(Poco::Int8));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);
	
	Int8 value = a1.extract<Int8>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testInt16()
{
	Poco::Int16 src = 32;
	Var a1 = src;

	assert (a1.type() == typeid(Poco::Int16));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	Int16 value = a1.extract<Int16>();
	assert (value == 32);
	
	try
	{
		Int32 value2; value2 = a1.extract<Int32>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testInt32()
{
	Poco::Int32 src = 32;
	Var a1 = src;
	
	assert (a1.type() == typeid(Poco::Int32));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);
	
	Int32 value = a1.extract<Int32>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testInt64()
{
	Poco::Int64 src = 32;
	Var a1 = src;
	
	assert (a1.type() == typeid(Poco::Int64));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	Int64 value = a1.extract<Int64>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testUInt8()
{
	Poco::UInt8 src = 32;
	Var a1 = src;

	assert (a1.type() == typeid(Poco::UInt8));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	UInt8 value = a1.extract<UInt8>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testUInt16()
{
	Poco::UInt16 src = 32;
	Var a1 = src;

	assert (a1.type() == typeid(Poco::UInt16));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	UInt16 value = a1.extract<UInt16>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testUInt32()
{
	Poco::UInt32 src = 32;
	Var a1 = src;

	assert (a1.type() == typeid(Poco::UInt32));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	UInt32 value = a1.extract<UInt32>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testUInt64()
{
	Poco::UInt64 src = 32;
	Var a1 = src;

	assert (a1.type() == typeid(Poco::UInt64));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	UInt64 value = a1.extract<UInt64>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testBool()
{
	bool src = true;
	Var a1 = src;
	
	assert (a1.type() == typeid(bool));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 1);
	assert (s15 == 1);
	assert (s1 == "true");
	assert (s2 == 1);
	assert (s3 == 1);
	assert (s4 == 1);
	assert (s5 == 1);
	assert (s6 == 1);
	assert (s7 == 1);
	assert (s8 == 1);
	assert (s9 == 1);
	assert (s10 == 1.0f);
	assert (s11 == 1.0);
	assert (s12);
	assert (s13 == '\x1');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	bool value = a1.extract<bool>();
	assert (value);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}
}


void VarTest::testChar()
{
	char src = ' ';
	Var a1 = src;
	
	assert (a1.type() == typeid(char));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == " ");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	char value = a1.extract<char>();
	assert (value == ' ');
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}
}


void VarTest::testFloat()
{
	Var any("0");
	float f = any;

	float src = 32.0f;
	Var a1 = src;
	
	assert (a1.type() == typeid(float));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	float value = a1.extract<float>();
	assert (value == 32.0f);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1.0f;
	assert (a3 == 33.0f);
	a3 = a1 - 1.0f;
	assert (a3 == 31.0f);
	a3 += 1.0f;
	assert (a3 == 32.0f);
	a3 -= 1.0f;
	assert (a3 == 31.0f);
	a3 = a1 / 2.0f;
	assert (a3 == 16.0f);
	a3 = a1 * 2.0f;
	assert (a3 == 64.0f);
	a3 /= 2.0f;
	assert (a3 == 32.0f);
	a3 *= 2.0f;
	assert (a3 == 64.0f);
}


void VarTest::testDouble()
{
	double d = 0;
	Var v(d);
	float f = v;

	double src = 32.0;
	Var a1 = src;
	
	assert (a1.type() == typeid(double));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	double value = a1.extract<double>();
	assert (value == 32.0);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	
	Var a3 = a1 + 1.0;
	assert (a3 == 33.0);
	a3 = a1 - 1.0;
	assert (a3 == 31.0);
	a3 += 1.0;
	assert (a3 == 32.0);
	a3 -= 1.0;
	assert (a3 == 31.0);
	a3 = a1 / 2.0;
	assert (a3 == 16.0);
	a3 = a1 * 2.0;
	assert (a3 == 64.0);
	a3 /= 2.0;
	assert (a3 == 32.0);
	a3 *= 2.0;
	assert (a3 == 64.0);
}


void VarTest::testString()
{
	Var a1("32");
	
	assert (a1.type() == typeid(std::string));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12 = false;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == '3');

	const std::string& value = a1.extract<std::string>();
	assert (value == "32");
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a4(123);
	std::string s("456");
	Var a5 = a4 + s;
	assert (a5 == "123456");
	a4 += s;
	assert (a4 == "123456");
	Var a6 = a4 + "789";
	assert (a6 == "123456789");
	a4 += "789";
	assert (a4 == "123456789");

	a4 = "";
	assert(!a4);
	a4 = "0";
	assert(!a4);
	a4 = "FaLsE";
	assert(!a4);
}


void VarTest::testLong()
{
	long src = 32;
	Var a1 = src;
	
	assert (a1.type() == typeid(long));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);
	
	long value = a1.extract<long>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testULong()
{
	unsigned long src = 32;
	Var a1 = src;
	
	assert (a1.type() == typeid(unsigned long));

	std::string s1;
	Poco::Int8 s2;
	Poco::Int16 s3;
	Poco::Int32 s4;
	Poco::Int64 s5;
	Poco::UInt8 s6;
	Poco::UInt16 s7;
	Poco::UInt32 s8;
	Poco::UInt64 s9;
	float s10;
	double s11;
	bool s12;
	char s13;
	a1.convert(s1);
	a1.convert(s2);
	a1.convert(s3);
	a1.convert(s4);
	a1.convert(s5);
	a1.convert(s6);
	a1.convert(s7);
	a1.convert(s8);
	a1.convert(s9);
	a1.convert(s10);
	a1.convert(s11);
	a1.convert(s12);
	a1.convert(s13);
	long s14;
	unsigned long s15;
	a1.convert(s14);
	a1.convert(s15);
	assert (s14 == 32);
	assert (s15 == 32);
	assert (s1 == "32");
	assert (s2 == 32);
	assert (s3 == 32);
	assert (s4 == 32);
	assert (s5 == 32);
	assert (s6 == 32);
	assert (s7 == 32);
	assert (s8 == 32);
	assert (s9 == 32);
	assert (s10 == 32.0f);
	assert (s11 == 32.0);
	assert (s12);
	assert (s13 == ' ');
	Var a2(a1);
	std::string t2;
	a2.convert(t2);
	assert (s1 == t2);

	unsigned long value = a1.extract<unsigned long>();
	assert (value == 32);
	
	try
	{
		Int16 value2; value2 = a1.extract<Int16>();
		fail("bad cast - must throw");
	}
	catch (Poco::BadCastException&)
	{
	}

	Var a3 = a1 + 1;
	assert (a3 == 33);
	a3 = a1 - 1;
	assert (a3 == 31);
	a3 += 1;
	assert (a3 == 32);
	a3 -= 1;
	assert (a3 == 31);
	a3 = a1 / 2;
	assert (a3 == 16);
	a3 = a1 * 2;
	assert (a3 == 64);
	a3 /= 2;
	assert (a3 == 32);
	a3 *= 2;
	assert (a3 == 64);
}


void VarTest::testUDT()
{
	Dummy d0;
	assert (d0 == 0);

	Dummy d(1);
	Var da = d;
	assert (da.extract<Dummy>() == 1);

	Dummy d1 = d;
	Var da1 = d1;
	assert (da1.extract<Dummy>() == 1);

	try
	{
		float f = da1;
		fail ("must fail");
	}
	catch (BadCastException&) { }
}


void VarTest::testConversionOperator()
{
	Var any("42");
	int i = any;
	assert (i == 42);
	assert (any == i);

	any = 123;
	std::string s1 = any.convert<std::string>();
	assert (s1 == "123");
	assert (s1 == any);
	assert (any == s1);
	assert ("123" == any);

	any = 321;
	s1 = any.convert<std::string>();
	assert (s1 == "321");

	any = "456";
	assert (any == "456");
	assert ("456" == any);

	any = 789;
	std::string s2 = any.convert<std::string>();
	assert (s2 == "789");
	assert (s2 == any);
	assert (any == s2);
	assert ("789" == any);

	Var any2 = "1.5";
	double d = any2;
	assert (d == 1.5);
	assert (any2 == d);
}


void VarTest::testComparisonOperators()
{
	Var any1 = 1;
	Var any2 = "1";
	assert (any1 == any2);
	assert (any1 == 1);
	assert (1 == any1);
	assert (any1 == "1");
	assert ("1" == any1);
	assert (any1 <= 1);
	assert (1 >= any1);
	assert (any1 <= 2);
	assert (2 >= any1);
	assert (any1 < 2);
	assert (2 > any1);
	assert (any1 > 0);
	assert (0 < any1);
	assert (any1 >= 1);
	assert (1 <= any1);
	assert (any1 >= 0);
	assert (0 <= any1);

	any1 = 1L;
	assert (any1 == any2);
	assert (any1 == 1L);
	assert (1L == any1);
	assert (any1 == "1");
	assert ("1" == any1);
	assert (any1 != 2L);
	assert (2L != any1);
	assert (any1 != "2");
	assert ("2" != any1);
	assert (any1 <= 1L);
	assert (1L >= any1);
	assert (any1 <= 2L);
	assert (2L >= any1);
	assert (any1 < 2L);
	assert (2L > any1);
	assert (any1 > 0);
	assert (0 < any1);
	assert (any1 >= 1L);
	assert (1L <= any1);
	assert (any1 >= 0);
	assert (0 <= any1);

	any1 = 0x31;
	assert (any1 == '1');
	assert ('1' == any1);
	assert (any1 <= '1');
	assert ('1' >= any1);
	assert (any1 <= '2');
	assert ('2' >= any1);
	assert (any1 < '2');
	assert ('2' > any1);
	assert (any1 > 0);
	assert (0 < any1);
	assert (any1 >= '1');
	assert ('1' <= any1);
	assert (any1 >= 0);
	assert (0 <= any1);

	any1 = "2";
	assert (any1 != any2);
	assert (any1 != 1);
	assert (1 != any1);
	assert (any1 != "1");
	assert ("1" != any1);

	any1 = 1.5;
	assert (any1 == 1.5);
	assert (1.5 == any1);
	assert (any1 == "1.5");
	assert ("1.5" == any1);
	assert (any1 != 2.5);
	assert (2.5 != any1);
	assert (any1 != "2.5");
	assert ("2.5" != any1);
	assert (any1 <= 1.5);
	assert (1.5 >= any1);
	assert (any1 <= 2.5);
	assert (2.5 >= any1);
	assert (any1 < 2.5);
	assert (2.5 > any1);
	assert (any1 > 0);
	assert (0 < any1);
	assert (any1 >= 1.5);
	assert (1.5 <= any1);
	assert (any1 >= 0);
	assert (0 <= any1);

	any1 = 1.5f;
	assert (any1 == 1.5f);
	assert (1.5f == any1);
	assert (any1 == "1.5");
	assert ("1.5" == any1);
	assert (any1 != 2.5f);
	assert (2.5f != any1);
	assert (any1 != "2.5");
	assert ("2.5" != any1);
	assert (any1 <= 1.5f);
	assert (1.5f >= any1);
	assert (any1 <= 2.5f);
	assert (2.5f >= any1);
	assert (any1 < 2.5f);
	assert (2.5f > any1);
	assert (any1 > 0);
	assert (0 < any1);
	assert (any1 >= 1.5f);
	assert (1.5f <= any1);
	assert (any1 >= 0);
	assert (0 <= any1);
}


void VarTest::testArithmeticOperators()
{
	Var any1 = 1;
	Var any2 = 2;
	Var any3 = any1 + any2;
	assert (any3 == 3);
	int i = 1;
	i += any1;
	assert (2 == i);

	any1 = 3;
	assert ((5 - any1) == 2);
	any2 = 5;
	any3 = any2 - any1;
	assert (any3 == 2);
	any3 -= 1;
	assert (any3 == 1);
	i = 5;
	i -= any1;
	assert (2 == i);

	any1 = 3;
	assert ((5 * any1) == 15);
	any2 = 5;
	any3 = any1 * any2;
	assert (any3 == 15);
	any3 *= 3;
	assert (any3 == 45);
	i = 5;
	i *= any1;
	assert (15 == i);

	any1 = 3;
	assert ((9 / any1) == 3);
	any2 = 9;
	any3 = any2 / any1;
	assert (any3 == 3);
	any3 /= 3;
	assert (any3 == 1);
	i = 9;
	i /= any1;
	assert (3 == i);

	any1 = 1.0f;
	any2 = .5f;
	any3 = .0f;
	any3 = any1 + any2;
	assert (any3 == 1.5f);
	any3 += .5f;
	assert (any3 == 2.0f);

	any1 = 1.0;
	any2 = .5;
	any3 = 0.0;
	any3 = any1 + any2;
	assert (any3 == 1.5);
	any3 += .5;
	assert (any3 == 2.0);

	any1 = 1;
	any2 = "2";
	any3 = any1 + any2;
	assert (any3 == 3);
	any2 = "4";
	any3 += any2;
	assert (any3 == 7);
	assert (1 + any3 == 8);

	any1 = "123";
	any2 = "456";
	any3 = any1 + any2;
	assert (any3 == "123456");
	any2 = "789";
	any3 += any2;
	assert (any3 == "123456789");
	assert (("xyz" + any3) == "xyz123456789");

	try	{ any3 = any1 - any2; fail ("must fail"); } 
	catch (InvalidArgumentException&){}

	try	{ any3 -= any2;	fail ("must fail");	} 
	catch (InvalidArgumentException&){}

	try	{ any3 = any1 * any2; fail ("must fail"); } 
	catch (InvalidArgumentException&){}

	try { any3 *= any2;	fail ("must fail");	} 
	catch (InvalidArgumentException&){}

	try { any3 = any1 / any2; fail ("must fail"); } 
	catch (InvalidArgumentException&){}

	try { any3 /= any2; fail ("must fail");	} 
	catch (InvalidArgumentException&){}

	any1 = 10;
	
	assert (any1++ == 10);
	assert (any1 == 11);
	assert (++any1 == 12);

	assert (any1-- == 12);
	assert (any1 == 11);
	assert (--any1 == 10);

	any1 = 1.23;

	try { ++any1; fail ("must fail"); } 
	catch (InvalidArgumentException&){}
	
	try { any1++; fail ("must fail"); } 
	catch (InvalidArgumentException&){}

	try { --any1; fail ("must fail"); } 
	catch (InvalidArgumentException&){}
	
	try { any1--; fail ("must fail"); } 
	catch (InvalidArgumentException&){}
}


void VarTest::testLimitsInt()
{
	testLimitsSigned<Int16, Int8>();
	testLimitsSigned<Int32, Int8>();
	testLimitsSigned<Int64, Int8>();
	testLimitsFloatToInt<float, Int8>();
	testLimitsFloatToInt<double, Int8>();

	testLimitsSigned<Int32, Int16>();
	testLimitsSigned<Int64, Int16>();
	testLimitsFloatToInt<float, Int16>();
	testLimitsFloatToInt<double, Int16>();

	testLimitsSigned<Int64, Int32>();
	testLimitsFloatToInt<float, Int32>();
	testLimitsFloatToInt<double, Int32>();

	testLimitsSignedUnsigned<Int8, UInt8>();
	testLimitsSignedUnsigned<Int16, UInt8>();
	testLimitsSignedUnsigned<Int32, UInt8>();
	testLimitsSignedUnsigned<Int64, UInt8>();
	testLimitsFloatToInt<float, UInt8>();
	testLimitsFloatToInt<double, UInt8>();

	testLimitsSignedUnsigned<Int8, UInt16>();
	testLimitsSignedUnsigned<Int16, UInt16>();
	testLimitsSignedUnsigned<Int32, UInt16>();
	testLimitsSignedUnsigned<Int64, UInt16>();
	testLimitsFloatToInt<float, UInt16>();
	testLimitsFloatToInt<double, UInt16>();

	testLimitsSignedUnsigned<Int8, UInt32>();
	testLimitsSignedUnsigned<Int16, UInt32>();
	testLimitsSignedUnsigned<Int32, UInt32>();
	testLimitsSignedUnsigned<Int64, UInt32>();
	testLimitsFloatToInt<float, UInt32>();
	testLimitsFloatToInt<double, UInt32>();

	testLimitsSignedUnsigned<Int8, UInt64>();
	testLimitsSignedUnsigned<Int16, UInt64>();
	testLimitsSignedUnsigned<Int32, UInt64>();
	testLimitsSignedUnsigned<Int64, UInt64>();
	testLimitsFloatToInt<float, UInt64>();
	testLimitsFloatToInt<double, UInt64>();


	testLimitsUnsigned<UInt16, UInt8>();
	testLimitsUnsigned<UInt32, UInt8>();
	testLimitsUnsigned<UInt64, UInt8>();

	testLimitsUnsigned<UInt32, UInt16>();
	testLimitsUnsigned<UInt64, UInt16>();

	testLimitsUnsigned<UInt64, UInt32>();
}


void VarTest::testLimitsFloat()
{
	if (std::numeric_limits<double>::max() != std::numeric_limits<float>::max())
	{
		double iMin = -1 * std::numeric_limits<float>::max();
		Var da = iMin * 10;
		try { float f; f = da; fail("must fail"); }
		catch (RangeException&) {}

		double iMax = std::numeric_limits<float>::max();
		da = iMax * 10;
		try { float f; f = da; fail("must fail"); }
		catch (RangeException&) {}
	}
}


void VarTest::testCtor()
{
	// this is mainly to test a reported compiler error with assignment on HP aCC.
	// (SF# 1733964)

	Var a1(42);
	Var a2(a1);
	Var a3;
	
	a3 = a1;
	
	assert (a2 == 42);
	assert (a3 == 42);
}


void VarTest::testIsStruct()
{
	std::string s1("string");
	Poco::Int8 s2(-23);
	Poco::Int16 s3(-33);
	Poco::Int32 s4(-388);
	Poco::Int64 s5(-23823838);
	Poco::UInt8 s6(32u);
	Poco::UInt16 s7(16000u);
	Poco::UInt32 s8(334234u);
	Poco::UInt64 s9(2328328382u);
	float s10(13.333f);
	double s11(13.555);
	bool s12(true);
	char s13('c');
	long s14(232323);
	unsigned long s15(21233232u);
	std::vector<Var> s16;
	Struct<std::string> s17;
	Struct<int> s18;

	Var d1(s1);
	Var d2(s2);
	Var d3(s3);
	Var d4(s4);
	Var d5(s5);
	Var d6(s6);
	Var d7(s7);
	Var d8(s8);
	Var d9(s9);
	Var d10(s10);
	Var d11(s11);
	Var d12(s12);
	Var d13(s13);
	Var d14(s14);
	Var d15(s15);
	Var d16(s16);
	Var d17(s17);
	Var d18(s18);

	assert (!d1.isStruct());
	assert (!d2.isStruct());
	assert (!d3.isStruct());
	assert (!d4.isStruct());
	assert (!d5.isStruct());
	assert (!d6.isStruct());
	assert (!d7.isStruct());
	assert (!d8.isStruct());
	assert (!d9.isStruct());
	assert (!d10.isStruct());
	assert (!d11.isStruct());
	assert (!d12.isStruct());
	assert (!d13.isStruct());
	assert (!d14.isStruct());
	assert (!d15.isStruct());
	assert (!d16.isStruct());
	assert (d17.isStruct());
	assert (d18.isStruct());
}


void VarTest::testIsArray()
{
	std::string s1("string");
	Poco::Int8 s2(-23);
	Poco::Int16 s3(-33);
	Poco::Int32 s4(-388);
	Poco::Int64 s5(-23823838);
	Poco::UInt8 s6(32u);
	Poco::UInt16 s7(16000u);
	Poco::UInt32 s8(334234u);
	Poco::UInt64 s9(2328328382u);
	float s10(13.333f);
	double s11(13.555);
	bool s12(true);
	char s13('c');
	long s14(232323);
	unsigned long s15(21233232u);
	std::vector<Var> s16;
	DynamicStruct s17;

	Var d0;
	Var d1(s1);
	Var d2(s2);
	Var d3(s3);
	Var d4(s4);
	Var d5(s5);
	Var d6(s6);
	Var d7(s7);
	Var d8(s8);
	Var d9(s9);
	Var d10(s10);
	Var d11(s11);
	Var d12(s12);
	Var d13(s13);
	Var d14(s14);
	Var d15(s15);
	Var d16(s16);
	Var d17(s17);

	assert (!d0.isArray());
	assert (!d1.isArray());
	assert (!d2.isArray());
	assert (!d3.isArray());
	assert (!d4.isArray());
	assert (!d5.isArray());
	assert (!d6.isArray());
	assert (!d7.isArray());
	assert (!d8.isArray());
	assert (!d9.isArray());
	assert (!d10.isArray());
	assert (!d11.isArray());
	assert (!d12.isArray());
	assert (!d13.isArray());
	assert (!d14.isArray());
	assert (!d15.isArray());
	assert (d16.isArray());
	assert (!d17.isArray());
}


void VarTest::testArrayIdxOperator()
{
	std::string s1("string");
	Poco::Int8 s2(-23);
	Poco::Int16 s3(-33);
	Poco::Int32 s4(-388);
	Poco::Int64 s5(-23823838);
	Poco::UInt8 s6(32u);
	Poco::UInt16 s7(16000u);
	Poco::UInt32 s8(334234u);
	Poco::UInt64 s9(2328328382u);
	float s10(13.333f);
	double s11(13.555);
	bool s12(true);
	char s13('c');
	long s14(232323);
	unsigned long s15(21233232u);
	std::vector<Var> s16;
	s16.push_back(s1);
	s16.push_back(s2);
	DynamicStruct s17;

	Var d1(s1);
	Var d2(s2);
	Var d3(s3);
	Var d4(s4);
	Var d5(s5);
	Var d6(s6);
	Var d7(s7);
	Var d8(s8);
	Var d9(s9);
	Var d10(s10);
	Var d11(s11);
	Var d12(s12);
	Var d13(s13);
	Var d14(s14);
	Var d15(s15);
	Var d16(s16);
	Var d17(s17);

	testGetIdxMustThrow(d1, 0);
	testGetIdxNoThrow(d2, 0);
	testGetIdxNoThrow(d3, 0);
	testGetIdxNoThrow(d4, 0);
	testGetIdxNoThrow(d5, 0);
	testGetIdxNoThrow(d6, 0);
	testGetIdxNoThrow(d7, 0);
	testGetIdxNoThrow(d8, 0);
	testGetIdxNoThrow(d9, 0);
	testGetIdxNoThrow(d10, 0);
	testGetIdxNoThrow(d11, 0);
	testGetIdxNoThrow(d12, 0);
	testGetIdxNoThrow(d13, 0);
	testGetIdxNoThrow(d14, 0);
	testGetIdxNoThrow(d15, 0);
	testGetIdx(d16, 0, s1);
	testGetIdx(d16, 1, s2);

	testGetIdxMustThrow(d1, 1);
	testGetIdxMustThrow(d2, 1);
	testGetIdxMustThrow(d3, 1);
	testGetIdxMustThrow(d4, 1);
	testGetIdxMustThrow(d5, 1);
	testGetIdxMustThrow(d6, 1);
	testGetIdxMustThrow(d7, 1);
	testGetIdxMustThrow(d8, 1);
	testGetIdxMustThrow(d9, 1);
	testGetIdxMustThrow(d10, 1);
	testGetIdxMustThrow(d11, 1);
	testGetIdxMustThrow(d12, 1);
	testGetIdxMustThrow(d13, 1);
	testGetIdxMustThrow(d14, 1);
	testGetIdxMustThrow(d15, 1);
	testGetIdxMustThrow(d17, 1);
}


void VarTest::testDynamicStructBasics()
{
	DynamicStruct aStruct;
	assert (aStruct.empty());
	assert (aStruct.size() == 0);
	assert (aStruct.members().empty());

	aStruct.insert("First Name", "Little");
	assert (!aStruct.empty());
	assert (aStruct.size() == 1);
	assert (*(aStruct.members().begin()) == "First Name");
	assert (aStruct["First Name"] == "Little");
	aStruct.insert("Last Name", "POCO");
	assert (aStruct.members().size() == 2);
	aStruct.erase("First Name");
	assert (aStruct.size() == 1);
	assert (*(aStruct.members().begin()) == "Last Name");
}


void VarTest::testDynamicStructString()
{
	DynamicStruct aStruct;
	aStruct["First Name"] = "Junior";
	aStruct["Last Name"] = "POCO";
	Var a1(aStruct);
	assert (a1["First Name"] == "Junior");
	assert (a1["Last Name"] == "POCO");
	a1["First Name"] = "Senior";
	assert (a1["First Name"] == "Senior");
	testGetIdxMustThrow(a1, 0);

	Struct<std::string> s1;
	s1["1"] = 1;
	s1["2"] = 2;
	s1["3"] = 3;

	Struct<std::string> s2(s1);
	assert (s2["1"] == 1);
	assert (s2["2"] == 2);
	assert (s2["3"] == 3);

	std::map<std::string, int> m1;
	m1["1"] = 1;
	m1["2"] = 2;
	m1["3"] = 3;

	Struct<std::string> m2(m1);
	assert (m2["1"] == 1);
	assert (m2["2"] == 2);
	assert (m2["3"] == 3);
}


void VarTest::testDynamicStructInt()
{
	Dynamic::Struct<int> aStruct;
	aStruct[0] = "Junior";
	aStruct[1] = "POCO";
	aStruct[2] = 10;
	Var a1(aStruct);
	assert (a1[0]== "Junior");
	assert (a1[1]== "POCO");
	assert (a1[2]== 10);
	a1[0] = "Senior";
	assert (a1[0] == "Senior");

	Struct<int> s1;
	s1[1] = "1";
	s1[2] = "2";
	s1[3] = "3";

	Struct<int> s2(s1);
	assert (s2[1] == "1");
	assert (s2[2] == "2");
	assert (s2[3] == "3");

	std::map<int, std::string> m1;
	m1[1] = "1";
	m1[2] = "2";
	m1[3] = "3";

	Struct<int> m2(m1);
	assert (m2[1] == "1");
	assert (m2[2] == "2");
	assert (m2[3] == "3");
}


void VarTest::testDynamicPair()
{
	Pair<int> aPair;
	assert (0 == aPair.first());
	try
	{
		std::string s = aPair.second().convert<std::string>();
		fail ("must fail");
	}
	catch (InvalidAccessException&) { }

	Var va(aPair);
	assert ("{ \"0\" : null }" == va.convert<std::string>());
	assert (aPair.toString() == va.convert<std::string>());

	aPair = Pair<int>(4, "123");
	assert ("123" == aPair.second());

	va = aPair;
	assert ("{ \"4\" : \"123\" }" == va.convert<std::string>());
	assert (aPair.toString() == va.convert<std::string>());

	int i = 1;
	std::string s = "2";
	Pair<int> iPair(i, s);
	assert (1 == iPair.first());
	assert ("2" == iPair.second());

	Pair<std::string> sPair(s, i);
	assert ("2" == sPair.first());
	assert (1 == sPair.second());

	std::pair<int, std::string> p = std::make_pair(i, s);
	Pair<int> pPair(p);
	assert (1 == pPair.first());
	assert ("2" == pPair.second());

	Var vp(pPair);
	assert ("{ \"1\" : \"2\" }" == vp.convert<std::string>());
	assert (pPair.toString() == vp.convert<std::string>());

	Var vs(sPair);
	assert ("{ \"2\" : 1 }" == vs.convert<std::string>());
	assert (sPair.toString() == vs.convert<std::string>());
}


void VarTest::testArrayToString()
{
	std::string s1("string");
	Poco::Int8 s2(23);
	std::vector<Var> s16;
	s16.push_back(s1);
	s16.push_back(s2);
	Var a1(s16);
	std::string res = a1.convert<std::string>();
	std::string expected("[ \"string\", 23 ]");
	assert (res == expected);
}


void VarTest::testArrayToStringEscape()
{
	std::string s1("\"quoted string\"");
	Poco::Int8 s2(23);
	std::vector<Var> s16;
	s16.push_back(s1);
	s16.push_back(s2);
	Var a1(s16);
	std::string res = a1.convert<std::string>();
	std::string expected("[ \"\\\"quoted string\\\"\", 23 ]");
	assert (res == expected);
}


void VarTest::testStructToString()
{
	DynamicStruct aStruct;
	aStruct["First Name"] = "Junior";
	aStruct["Last Name"] = "POCO";
	aStruct["Age"] = 1;
	Var a1(aStruct);
	std::string res = a1.convert<std::string>();
	std::string expected = "{ \"Age\" : 1, \"First Name\" : \"Junior\", \"Last Name\" : \"POCO\" }";
	assert (res == expected);
	assert (aStruct.toString() == res);
}


void VarTest::testStructToStringEscape()
{
	DynamicStruct aStruct;
	aStruct["Value"] = "Value with \" and \n";
	Var a1(aStruct);
	std::string res = a1.convert<std::string>();
	std::string expected = "{ \"Value\" : \"Value with \\\" and \\n\" }";
	assert (res == expected);
	assert (aStruct.toString() == res);
}


void VarTest::testArrayOfStructsToString()
{
	std::vector<Var> s16;
	DynamicStruct aStruct;
	aStruct["First Name"] = "Junior";
	aStruct["Last Name"] = "POCO";
	aStruct["Age"] = 1;
	s16.push_back(aStruct);
	aStruct["First Name"] = "Senior";
	aStruct["Last Name"] = "POCO";
	aStruct["Age"] = 100;
	s16.push_back(aStruct);
	std::vector<Var> s16Cpy = s16;
	// recursive arrays!
	s16Cpy.push_back(s16);
	s16.push_back(s16Cpy);
	Var a1(s16);
	std::string res = a1.convert<std::string>();
	std::string expected = "[ "
						"{ \"Age\" : 1, \"First Name\" : \"Junior\", \"Last Name\" : \"POCO\" }, "
						"{ \"Age\" : 100, \"First Name\" : \"Senior\", \"Last Name\" : \"POCO\" }, "
							"[ "
							"{ \"Age\" : 1, \"First Name\" : \"Junior\", \"Last Name\" : \"POCO\" }, "
							"{ \"Age\" : 100, \"First Name\" : \"Senior\", \"Last Name\" : \"POCO\" }, "
								"[ "
								"{ \"Age\" : 1, \"First Name\" : \"Junior\", \"Last Name\" : \"POCO\" }, "
								"{ \"Age\" : 100, \"First Name\" : \"Senior\", \"Last Name\" : \"POCO\" } "
								"] ] ]";
	
	assert (res == expected);
	assert (a1.toString() == res);
}


void VarTest::testStructWithArraysToString()
{
	std::string s1("string");
	Poco::Int8 s2(23);
	std::vector<Var> s16;
	s16.push_back(s1);
	s16.push_back(s2);
	Var a1(s16);
	DynamicStruct addr;
	addr["Number"] = 4;
	addr["Street"] = "Unknown";
	addr["Country"] = "Carinthia";
	DynamicStruct aStruct;
	aStruct["First Name"] = "Junior";
	aStruct["Last Name"] = a1;
	aStruct["Age"] = 1;
	aStruct["Address"] = addr;
	Var a2(aStruct);
	std::string res = a2.convert<std::string>();
	std::string expected = "{ \"Address\" : { \"Country\" : \"Carinthia\", \"Number\" : 4, \"Street\" : \"Unknown\" }, "
								"\"Age\" : 1, \"First Name\" : \"Junior\", \"Last Name\" : [ \"string\", 23 ] }";

	assert (res == expected);
	assert (aStruct.toString() == res);
}


void VarTest::testJSONDeserializeString()
{
	Var a("test");
	std::string tst = Var::toString(a);
	Var b = Var::parse(tst);
	assert (b.convert<std::string>() == "test");

	Var c('c');
	tst = Var::toString(c);
	Var b2 = Var::parse(tst);
	char cc = b2.convert<char>();
	assert (cc == 'c');

	tst = "{ \"a\" : 1, \"b\" : 2 \n}";
	a = Var::parse(tst);
	assert(a.toString() == "{ \"a\" : \"1\", \"b\" : \"2\" }");

	tst = "{ \"a\" : 1, \"b\" : 2\n}";
	a = Var::parse(tst);
	assert(a.toString() == "{ \"a\" : \"1\", \"b\" : \"2\" }");
}


void VarTest::testJSONDeserializePrimitives()
{
	Poco::Int8 i8(-12);
	Poco::UInt16 u16(2345);
	Poco::Int32 i32(-24343);
	Poco::UInt64 u64(1234567890);
	u64 *= u64;
	bool b = false;
	float f = 3.1415f;
	double d = 3.1415;

	std::string s8 = Var::toString(i8);
	std::string s16 = Var::toString(u16);
	std::string s32 = Var::toString(i32);
	std::string s64 = Var::toString(u64);
	std::string sb = Var::toString(b);
	std::string sf = Var::toString(f);
	std::string sd = Var::toString(d);
	Var a8 = Var::parse(s8);
	Var a16 = Var::parse(s16);
	Var a32 = Var::parse(s32);
	Var a64 = Var::parse(s64);
	Var ab = Var::parse(sb);
	Var af = Var::parse(sf);
	Var ad = Var::parse(sd);
	assert (a8 == i8);
	assert (a16 == u16);
	assert (a32 == i32);
	assert (a64 == u64);
	assert (ab == b);
	assert (af == f);
	assert (ad == d);
}


void VarTest::testJSONDeserializeArray()
{
	Poco::Int8 i8(-12);
	Poco::UInt16 u16(2345);
	Poco::Int32 i32(-24343);
	Poco::UInt64 u64(1234567890);
	u64 *= u64;
	bool b = false;
	float f = 3.1415f;
	double d = 3.1415;
	std::string s("test string");
	char c('x');
	std::vector<Var> aVec;
	aVec.push_back(i8);
	aVec.push_back(u16);
	aVec.push_back(i32);
	aVec.push_back(u64);
	aVec.push_back(b);
	aVec.push_back(f);
	aVec.push_back(d);
	aVec.push_back(s);
	aVec.push_back(c);

	std::string sVec = Var::toString(aVec);
	Var a = Var::parse(sVec);
	assert (a[0] == i8);
	assert (a[1] == u16);
	assert (a[2] == i32);
	assert (a[3] == u64);
	assert (a[4] == b);
	assert (a[5] == f);
	assert (a[6] == d);
	assert (a[7] == s);
	assert (a[8] == c);
}


void VarTest::testJSONDeserializeComplex()
{
	Poco::Int8 i8(-12);
	Poco::UInt16 u16(2345);
	Poco::Int32 i32(-24343);
	Poco::UInt64 u64(1234567890);
	u64 *= u64;
	bool b = false;
	float f = 3.1415f;
	double d = 3.1415;
	std::string s("test string");
	char c('x');
	DynamicStruct aStr;
	aStr["i8"] = i8;
	aStr["u16"] = u16;
	aStr["i32"] = i32;
	aStr["u64"] = u64;
	aStr["b"] = b;
	aStr["f"] = f;
	aStr["d"] = d;
	aStr["s"] = s;
	aStr["c"] = c;
	std::vector<Var> aVec;
	aVec.push_back(i8);
	aVec.push_back(u16);
	aVec.push_back(i32);
	aVec.push_back(u64);
	aVec.push_back(b);
	aVec.push_back(f);
	aVec.push_back(d);
	aVec.push_back(s);
	aVec.push_back(c);
	aVec.push_back(aStr);
	aStr["vec"] = aVec;

	std::string sStr = Var::toString(aStr);
	Var a = Var::parse(sStr);
	assert (a.isStruct());
	assert (aStr["i8"] == i8);
	assert (aStr["u16"] == u16);
	assert (aStr["i32"] == i32);
	assert (aStr["u64"] == u64);
	assert (aStr["b"] == b);
	assert (aStr["f"] == f);
	assert (aStr["d"] == d);
	assert (aStr["s"] == s);
	assert (aStr["c"] == c);
	Var vecRet = a["vec"];
	assert (vecRet.isArray());
	assert (vecRet[0] == i8);
	assert (vecRet[1] == u16);
	assert (vecRet[2] == i32);
	assert (vecRet[3] == u64);
	assert (vecRet[4] == b);
	assert (vecRet[5] == f);
	assert (vecRet[6] == d);
	assert (vecRet[7] == s);
	assert (vecRet[8] == c);
	Var strRet = vecRet[9];
	assert (strRet.isStruct());
}


void VarTest::testJSONDeserializeStruct()
{
	Poco::Int8 i8(-12);
	Poco::UInt16 u16(2345);
	Poco::Int32 i32(-24343);
	Poco::UInt64 u64(1234567890);
	u64 *= u64;
	bool b = false;
	float f = 3.1415f;
	double d = 3.1415;
	std::string s("test string");
	char c('x');
	DynamicStruct aStr;
	aStr["i8"] = i8;
	aStr["u16"] = u16;
	aStr["i32"] = i32;
	aStr["u64"] = u64;
	aStr["b"] = b;
	aStr["f"] = f;
	aStr["d"] = d;
	aStr["s"] = s;
	aStr["c"] = c;

	std::string sStr = Var::toString(aStr);
	Var a = Var::parse(sStr);
	assert (aStr["i8"] == i8);
	assert (aStr["u16"] == u16);
	assert (aStr["i32"] == i32);
	assert (aStr["u64"] == u64);
	assert (aStr["b"] == b);
	assert (aStr["f"] == f);
	assert (aStr["d"] == d);
	assert (aStr["s"] == s);
	assert (aStr["c"] == c);
}


void VarTest::testDate()
{
	Poco::DateTime dtNow(2007, 3, 13, 8, 12, 15);
	
	Poco::Timestamp tsNow = dtNow.timestamp();
	Poco::LocalDateTime ldtNow(dtNow.timestamp());
	Var dt(dtNow);
	Var ts(tsNow);
	Var ldt(ldtNow);
	Var dtStr(dt.convert<std::string>());
	Var tsStr(ts.convert<std::string>());
	Var ldtStr(ldt.convert<std::string>());
	DateTime dtRes = dtStr.convert<DateTime>();
	LocalDateTime ldtRes = ldtStr.convert<LocalDateTime>();
	Timestamp tsRes = tsStr.convert<Timestamp>();
	assert (dtNow == dtRes);
	assert (ldtNow == ldtRes);
	assert (tsNow == tsRes);
}


void VarTest::testGetIdxNoThrow(Var& a1, std::vector<Var>::size_type n)
{
	Var val1 = a1[n];
}


void VarTest::testGetIdxMustThrow(Var& a1, std::vector<Var>::size_type n)
{
	try
	{
		Var val1 = a1[n]; 
		fail("bad cast - must throw");
		val1 = 0; // silence the compiler
	}
	catch (Poco::InvalidAccessException&)
	{
	}
	catch (Poco::RangeException&)
	{
	}

	try
	{
		const Var& c1 = a1;
		const Var& cval1 = c1[n]; 
		fail("bad const cast - must throw");
		assert (cval1 == c1); // silence the compiler
	}
	catch (Poco::InvalidAccessException&)
	{
	}
	catch (Poco::RangeException&)
	{
	}
}


void VarTest::testEmpty()
{
	Var da;
	assert (da.isEmpty());
	assert (da.type() == typeid(void));
	assert (!da.isArray());
	assert (!da.isInteger());
	assert (!da.isNumeric());
	assert (!da.isSigned());
	assert (!da.isString());
	assert (da == da);
	assert (!(da != da));

	assert (da != Var(1));
	assert (!(da == Var(1)));
	assert (Var(1) != da);
	assert (!(Var(1) == da));

	da = "123";
	int i = da.convert<int>();
	assert (123 == i);
	std::string s = da.extract<std::string>();
	assert ("123" == s);
	assert (!da.isEmpty());
	da.empty();
	assert (da.isEmpty());
	assert (da.type() == typeid(void));
	assert (!da.isArray());
	assert (!da.isInteger());
	assert (!da.isNumeric());
	assert (!da.isSigned());
	assert (!da.isString());
	assert (da == da);
	assert (!(da != da));

	assert (da != Var(1));
	assert (!(da == Var(1)));
	assert (Var(1) != da);
	assert (!(Var(1) == da));

	assert (da != "");
	assert ("" != da);
	assert (!(da == ""));
	assert (!("" == da));

	testEmptyComparisons<unsigned char>();
	testEmptyComparisons<char>();
	testEmptyComparisons<Poco::UInt8>();
	testEmptyComparisons<Poco::Int8>();
	testEmptyComparisons<Poco::UInt16>();
	testEmptyComparisons<Poco::Int16>();
	testEmptyComparisons<Poco::UInt32>();
	testEmptyComparisons<Poco::Int32>();
	testEmptyComparisons<Poco::UInt64>();
	testEmptyComparisons<Poco::Int64>();
#ifdef POCO_LONG_IS_64_BIT
	testEmptyComparisons<unsigned long>();
	testEmptyComparisons<long>();
#endif
	testEmptyComparisons<float>();
	testEmptyComparisons<double>();

	try
	{
		int i = da;
		fail ("must fail");
	} catch (InvalidAccessException&) { }

	try
	{
		int i = da.extract<int>();
		fail ("must fail");
	} catch (InvalidAccessException&) { }
}


void VarTest::testIterator()
{
	Var da;
	assert (da.isEmpty());
	assert (da.begin() == da.end());

	da = 1;
	assert (!da.isEmpty());
	assert (da == 1);
	assert (da[0] == 1);
	try
	{
		da[1] = 2;
	}
	catch (RangeException&) {}
	assert (da.begin() != da.end());

	Var::Iterator it = da.begin();
	Var::Iterator end = da.end();
	assert (it != end);
	assert (++it == end);
	assert (--it == da.begin());
	it++;
	assert (it == end);
	try
	{
		++it;
		fail ("must fail");
	}
	catch (RangeException&) {}
	assert (it == end);

	da = "abc";
	assert (da.size() == 3);
	assert (!da.isArray());
	assert (da.isString());
	//assert (da[0] == 'a');
	assert (da.at(0) == 'a');
	//assert (da[1] = 'b');
	assert (da.at(1) == 'b');
	//assert (da[2] = 'c');
	assert (da.at(2) == 'c');

	da.at(0) = 'b';
	assert (da.at(0) == 'b');
	// TODO: allow treatment of strings like arrays
	//da[1] = 'c';
	da.at(1) = 'c';
	assert (da.at(1) == 'c');
	//da[2] = 'a';
	da.at(2) = 'a';
	assert (da.at(2) == 'a');

	it = da.begin();
	end = da.end();
	assert (it != end);
	assert (++it != end);
	assert (--it == da.begin());

	testContainerIterator<std::vector<Var> >();
	testContainerIterator<std::list<Var> >();
	testContainerIterator<std::deque<Var> >();
}


void VarTest::setUp()
{
}


void VarTest::tearDown()
{
}


CppUnit::Test* VarTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("VarTest");

	CppUnit_addTest(pSuite, VarTest, testInt8);
	CppUnit_addTest(pSuite, VarTest, testInt16);
	CppUnit_addTest(pSuite, VarTest, testInt32);
	CppUnit_addTest(pSuite, VarTest, testInt64);
	CppUnit_addTest(pSuite, VarTest, testUInt8);
	CppUnit_addTest(pSuite, VarTest, testUInt16);
	CppUnit_addTest(pSuite, VarTest, testUInt32);
	CppUnit_addTest(pSuite, VarTest, testUInt64);
	CppUnit_addTest(pSuite, VarTest, testBool);
	CppUnit_addTest(pSuite, VarTest, testChar);
	CppUnit_addTest(pSuite, VarTest, testFloat);
	CppUnit_addTest(pSuite, VarTest, testDouble);
	CppUnit_addTest(pSuite, VarTest, testLong);
	CppUnit_addTest(pSuite, VarTest, testULong);
	CppUnit_addTest(pSuite, VarTest, testString);
	CppUnit_addTest(pSuite, VarTest, testUDT);
	CppUnit_addTest(pSuite, VarTest, testConversionOperator);
	CppUnit_addTest(pSuite, VarTest, testComparisonOperators);
	CppUnit_addTest(pSuite, VarTest, testArithmeticOperators);
	CppUnit_addTest(pSuite, VarTest, testLimitsInt);
	CppUnit_addTest(pSuite, VarTest, testLimitsFloat);
	CppUnit_addTest(pSuite, VarTest, testCtor);
	CppUnit_addTest(pSuite, VarTest, testIsStruct);
	CppUnit_addTest(pSuite, VarTest, testIsArray);
	CppUnit_addTest(pSuite, VarTest, testArrayIdxOperator);
	CppUnit_addTest(pSuite, VarTest, testDynamicPair);
	CppUnit_addTest(pSuite, VarTest, testDynamicStructBasics);
	CppUnit_addTest(pSuite, VarTest, testDynamicStructString);
	CppUnit_addTest(pSuite, VarTest, testDynamicStructInt);
	CppUnit_addTest(pSuite, VarTest, testArrayToString);
	CppUnit_addTest(pSuite, VarTest, testArrayToStringEscape);
	CppUnit_addTest(pSuite, VarTest, testStructToString);
	CppUnit_addTest(pSuite, VarTest, testStructToStringEscape);
	CppUnit_addTest(pSuite, VarTest, testArrayOfStructsToString);
	CppUnit_addTest(pSuite, VarTest, testStructWithArraysToString);
	CppUnit_addTest(pSuite, VarTest, testJSONDeserializeString);
	CppUnit_addTest(pSuite, VarTest, testJSONDeserializePrimitives);
	CppUnit_addTest(pSuite, VarTest, testJSONDeserializeArray);
	CppUnit_addTest(pSuite, VarTest, testJSONDeserializeStruct);
	CppUnit_addTest(pSuite, VarTest, testJSONDeserializeComplex);
	CppUnit_addTest(pSuite, VarTest, testDate);
	CppUnit_addTest(pSuite, VarTest, testEmpty);
	CppUnit_addTest(pSuite, VarTest, testIterator);

	return pSuite;
}
