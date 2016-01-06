//
// StringTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/StringTest.h#1 $
//
// Definition of the StringTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef StringTest_INCLUDED
#define StringTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include "Poco/NumericString.h"
#include "Poco/MemoryStream.h"


class StringTest: public CppUnit::TestCase
{
public:
	StringTest(const std::string& name);
	~StringTest();

	void testTrimLeft();
	void testTrimLeftInPlace();
	void testTrimRight();
	void testTrimRightInPlace();
	void testTrim();
	void testTrimInPlace();
	void testToUpper();
	void testToLower();
	void testIstring();
	void testIcompare();
	void testCILessThan();
	void testTranslate();
	void testTranslateInPlace();
	void testReplace();
	void testReplaceInPlace();
	void testCat();

	void testStringToInt();
	void testStringToFloat();
	void testStringToDouble();
	void testStringToFloatError();
	void testNumericLocale();
	void benchmarkStrToFloat();
	void benchmarkStrToInt();

	void testIntToString();
	void testFloatToString();
	void benchmarkFloatToStr();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:

	template <typename T>
	void stringToInt()
	{
		T result = 0;
		if (123 <= std::numeric_limits<T>::max())
			assert(Poco::strToInt("123", result, 10)); assert(result == 123);
		
		assert(Poco::strToInt("0", result, 10)); assert(result == 0);
		assert(Poco::strToInt("000", result, 10)); assert(result == 0);
		
		if (123 < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("  123  ", result, 10)); assert(result == 123); }
		if (123 < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt(" 123", result, 10)); assert(result == 123); }
		if (123 < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("123 ", result, 10)); assert(result == 123); }
		if (std::numeric_limits<T>::is_signed && (-123 > std::numeric_limits<T>::min()))
			{ assert(Poco::strToInt("-123", result, 10)); assert(result == -123); }
		if (0x123 < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("123", result, 0x10)); assert(result == 0x123); }
		if (0x12ab < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("12AB", result, 0x10)); assert(result == 0x12ab); }
		if (0x12ab < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("0X12AB", result, 0x10)); assert(result == 0x12ab); }
		if (0x12ab < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("0x12AB", result, 0x10)); assert(result == 0x12ab); }
		if (0x12ab < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("0x12aB", result, 0x10)); assert(result == 0x12ab); }
		if (0x98fe < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("0X98Fe", result, 0x10)); assert(result == 0x98fe); }
		if (123 < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("0x0", result, 0x10)); assert(result == 0); }
		if (123 < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("00", result, 0x10)); assert(result == 0); }
		if (0123 < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("123", result, 010));  assert(result == 0123); }
		if (0123 < std::numeric_limits<T>::max())
			{ assert(Poco::strToInt("0123", result, 010)); assert(result == 0123); }
		
		assert(Poco::strToInt("0", result, 010)); assert(result == 0);
		assert(Poco::strToInt("000", result, 010)); assert(result == 0);
	}

	template <typename T>
	bool parseStream(const std::string& s, T& value)
	{
		Poco::MemoryInputStream istr(s.data(), s.size());
#if !defined(POCO_NO_LOCALE)
		istr.imbue(std::locale::classic());
#endif
		istr >> value;
		return istr.eof() && !istr.fail();
	}
};


#endif // StringTest_INCLUDED
