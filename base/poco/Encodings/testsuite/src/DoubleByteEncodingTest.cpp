//
// DoubleByteEncodingTest.cpp
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: Apache-2.0
//


#include "DoubleByteEncodingTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/ISO8859_4Encoding.h"
#include "Poco/Windows950Encoding.h"


DoubleByteEncodingTest::DoubleByteEncodingTest(const std::string& name): CppUnit::TestCase(name)
{
}


DoubleByteEncodingTest::~DoubleByteEncodingTest()
{
}


void DoubleByteEncodingTest::testSingleByte()
{
	Poco::ISO8859_4Encoding enc;

	assert (std::string(enc.canonicalName()) == "ISO-8859-4");
	assert (enc.isA("Latin4"));

	unsigned char seq1[] = { 0xF8 }; // 0x00F8 LATIN SMALL LETTER O WITH STROKE
	assert (enc.convert(seq1) == 0x00F8);
	assert (enc.queryConvert(seq1, 1) == 0x00F8);
	assert (enc.sequenceLength(seq1, 1) == 1);

	unsigned char seq2[] = { 0xF9 }; // 0x0173 LATIN SMALL LETTER U WITH OGONEK
	assert (enc.convert(seq2) == 0x0173);
	assert (enc.queryConvert(seq2, 1) == 0x0173);
	assert (enc.sequenceLength(seq2, 1) == 1);
}


void DoubleByteEncodingTest::testSingleByteReverse()
{
	Poco::ISO8859_4Encoding enc;

	unsigned char seq[2];

	assert (enc.convert(0x00F8, seq, 2) == 1);
	assert (seq[0] == 0xF8);

	assert (enc.convert(0x0173, seq, 2) == 1);
	assert (seq[0] == 0xF9);

	assert (enc.convert(0x3000, seq, 2) == 0);
}


void DoubleByteEncodingTest::testDoubleByte()
{
	Poco::Windows950Encoding enc;

	assert (std::string(enc.canonicalName()) == "windows-950");
	assert (enc.isA("Windows-950"));
	assert (enc.isA("cp950"));

	unsigned char seq1[] = { 0x41 }; // 0x0041 LATIN CAPITAL LETTER A
	assert (enc.convert(seq1) == 0x0041);
	assert (enc.queryConvert(seq1, 1) == 0x0041);
	assert (enc.sequenceLength(seq1, 1) == 1);

	unsigned char seq2[] = { 0xA1, 0x40 }; // 0x3000 IDEOGRAPHIC SPACE
	assert (enc.convert(seq2) == 0x3000);
	assert (enc.queryConvert(seq2, 1) == -2);
	assert (enc.queryConvert(seq2, 2) == 0x3000);
	assert (enc.sequenceLength(seq2, 1) == 2);
	assert (enc.sequenceLength(seq2, 2) == 2);

	unsigned char seq3[] = { 0x92 }; // invalid
	assert (enc.convert(seq3) == -1);
	assert (enc.queryConvert(seq3, 1) == -1);
	assert (enc.sequenceLength(seq3, 1) == -1);
}


void DoubleByteEncodingTest::testDoubleByteReverse()
{
	Poco::Windows950Encoding enc;

	unsigned char seq[2];

	assert (enc.convert(0x0041, seq, 2) == 1);
	assert (seq[0] == 0x41);

	assert (enc.convert(0x3000, seq, 2) == 2);
	assert (seq[0] == 0xA1);
	assert (seq[1] == 0x40);

	assert (enc.convert(0x3004, seq, 2) == 0);
}


void DoubleByteEncodingTest::setUp()
{
}


void DoubleByteEncodingTest::tearDown()
{
}


CppUnit::Test* DoubleByteEncodingTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DoubleByteEncodingTest");

	CppUnit_addTest(pSuite, DoubleByteEncodingTest, testSingleByte);
	CppUnit_addTest(pSuite, DoubleByteEncodingTest, testSingleByteReverse);
	CppUnit_addTest(pSuite, DoubleByteEncodingTest, testDoubleByte);
	CppUnit_addTest(pSuite, DoubleByteEncodingTest, testDoubleByteReverse);

	return pSuite;
}
