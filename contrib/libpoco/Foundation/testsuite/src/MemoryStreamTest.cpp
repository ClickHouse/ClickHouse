//
// MemoryStreamTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/MemoryStreamTest.cpp#2 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MemoryStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Buffer.h"
#include "Poco/MemoryStream.h"
#include <sstream>


using Poco::MemoryInputStream;
using Poco::MemoryOutputStream;


MemoryStreamTest::MemoryStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


MemoryStreamTest::~MemoryStreamTest()
{
}


void MemoryStreamTest::testInput()
{
	const char* data = "This is a test";
	MemoryInputStream istr1(data, 14);
	
	int c = istr1.get();
	assert (c == 'T');
	c = istr1.get();
	assert (c == 'h');
	
	std::string str;
	istr1 >> str;
	assert (str == "is");
	
	char buffer[32];
	istr1.read(buffer, sizeof(buffer));
	assert (istr1.gcount() == 10);
	buffer[istr1.gcount()] = 0;
	assert (std::string(" is a test") == buffer);
	
	const char* data2 = "123";
	MemoryInputStream istr2(data2, 3);
	c = istr2.get();
	assert (c == '1');
	assert (istr2.good());
	c = istr2.get();
	assert (c == '2');
	istr2.unget();
	c = istr2.get();
	assert (c == '2');
	assert (istr2.good());
	c = istr2.get();
	assert (c == '3');
	assert (istr2.good());
	c = istr2.get();
	assert (c == -1);
	assert (istr2.eof());
}


void MemoryStreamTest::testOutput()
{
	char output[64];
	MemoryOutputStream ostr1(output, 64);
	ostr1 << "This is a test " << 42 << std::ends;
	assert (ostr1.charsWritten() == 18);
	assert (std::string("This is a test 42") == output);
	
	char output2[4];
	MemoryOutputStream ostr2(output2, 4);
	ostr2 << "test";
	assert (ostr2.good());
	ostr2 << 'x';
	assert (ostr2.fail());
}

void MemoryStreamTest::testTell()
{
	Poco::Buffer<char> buffer(1024);
	Poco::MemoryOutputStream ostr(buffer.begin(), buffer.size());
	ostr << 'H' << 'e' << 'l' << 'l' << 'o' << '\0';
	std::streamoff np = ostr.tellp();
	assert (np == 6);

	Poco::MemoryInputStream istr(buffer.begin(), buffer.size());

	char c;
	istr >> c;
	assert (c == 'H');

	istr >> c;
	assert (c == 'e');

	istr >> c;
	assert (c == 'l');

	std::streamoff ng = istr.tellg();
	assert (ng == 3);
}


void MemoryStreamTest::testInputSeek()
{
	Poco::Buffer<char> buffer(9);
	Poco::MemoryOutputStream ostr(buffer.begin(), buffer.size());
	ostr << '1' << '2' << '3' << '4' << '5' << '6' << '7' << '8' << '9';

	Poco::MemoryInputStream istr(buffer.begin(), buffer.size());
	char c;

	istr >> c;
	assert (c == '1');

	istr.seekg(3, std::ios_base::beg);	// 3 from beginning
	istr >> c;							// now that makes 4
	assert (4 == istr.tellg());
	assert (c == '4');

	istr.seekg(2, std::ios_base::cur);	// now that makes 6
	istr >> c;							// now that makes 7
	assert (7 == istr.tellg());
	assert (c == '7');

	istr.seekg(-7, std::ios_base::end);	// so that puts us at 9-7=2
	istr >> c;							// now 3
	assert (3 == istr.tellg());
	assert (c == '3');


	istr.seekg(9, std::ios_base::beg);
	assert (istr.good());
	assert (9 == istr.tellg());

	{
		Poco::MemoryInputStream istr2(buffer.begin(), buffer.size());
		istr2.seekg(10, std::ios_base::beg);
#ifdef __APPLE__
		// workaround for clang libstdc++, which does not
		// set failbit if seek returns -1
		assert (istr2.fail() || istr2.tellg() == std::streampos(0));
#else
		assert (istr2.fail());
#endif
	}


	istr.seekg(-9, std::ios_base::end);
	assert (istr.good());
	assert (0 == istr.tellg());

	{
		Poco::MemoryInputStream istr2(buffer.begin(), buffer.size());
		istr2.seekg(-10, std::ios_base::end);
#ifdef __APPLE__
		// workaround for clang libstdc++, which does not
		// set failbit if seek returns -1
		assert (istr2.fail() || istr2.tellg() == std::streampos(0));
#else
		assert (istr2.fail());
#endif
	}


	istr.seekg(0, std::ios_base::beg);
	assert (istr.good());
	assert (0 == istr.tellg());

	{
		Poco::MemoryInputStream istr2(buffer.begin(), buffer.size());
		istr2.seekg(-1, std::ios_base::beg);
#ifdef __APPLE__
		// workaround for clang libstdc++, which does not
		// set failbit if seek returns -1
		assert (istr2.fail() || istr2.tellg() == std::streampos(0));
#else
		assert (istr2.fail());
#endif
	}


	istr.seekg(0, std::ios_base::end);
	assert (istr.good());
	assert (9 == istr.tellg());

	{
		Poco::MemoryInputStream istr2(buffer.begin(), buffer.size());
		istr2.seekg(1, std::ios_base::end);
#ifdef __APPLE__
		// workaround for clang libstdc++, which does not
		// set failbit if seek returns -1
		assert (istr2.fail() || istr2.tellg() == std::streampos(0));
#else
		assert (istr2.fail());
#endif
	}


	istr.seekg(3, std::ios_base::beg);
	assert (istr.good());
	assert (3 == istr.tellg());
	istr.seekg(6, std::ios_base::cur);
	assert (istr.good());
	assert (9 == istr.tellg());

	{
		Poco::MemoryInputStream istr2(buffer.begin(), buffer.size());
		istr2.seekg(4, std::ios_base::beg);
		istr2.seekg(6, std::ios_base::cur);
#ifdef __APPLE__
		// workaround for clang libstdc++, which does not
		// set failbit if seek returns -1
		assert (istr2.fail() || istr2.tellg() == std::streampos(4));
#else
		assert (istr2.fail());
#endif
	}


	istr.seekg(-4, std::ios_base::end);
	assert (istr.good());
	assert (5 == istr.tellg());
	istr.seekg(4, std::ios_base::cur);
	assert (istr.good());
	assert (9 == istr.tellg());

	{
		Poco::MemoryInputStream istr2(buffer.begin(), buffer.size());
		istr2.seekg(-4, std::ios_base::end);
		istr2.seekg(5, std::ios_base::cur);
#ifdef __APPLE__
		// workaround for clang libstdc++, which does not
		// set failbit if seek returns -1
		assert (istr2.fail() || istr2.tellg() == std::streampos(5));
#else
		assert (istr2.fail());
#endif
	}


	istr.seekg(4, std::ios_base::beg);
	assert (istr.good());
	assert (4 == istr.tellg());
	istr.seekg(-4, std::ios_base::cur);
	assert (istr.good());
	assert (0 == istr.tellg());

	{
		Poco::MemoryInputStream istr2(buffer.begin(), buffer.size());
		istr2.seekg(4, std::ios_base::beg);
		istr2.seekg(-5, std::ios_base::cur);
#ifdef __APPLE__
		// workaround for clang libstdc++, which does not
		// set failbit if seek returns -1
		assert (istr2.fail() || istr2.tellg() == std::streampos(4));
#else
		assert (istr2.fail());
#endif
	}
}


void MemoryStreamTest::testInputSeekVsStringStream()
{
	Poco::Buffer<char> buffer(9);
	Poco::MemoryOutputStream ostr(buffer.begin(), buffer.size());
	ostr << '1' << '2' << '3' << '4' << '5' << '6' << '7' << '8' << '9';

	std::stringstream sss;
	sss << '1' << '2' << '3' << '4' << '5' << '6' << '7' << '8' << '9';

	Poco::MemoryInputStream mis(buffer.begin(), buffer.size());

	char x, y;

	sss >> x;
	mis >> y;
	assert (x == y);

	sss.seekg(3, std::ios_base::beg);
	mis.seekg(3, std::ios_base::beg);
	sss >> x;
	mis >> y;
	assert (x == y);
	assert (sss.tellg() == mis.tellg());

	sss.seekg(2, std::ios_base::cur);
	mis.seekg(2, std::ios_base::cur);
	sss >> x;
	mis >> y;
	assert (x == y);
	assert (sss.tellg() == mis.tellg());

	sss.seekg(-7, std::ios_base::end);
	mis.seekg(-7, std::ios_base::end);
	sss >> x;
	mis >> y;
	assert (x == y);
	assert (sss.tellg() == mis.tellg());
}


void MemoryStreamTest::testOutputSeek()
{
	Poco::Buffer<char> buffer(9);
	Poco::MemoryOutputStream ostr(buffer.begin(), buffer.size());
	ostr << '1' << '2' << '3' << '4' << '5' << '6' << '7' << '8' << '9';

	ostr.seekp(4, std::ios_base::beg);	// 4 from beginning
	ostr << 'a';						// and that makes 5 (zero index 4)
	assert (5 == ostr.tellp());
	assert (buffer[4] == 'a');

	ostr.seekp(2, std::ios_base::cur);	// and this makes 7
	ostr << 'b';						// and this makes 8 (zero index 7)
	assert (8 == ostr.tellp());
	assert (buffer[7] == 'b');

	ostr.seekp(-3, std::ios_base::end);	// 9-3=6 from the beginning
	ostr << 'c';						// and this makes 7 (zero index 6)
	assert (7 == ostr.tellp());
	assert (buffer[6] == 'c');


	ostr.seekp(9, std::ios_base::beg);
	assert (ostr.good());
	assert (9 == ostr.tellp());

	{
		Poco::MemoryOutputStream ostr2(buffer.begin(), buffer.size());
		ostr2.seekp(10, std::ios_base::beg);
		assert (ostr2.fail());
	}


	ostr.seekp(-9, std::ios_base::end);
	assert (ostr.good());
	assert (0 == ostr.tellp());

	{
		Poco::MemoryOutputStream ostr2(buffer.begin(), buffer.size());
		ostr2.seekp(-10, std::ios_base::end);
		assert (ostr2.fail());
	}


	ostr.seekp(0, std::ios_base::beg);
	assert (ostr.good());
	assert (0 == ostr.tellp());

	{
		Poco::MemoryOutputStream ostr2(buffer.begin(), buffer.size());
		ostr2.seekp(-1, std::ios_base::beg);
		assert (ostr2.fail());
	}


	ostr.seekp(0, std::ios_base::end);
	assert (ostr.good());
	assert (9 == ostr.tellp());

	{
		Poco::MemoryOutputStream ostr2(buffer.begin(), buffer.size());
		ostr2.seekp(1, std::ios_base::end);
		assert (ostr2.fail());
	}


	ostr.seekp(3, std::ios_base::beg);
	assert (ostr.good());
	assert (3 == ostr.tellp());
	ostr.seekp(6, std::ios_base::cur);
	assert (ostr.good());
	assert (9 == ostr.tellp());

	{
		Poco::MemoryOutputStream ostr2(buffer.begin(), buffer.size());
		ostr2.seekp(4, std::ios_base::beg);
		ostr2.seekp(6, std::ios_base::cur);
		assert (ostr2.fail());
	}


	ostr.seekp(-4, std::ios_base::end);
	assert (ostr.good());
	assert (5 == ostr.tellp());
	ostr.seekp(4, std::ios_base::cur);
	assert (ostr.good());
	assert (9 == ostr.tellp());

	{
		Poco::MemoryOutputStream ostr2(buffer.begin(), buffer.size());
		ostr2.seekp(-4, std::ios_base::end);
		ostr2.seekp(5, std::ios_base::cur);
		assert (ostr2.fail());
	}


	ostr.seekp(4, std::ios_base::beg);
	assert (ostr.good());
	assert (4 == ostr.tellp());
	ostr.seekp(-4, std::ios_base::cur);
	assert (ostr.good());
	assert (0 == ostr.tellp());

	{
		Poco::MemoryOutputStream ostr2(buffer.begin(), buffer.size());
		ostr2.seekp(4, std::ios_base::beg);
		ostr2.seekp(-5, std::ios_base::cur);
		assert (ostr2.fail());
	}
}


void MemoryStreamTest::testOutputSeekVsStringStream()
{
	Poco::Buffer<char> buffer(9);
	Poco::MemoryOutputStream mos(buffer.begin(), buffer.size());
	mos << '1' << '2' << '3' << '4' << '5' << '6' << '7' << '8' << '9';

	std::ostringstream oss;
	oss << '1' << '2' << '3' << '4' << '5' << '6' << '7' << '8' << '9';

	mos.seekp(4, std::ios_base::beg);
	oss.seekp(4, std::ios_base::beg);
	mos << 'a';
	oss << 'a';
	assert (oss.str()[4] == 'a');
	assert (buffer[4] == oss.str()[4]);
	assert (oss.tellp() == mos.tellp());

	mos.seekp(2, std::ios_base::cur);
	oss.seekp(2, std::ios_base::cur);
	mos << 'b';
	oss << 'b';
	assert (oss.str()[7] == 'b');
	assert (buffer[7] == oss.str()[7]);
	assert (oss.tellp() == mos.tellp());

	mos.seekp(-3, std::ios_base::end);
	oss.seekp(-3, std::ios_base::end);
	mos << 'c';
	oss << 'c';
	assert (oss.str()[6] == 'c');
	assert (buffer[6] == oss.str()[6]);
	assert (oss.tellp() == mos.tellp());

	mos.seekp(-2, std::ios_base::cur);
	oss.seekp(-2, std::ios_base::cur);
	mos << 'd';
	oss << 'd';
	assert (oss.str()[5] == 'd');
	assert (buffer[5] == oss.str()[5]);
	assert (oss.tellp() == mos.tellp());
}


void MemoryStreamTest::setUp()
{
}


void MemoryStreamTest::tearDown()
{
}


CppUnit::Test* MemoryStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MemoryStreamTest");

	CppUnit_addTest(pSuite, MemoryStreamTest, testInput);
	CppUnit_addTest(pSuite, MemoryStreamTest, testOutput);
	CppUnit_addTest(pSuite, MemoryStreamTest, testTell);
	CppUnit_addTest(pSuite, MemoryStreamTest, testInputSeek);
	CppUnit_addTest(pSuite, MemoryStreamTest, testInputSeekVsStringStream);
	CppUnit_addTest(pSuite, MemoryStreamTest, testOutputSeek);
	CppUnit_addTest(pSuite, MemoryStreamTest, testOutputSeekVsStringStream);

	return pSuite;
}
