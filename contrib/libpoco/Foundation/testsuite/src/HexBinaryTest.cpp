//
// HexBinaryTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/HexBinaryTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HexBinaryTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/HexBinaryEncoder.h"
#include "Poco/HexBinaryDecoder.h"
#include "Poco/Exception.h"
#include <sstream>


using Poco::HexBinaryEncoder;
using Poco::HexBinaryDecoder;
using Poco::DataFormatException;


HexBinaryTest::HexBinaryTest(const std::string& name): CppUnit::TestCase(name)
{
}


HexBinaryTest::~HexBinaryTest()
{
}


void HexBinaryTest::testEncoder()
{
	{
		std::ostringstream str;
		HexBinaryEncoder encoder(str);
		encoder << std::string("\00\01\02\03\04\05", 6);
		encoder.close();
		assert (str.str() == "000102030405");
	}
	{
		std::ostringstream str;
		HexBinaryEncoder encoder(str);
		encoder << std::string("\00\01\02\03", 4);
		encoder.close();
		assert (str.str() == "00010203");
	}
	{
		std::ostringstream str;
		HexBinaryEncoder encoder(str);
		encoder << "ABCDEF";
		encoder << char(0xaa) << char(0xbb);
		encoder.close();
		assert (str.str() == "414243444546aabb");
	}
	{
		std::ostringstream str;
		HexBinaryEncoder encoder(str);
		encoder.rdbuf()->setUppercase();
		encoder << "ABCDEF";
		encoder << char(0xaa) << char(0xbb);
		encoder.close();
		assert (str.str() == "414243444546AABB");
	}
}


void HexBinaryTest::testDecoder()
{
	{
		std::istringstream istr("000102030405");
		HexBinaryDecoder decoder(istr);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == 4);
		assert (decoder.good() && decoder.get() == 5);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("0001020304");
		HexBinaryDecoder decoder(istr);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == 4);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("0a0bcdef");
		HexBinaryDecoder decoder(istr);
		assert (decoder.good() && decoder.get() == 0x0a);
		assert (decoder.good() && decoder.get() == 0x0b);
		assert (decoder.good() && decoder.get() == 0xcd);
		assert (decoder.good() && decoder.get() == 0xef);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("0A0BCDEF");
		HexBinaryDecoder decoder(istr);
		assert (decoder.good() && decoder.get() == 0x0a);
		assert (decoder.good() && decoder.get() == 0x0b);
		assert (decoder.good() && decoder.get() == 0xcd);
		assert (decoder.good() && decoder.get() == 0xef);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("00 01 02 03");
		HexBinaryDecoder decoder(istr);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("414243444546");
		HexBinaryDecoder decoder(istr);
		std::string s;
		decoder >> s;
		assert (s == "ABCDEF");
		assert (decoder.eof());
		assert (!decoder.fail());
	}
	{
		std::istringstream istr("4041\r\n4243\r\n4445");
		HexBinaryDecoder decoder(istr);
		std::string s;
		decoder >> s;
		assert (s == "@ABCDE");
		assert (decoder.eof());
		assert (!decoder.fail());
	}
	{
		std::istringstream istr("AABB#CCDD");
		HexBinaryDecoder decoder(istr);
		std::string s;
		try
		{
			decoder >> s;
			assert (decoder.bad());
		}
		catch (DataFormatException&)
		{
		}
		assert (!decoder.eof());
	}
}


void HexBinaryTest::testEncodeDecode()
{
	{
		std::stringstream str;
		HexBinaryEncoder encoder(str);
		encoder << "The quick brown fox ";
		encoder << "jumped over the lazy dog.";
		encoder.close();
		HexBinaryDecoder decoder(str);
		std::string s;
		int c = decoder.get();
		while (c != -1) { s += char(c); c = decoder.get(); }
		assert (s == "The quick brown fox jumped over the lazy dog.");
	}
	{
		std::string src;
		for (int i = 0; i < 255; ++i) src += char(i);
		std::stringstream str;
		HexBinaryEncoder encoder(str);
		encoder.write(src.data(), (std::streamsize) src.size());
		encoder.close();
		HexBinaryDecoder decoder(str);
		std::string s;
		int c = decoder.get();
		while (c != -1) { s += char(c); c = decoder.get(); }
		assert (s == src);
	}
}


void HexBinaryTest::setUp()
{
}


void HexBinaryTest::tearDown()
{
}


CppUnit::Test* HexBinaryTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HexBinaryTest");

	CppUnit_addTest(pSuite, HexBinaryTest, testEncoder);
	CppUnit_addTest(pSuite, HexBinaryTest, testDecoder);
	CppUnit_addTest(pSuite, HexBinaryTest, testEncodeDecode);

	return pSuite;
}
