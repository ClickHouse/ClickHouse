//
// Base64Test.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Base64Test.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Base64Encoder.h"
#include "Poco/Base64Decoder.h"
#include "Poco/Exception.h"
#include <sstream>


using Poco::Base64Encoder;
using Poco::Base64Decoder;
using Poco::DataFormatException;


Base64Test::Base64Test(const std::string& name): CppUnit::TestCase(name)
{
}


Base64Test::~Base64Test()
{
}


void Base64Test::testEncoder()
{
	{
		std::ostringstream str;
		Base64Encoder encoder(str);
		encoder << std::string("\00\01\02\03\04\05", 6);
		encoder.close();
		assert (str.str() == "AAECAwQF");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str);
		encoder << std::string("\00\01\02\03", 4);
		encoder.close();
		assert (str.str() == "AAECAw==");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str);
		encoder << "ABCDEF";
		encoder.close();
		assert (str.str() == "QUJDREVG");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str);
		encoder << "!@#$%^&*()_~<>";
		encoder.close();
		assert (str.str() == "IUAjJCVeJiooKV9+PD4=");
	}
}


void Base64Test::testEncoderURL()
{
	{
		std::ostringstream str;
		Base64Encoder encoder(str, Poco::BASE64_URL_ENCODING);
		encoder << std::string("\00\01\02\03\04\05", 6);
		encoder.close();
		assert (str.str() == "AAECAwQF");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str, Poco::BASE64_URL_ENCODING);
		encoder << std::string("\00\01\02\03", 4);
		encoder.close();
		assert (str.str() == "AAECAw==");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str, Poco::BASE64_URL_ENCODING);
		encoder << "ABCDEF";
		encoder.close();
		assert (str.str() == "QUJDREVG");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str, Poco::BASE64_URL_ENCODING);
		encoder << "!@#$%^&*()_~<>";
		encoder.close();
		assert (str.str() == "IUAjJCVeJiooKV9-PD4=");
	}
}


void Base64Test::testEncoderNoPadding()
{
	{
		std::ostringstream str;
		Base64Encoder encoder(str, Poco::BASE64_URL_ENCODING | Poco::BASE64_NO_PADDING);
		encoder << std::string("\00\01\02\03\04\05", 6);
		encoder.close();
		assert (str.str() == "AAECAwQF");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str, Poco::BASE64_URL_ENCODING | Poco::BASE64_NO_PADDING);
		encoder << std::string("\00\01\02\03", 4);
		encoder.close();
		assert (str.str() == "AAECAw");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str, Poco::BASE64_URL_ENCODING | Poco::BASE64_NO_PADDING);
		encoder << "ABCDEF";
		encoder.close();
		assert (str.str() == "QUJDREVG");
	}
	{
		std::ostringstream str;
		Base64Encoder encoder(str, Poco::BASE64_URL_ENCODING | Poco::BASE64_NO_PADDING);
		encoder << "!@#$%^&*()_~<>";
		encoder.close();
		assert (str.str() == "IUAjJCVeJiooKV9-PD4");
	}
}


void Base64Test::testDecoder()
{
	{
		std::istringstream istr("AAECAwQF");
		Base64Decoder decoder(istr);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == 4);
		assert (decoder.good() && decoder.get() == 5);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("AAECAwQ=");
		Base64Decoder decoder(istr);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == 4);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("AAECAw==");
		Base64Decoder decoder(istr);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("QUJDREVG");
		Base64Decoder decoder(istr);
		std::string s;
		decoder >> s;
		assert (s == "ABCDEF");
		assert (decoder.eof());
		assert (!decoder.fail());
	}
	{
		std::istringstream istr("QUJ\r\nDRE\r\nVG");
		Base64Decoder decoder(istr);
		std::string s;
		decoder >> s;
		assert (s == "ABCDEF");
		assert (decoder.eof());
		assert (!decoder.fail());
	}
	{
		std::istringstream istr("QUJD#REVG");
		Base64Decoder decoder(istr);
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


void Base64Test::testDecoderURL()
{
	{
		std::istringstream istr("AAECAwQF");
		Base64Decoder decoder(istr, Poco::BASE64_URL_ENCODING);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == 4);
		assert (decoder.good() && decoder.get() == 5);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("AAECAwQ=");
		Base64Decoder decoder(istr, Poco::BASE64_URL_ENCODING);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == 4);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("AAECAw==");
		Base64Decoder decoder(istr, Poco::BASE64_URL_ENCODING);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("QUJDREVG");
		Base64Decoder decoder(istr, Poco::BASE64_URL_ENCODING);
		std::string s;
		decoder >> s;
		assert (s == "ABCDEF");
		assert (decoder.eof());
		assert (!decoder.fail());
	}
	{
		std::istringstream istr("QUJ\r\nDRE\r\nVG");
		Base64Decoder decoder(istr, Poco::BASE64_URL_ENCODING);
		try
		{
			std::string s;
			decoder >> s;
			assert (decoder.bad());
		}
		catch (DataFormatException&)
		{
		}
	}
	{
		std::istringstream istr("QUJD#REVG");
		Base64Decoder decoder(istr, Poco::BASE64_URL_ENCODING);
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
	{
		std::istringstream istr("IUAjJCVeJiooKV9-PD4=");
		Base64Decoder decoder(istr, Poco::BASE64_URL_ENCODING);
		std::string s;
		decoder >> s;
		assert (s == "!@#$%^&*()_~<>");
		assert (decoder.eof());
		assert (!decoder.fail());
	}
}


void Base64Test::testDecoderNoPadding()
{
	{
		std::istringstream istr("AAECAwQF");
		Base64Decoder decoder(istr, Poco::BASE64_NO_PADDING);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == 4);
		assert (decoder.good() && decoder.get() == 5);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("AAECAwQ");
		Base64Decoder decoder(istr, Poco::BASE64_NO_PADDING);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == 4);
		assert (decoder.good() && decoder.get() == -1);
	}
	{
		std::istringstream istr("AAECAw");
		Base64Decoder decoder(istr, Poco::BASE64_NO_PADDING);
		assert (decoder.good() && decoder.get() == 0);
		assert (decoder.good() && decoder.get() == 1);
		assert (decoder.good() && decoder.get() == 2);
		assert (decoder.good() && decoder.get() == 3);
		assert (decoder.good() && decoder.get() == -1);
	}
}


void Base64Test::testEncodeDecode()
{
	{
		std::stringstream str;
		Base64Encoder encoder(str);
		encoder << "The quick brown fox ";
		encoder << "jumped over the lazy dog.";
		encoder.close();
		Base64Decoder decoder(str);
		std::string s;
		int c = decoder.get();
		while (c != -1) { s += char(c); c = decoder.get(); }
		assert (s == "The quick brown fox jumped over the lazy dog.");
	}
	{
		std::string src;
		for (int i = 0; i < 255; ++i) src += char(i);
		std::stringstream str;
		Base64Encoder encoder(str);
		encoder.write(src.data(), (std::streamsize) src.size());
		encoder.close();
		Base64Decoder decoder(str);
		std::string s;
		int c = decoder.get();
		while (c != -1) { s += char(c); c = decoder.get(); }
		assert (s == src);
	}
}


void Base64Test::setUp()
{
}


void Base64Test::tearDown()
{
}


CppUnit::Test* Base64Test::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("Base64Test");

	CppUnit_addTest(pSuite, Base64Test, testEncoder);
	CppUnit_addTest(pSuite, Base64Test, testEncoderURL);
	CppUnit_addTest(pSuite, Base64Test, testEncoderNoPadding);
	CppUnit_addTest(pSuite, Base64Test, testDecoder);
	CppUnit_addTest(pSuite, Base64Test, testDecoderURL);
	CppUnit_addTest(pSuite, Base64Test, testDecoderNoPadding);
	CppUnit_addTest(pSuite, Base64Test, testEncodeDecode);

	return pSuite;
}
