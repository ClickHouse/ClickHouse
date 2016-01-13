//
// HTMLFormTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/HTMLFormTest.cpp#3 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HTMLFormTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/PartSource.h"
#include "Poco/Net/StringPartSource.h"
#include "Poco/Net/PartHandler.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/NetException.h"
#include <sstream>


using Poco::Net::HTMLForm;
using Poco::Net::PartSource;
using Poco::Net::StringPartSource;
using Poco::Net::PartHandler;
using Poco::Net::HTTPRequest;
using Poco::Net::HTTPMessage;
using Poco::Net::MessageHeader;


namespace
{
	class StringPartHandler: public PartHandler
	{
	public:
		StringPartHandler()
		{
		}
		
		void handlePart(const MessageHeader& header, std::istream& stream)
		{
			_disp = header["Content-Disposition"];
			_type = header["Content-Type"];
			int ch = stream.get();
			while (ch > 0)
			{
				_data += (char) ch;
				ch = stream.get();
			}
		}
		
		const std::string& data() const
		{
			return _data;
		}

		const std::string& disp() const
		{
			return _disp;
		}

		const std::string& type() const
		{
			return _type;
		}
		
	private:
		std::string _data;
		std::string _disp;
		std::string _type;
	};
}


HTMLFormTest::HTMLFormTest(const std::string& name): CppUnit::TestCase(name)
{
}


HTMLFormTest::~HTMLFormTest()
{
}


void HTMLFormTest::testWriteUrl()
{
	HTMLForm form;
	form.set("field1", "value1");
	form.set("field2", "value 2");
	form.set("field3", "value=3");
	form.set("field4", "value&4");
	form.set("field5", "value+5");
	
	std::ostringstream ostr;
	form.write(ostr);
	std::string s = ostr.str();
	assert (s == "field1=value1&field2=value%202&field3=value%3D3&field4=value%264&field5=value%2B5");
}


void HTMLFormTest::testWriteMultipart()
{
	HTMLForm form(HTMLForm::ENCODING_MULTIPART);
	form.set("field1", "value1");
	form.set("field2", "value 2");
	form.set("field3", "value=3");
	form.set("field4", "value&4");
	
	form.addPart("attachment1", new StringPartSource("This is an attachment"));
	StringPartSource* pSPS = new StringPartSource("This is another attachment", "text/plain", "att2.txt");
	pSPS->headers().set("Content-ID", "1234abcd");
	form.addPart("attachment2", pSPS);
	
	std::ostringstream ostr;
	form.write(ostr, "MIME_boundary_0123456789");
	std::string s = ostr.str();
	assert (s == 
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field1\"\r\n"
		"\r\n"
		"value1\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field2\"\r\n"
		"\r\n"
		"value 2\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field3\"\r\n"
		"\r\n"
		"value=3\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field4\"\r\n"
		"\r\n"
		"value&4\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"attachment1\"\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"This is an attachment\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-ID: 1234abcd\r\n"
		"Content-Disposition: form-data; name=\"attachment2\"; filename=\"att2.txt\"\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"This is another attachment\r\n"
		"--MIME_boundary_0123456789--\r\n"
	);
	assert(s.length() == form.calculateContentLength());
}


void HTMLFormTest::testReadUrlGET()
{
	HTTPRequest req("GET", "/form.cgi?field1=value1&field2=value%202&field3=value%3D3&field4=value%264");
	HTMLForm form(req);
	assert (form.size() == 4);
	assert (form["field1"] == "value1");
	assert (form["field2"] == "value 2");
	assert (form["field3"] == "value=3");
	assert (form["field4"] == "value&4");
}


void HTMLFormTest::testReadUrlPOST()
{
	HTTPRequest req("POST", "/form.cgi?field0=value0");
	std::istringstream istr("field1=value1&field2=value%202&field3=value%3D3&field4=value%264");
	HTMLForm form(req, istr);
	assert (form.size() == 5);
	assert (form["field0"] == "value0");
	assert (form["field1"] == "value1");
	assert (form["field2"] == "value 2");
	assert (form["field3"] == "value=3");
	assert (form["field4"] == "value&4");
}


void HTMLFormTest::testReadUrlPUT()
{
	HTTPRequest req("PUT", "/form.cgi?field0=value0");
	std::istringstream istr("field1=value1&field2=value%202&field3=value%3D3&field4=value%264");
	HTMLForm form(req, istr);
	assert (form.size() == 5);
	assert (form["field0"] == "value0");
	assert (form["field1"] == "value1");
	assert (form["field2"] == "value 2");
	assert (form["field3"] == "value=3");
	assert (form["field4"] == "value&4");
}


void HTMLFormTest::testReadUrlBOM()
{
	HTTPRequest req("PUT", "/form.cgi?field0=value0");
	std::istringstream istr("\357\273\277field1=value1&field2=value%202&field3=value%3D3&field4=value%264");
	HTMLForm form(req, istr);
	assert (form.size() == 5);
	assert (form["field0"] == "value0");
	assert (form["field1"] == "value1");
	assert (form["field2"] == "value 2");
	assert (form["field3"] == "value=3");
	assert (form["field4"] == "value&4");
}


void HTMLFormTest::testReadMultipart()
{
	std::istringstream istr(
		"\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field1\"\r\n"
		"\r\n"
		"value1\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field2\"\r\n"
		"\r\n"
		"value 2\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field3\"\r\n"
		"\r\n"
		"value=3\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field4\"\r\n"
		"\r\n"
		"value&4\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: file; name=\"attachment1\"; filename=\"att1.txt\"\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"This is an attachment\r\n"
		"--MIME_boundary_0123456789--\r\n"
	);
	HTTPRequest req("POST", "/form.cgi");
	req.setContentType(HTMLForm::ENCODING_MULTIPART + "; boundary=\"MIME_boundary_0123456789\"");
	StringPartHandler sah;
	HTMLForm form(req, istr, sah);	
	assert (form.size() == 4);
	assert (form["field1"] == "value1");
	assert (form["field2"] == "value 2");
	assert (form["field3"] == "value=3");
	assert (form["field4"] == "value&4");

	assert (sah.type() == "text/plain");
	assert (sah.disp() == "file; name=\"attachment1\"; filename=\"att1.txt\"");
	assert (sah.data() == "This is an attachment");
}


void HTMLFormTest::testSubmit1()
{
	HTMLForm form;
	form.set("field1", "value1");
	form.set("field2", "value 2");
	form.set("field3", "value=3");
	form.set("field4", "value&4");
	
	HTTPRequest req("GET", "/form.cgi");
	form.prepareSubmit(req);
	assert (req.getURI() == "/form.cgi?field1=value1&field2=value%202&field3=value%3D3&field4=value%264");
}


void HTMLFormTest::testSubmit2()
{
	HTMLForm form;
	form.set("field1", "value1");
	form.set("field2", "value 2");
	form.set("field3", "value=3");
	form.set("field4", "value&4");
	
	HTTPRequest req("POST", "/form.cgi");
	form.prepareSubmit(req);
	assert (req.getContentType() == HTMLForm::ENCODING_URL);
}


void HTMLFormTest::testSubmit3()
{
	HTMLForm form(HTMLForm::ENCODING_MULTIPART);
	form.set("field1", "value1");
	form.set("field2", "value 2");
	form.set("field3", "value=3");
	form.set("field4", "value&4");
	
	HTTPRequest req("POST", "/form.cgi", HTTPMessage::HTTP_1_1);
	form.prepareSubmit(req);
	std::string expCT(HTMLForm::ENCODING_MULTIPART);
	expCT.append("; boundary=\"");
	expCT.append(form.boundary());
	expCT.append("\"");
	assert (req.getContentType() == expCT);
	assert (req.getChunkedTransferEncoding());
}


void HTMLFormTest::testFieldLimitUrl()
{
	HTTPRequest req("GET", "/form.cgi?field1=value1&field2=value%202&field3=value%3D3&field4=value%264");
	HTMLForm form;
	form.setFieldLimit(3);
	try
	{
		form.load(req);
		fail("field limit violated - must throw");
	}
	catch (Poco::Net::HTMLFormException&)
	{
	}
}


void HTMLFormTest::testFieldLimitMultipart()
{
	std::istringstream istr(
		"\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field1\"\r\n"
		"\r\n"
		"value1\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field2\"\r\n"
		"\r\n"
		"value 2\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field3\"\r\n"
		"\r\n"
		"value=3\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: form-data; name=\"field4\"\r\n"
		"\r\n"
		"value&4\r\n"
		"--MIME_boundary_0123456789\r\n"
		"Content-Disposition: file; name=\"attachment1\"; filename=\"att1.txt\"\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"This is an attachment\r\n"
		"--MIME_boundary_0123456789--\r\n"
	);
	HTTPRequest req("POST", "/form.cgi");
	req.setContentType(HTMLForm::ENCODING_MULTIPART + "; boundary=\"MIME_boundary_0123456789\"");
	StringPartHandler sah;
	HTMLForm form;
	form.setFieldLimit(3);
	try
	{
		form.load(req, istr, sah);	
		fail("field limit violated - must throw");
	}
	catch (Poco::Net::HTMLFormException&)
	{
	}
}


void HTMLFormTest::setUp()
{
}


void HTMLFormTest::tearDown()
{
}


CppUnit::Test* HTMLFormTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HTMLFormTest");

	CppUnit_addTest(pSuite, HTMLFormTest, testWriteUrl);
	CppUnit_addTest(pSuite, HTMLFormTest, testWriteMultipart);
	CppUnit_addTest(pSuite, HTMLFormTest, testReadUrlGET);
	CppUnit_addTest(pSuite, HTMLFormTest, testReadUrlPOST);
	CppUnit_addTest(pSuite, HTMLFormTest, testReadUrlPUT);
	CppUnit_addTest(pSuite, HTMLFormTest, testReadUrlBOM);
	CppUnit_addTest(pSuite, HTMLFormTest, testReadMultipart);
	CppUnit_addTest(pSuite, HTMLFormTest, testSubmit1);
	CppUnit_addTest(pSuite, HTMLFormTest, testSubmit2);
	CppUnit_addTest(pSuite, HTMLFormTest, testSubmit3);
	CppUnit_addTest(pSuite, HTMLFormTest, testFieldLimitUrl);
	CppUnit_addTest(pSuite, HTMLFormTest, testFieldLimitMultipart);

	return pSuite;
}
