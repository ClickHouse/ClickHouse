//
// MailMessageTest.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MailMessageTest.cpp#2 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MailMessageTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/MailMessage.h"
#include "Poco/Net/MailRecipient.h"
#include "Poco/Net/PartHandler.h"
#include "Poco/Net/StringPartSource.h"
#include "Poco/Net/PartStore.h"
#include "Poco/Net/MediaType.h"
#include "Poco/Timestamp.h"
#include "Poco/FileStream.h"
#include "Poco/String.h"
#include <sstream>
#include <vector>


using Poco::Net::MailMessage;
using Poco::Net::MailRecipient;
using Poco::Net::MessageHeader;
using Poco::Net::PartHandler;
using Poco::Net::MediaType;
using Poco::Net::StringPartSource;
using Poco::Net::FilePartStoreFactory;
using Poco::Net::FilePartStore;
using Poco::Timestamp;
using Poco::FileInputStream;
using Poco::replaceInPlace;
using Poco::icompare;


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
			_disp.push_back(header["Content-Disposition"]);
			_type.push_back(header["Content-Type"]);
			std::string data;
			int ch = stream.get();
			while (ch > 0)
			{
				data += (char) ch;
				ch = stream.get();
			}
			_data.push_back(data);
		}
		
		const std::vector<std::string>& data() const
		{
			return _data;
		}

		const std::vector<std::string>& disp() const
		{
			return _disp;
		}

		const std::vector<std::string>& type() const
		{
			return _type;
		}
		
	private:
		std::vector<std::string> _data;
		std::vector<std::string> _disp;
		std::vector<std::string> _type;
	};
}


MailMessageTest::MailMessageTest(const std::string& name): CppUnit::TestCase(name)
{
}


MailMessageTest::~MailMessageTest()
{
}


void MailMessageTest::testWriteQP()
{
	MailMessage message;
	MailRecipient r1(MailRecipient::PRIMARY_RECIPIENT, "john.doe@no.where", "John Doe");
	MailRecipient r2(MailRecipient::CC_RECIPIENT, "jane.doe@no.where", "Jane Doe");
	MailRecipient r3(MailRecipient::BCC_RECIPIENT, "walter.foo@no.where", "Frank Foo");
	MailRecipient r4(MailRecipient::BCC_RECIPIENT, "bernie.bar@no.where", "Bernie Bar");
	message.addRecipient(r1);
	message.addRecipient(r2);
	message.addRecipient(r3);
	message.addRecipient(r4);
	message.setSubject("Test Message");
	message.setSender("poco@appinf.com");
	message.setContent(
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
		"To test the quoted-printable encoding, we'll put an extra long line here. This should be enough.\r\n"
		"And here is some more =fe.\r\n"
	);
	Timestamp ts(0);
	message.setDate(ts);
	
	assert (!message.isMultipart());
	
	std::ostringstream str;
	message.write(str);
	std::string s = str.str();

	assert (s == 
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"Content-Type: text/plain\r\n"
		"Subject: Test Message\r\n"
		"From: poco@appinf.com\r\n"
		"Content-Transfer-Encoding: quoted-printable\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"CC: Jane Doe <jane.doe@no.where>\r\n"
		"\r\n"
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
		"To test the quoted-printable encoding, we'll put an extra long line here. T=\r\n"
		"his should be enough.\r\n"
		"And here is some more =3Dfe.\r\n"
	);
}


void MailMessageTest::testWrite8Bit()
{
	MailMessage message;
	MailRecipient r1(MailRecipient::PRIMARY_RECIPIENT, "john.doe@no.where", "John Doe");
	message.addRecipient(r1);
	message.setSubject("Test Message");
	message.setSender("poco@appinf.com");
	message.setContent(
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n",
		MailMessage::ENCODING_8BIT
	);
	Timestamp ts(0);
	message.setDate(ts);
	
	std::ostringstream str;
	message.write(str);
	std::string s = str.str();
	assert (s == 
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"Content-Type: text/plain\r\n"
		"Subject: Test Message\r\n"
		"From: poco@appinf.com\r\n"
		"Content-Transfer-Encoding: 8bit\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
	);
}


void MailMessageTest::testWriteBase64()
{
	MailMessage message;
	MailRecipient r1(MailRecipient::PRIMARY_RECIPIENT, "john.doe@no.where", "John Doe");
	message.addRecipient(r1);
	message.setSubject("Test Message");
	message.setSender("poco@appinf.com");
	message.setContent(
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n",
		MailMessage::ENCODING_BASE64
	);
	Timestamp ts(0);
	message.setDate(ts);

	std::ostringstream str;
	message.write(str);
	std::string s = str.str();
	assert (s == 
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"Content-Type: text/plain\r\n"
		"Subject: Test Message\r\n"
		"From: poco@appinf.com\r\n"
		"Content-Transfer-Encoding: base64\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"SGVsbG8sIHdvcmxkIQ0KVGhpcyBpcyBhIHRlc3QgZm9yIHRoZSBNYWlsTWVzc2FnZSBjbGFz\r\n"
		"cy4NCg=="
	);
}


void MailMessageTest::testWriteManyRecipients()
{
	MailMessage message;
	MailRecipient r1(MailRecipient::PRIMARY_RECIPIENT, "john.doe@no.where", "John Doe");
	MailRecipient r2(MailRecipient::PRIMARY_RECIPIENT, "jane.doe@no.where", "Jane Doe");
	MailRecipient r3(MailRecipient::PRIMARY_RECIPIENT, "walter.foo@no.where", "Frank Foo");
	MailRecipient r4(MailRecipient::PRIMARY_RECIPIENT, "bernie.bar@no.where", "Bernie Bar");
	MailRecipient r5(MailRecipient::PRIMARY_RECIPIENT, "joe.spammer@no.where", "Joe Spammer");
	message.addRecipient(r1);
	message.addRecipient(r2);
	message.addRecipient(r3);
	message.addRecipient(r4);
	message.addRecipient(r5);
	message.setSubject("Test Message");
	message.setSender("poco@appinf.com");
	message.setContent(
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n",
		MailMessage::ENCODING_8BIT
	);
	Timestamp ts(0);
	message.setDate(ts);
	
	std::ostringstream str;
	message.write(str);
	std::string s = str.str();
	assert (s == 
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"Content-Type: text/plain\r\n"
		"Subject: Test Message\r\n"
		"From: poco@appinf.com\r\n"
		"Content-Transfer-Encoding: 8bit\r\n"
		"To: John Doe <john.doe@no.where>, Jane Doe <jane.doe@no.where>, \r\n"
		"\tFrank Foo <walter.foo@no.where>, Bernie Bar <bernie.bar@no.where>, \r\n"
		"\tJoe Spammer <joe.spammer@no.where>\r\n"
		"\r\n"
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
	);
}


void MailMessageTest::testWriteMultiPart()
{
	MailMessage message;
	MailRecipient r1(MailRecipient::PRIMARY_RECIPIENT, "john.doe@no.where", "John Doe");
	message.addRecipient(r1);
	message.setSubject("Test Message");
	message.setSender("poco@appinf.com");
	Timestamp ts(0);
	message.setDate(ts);
	message.addContent(new StringPartSource("Hello World!\r\n", "text/plain"), MailMessage::ENCODING_8BIT);
	StringPartSource* pSPS = new StringPartSource("This is some binary data. Really.", "application/octet-stream", "sample.dat");
	pSPS->headers().set("Content-ID", "abcd1234");
	message.addAttachment("sample", pSPS);

	assert (message.isMultipart());

	std::ostringstream str;
	message.write(str);
	std::string s = str.str();
	std::string rawMsg(
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"Content-Type: multipart/mixed; boundary=$\r\n"
		"Subject: Test Message\r\n"
		"From: poco@appinf.com\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"Mime-Version: 1.0\r\n"
		"\r\n"
		"--$\r\n"
		"Content-Type: text/plain\r\n"
		"Content-Transfer-Encoding: 8bit\r\n"
		"Content-Disposition: inline\r\n"
		"\r\n"
		"Hello World!\r\n"
		"\r\n"
		"--$\r\n"
		"Content-ID: abcd1234\r\n"
		"Content-Type: application/octet-stream; name=sample\r\n"
		"Content-Transfer-Encoding: base64\r\n"
		"Content-Disposition: attachment; filename=sample.dat\r\n"
		"\r\n"
		"VGhpcyBpcyBzb21lIGJpbmFyeSBkYXRhLiBSZWFsbHku\r\n"
		"--$--\r\n"
	);
	std::string::size_type p1 = s.find('=') + 1;
	std::string::size_type p2 = s.find('\r', p1);
	std::string boundary(s, p1, p2 - p1);
	std::string msg;
	for (std::string::const_iterator it = rawMsg.begin(); it != rawMsg.end(); ++it)
	{
		if (*it == '$')
			msg += boundary;
		else
			msg += *it;
	}

	assert (s == msg);
}


void MailMessageTest::testReadQP()
{
	std::istringstream istr(
		"Content-Transfer-Encoding: quoted-printable\r\n"
		"Content-Type: text/plain\r\n"
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"From: poco@appinf.com\r\n"
		"Subject: Test Message\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
		"To test the quoted-printable encoding, we'll put an extra long line here. T=\r\n"
		"his should be enough.\r\n"
		"And here is some more =3Dfe.\r\n"
	);
	
	MailMessage message;
	message.read(istr);
	
	assert (message.getSender() == "poco@appinf.com");
	assert (message.getContentType() == "text/plain");
	assert (message.getContent() == 
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
		"To test the quoted-printable encoding, we'll put an extra long line here. This should be enough.\r\n"
		"And here is some more =fe.\r\n"
	);
}


void MailMessageTest::testReadDefaultTransferEncoding()
{
	std::istringstream istr(
		"Content-Type: text/plain\r\n"
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"From: poco@appinf.com\r\n"
		"Subject: Test Message\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
	);

	MailMessage message;
	message.read(istr);

	assert (message.getSender() == "poco@appinf.com");
	assert (message.getContentType() == "text/plain");
	assert (message.getContent() ==
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
	);
}


void MailMessageTest::testRead8Bit()
{
	std::istringstream istr(
		"Content-Transfer-Encoding: 8bit\r\n"
		"Content-Type: text/plain\r\n"
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"From: poco@appinf.com\r\n"
		"Subject: Test Message\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
	);
	
	MailMessage message;
	message.read(istr);
	
	assert (message.getSender() == "poco@appinf.com");
	assert (message.getContentType() == "text/plain");
	assert (message.getContent() == 
		"Hello, world!\r\n"
		"This is a test for the MailMessage class.\r\n"
	);
}


void MailMessageTest::testReadMultiPart()
{
	std::istringstream istr(
		"Content-Type: multipart/mixed; boundary=MIME_boundary_01234567\r\n"
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"From: poco@appinf.com\r\n"
		"Mime-Version: 1.0\r\n"
		"Subject: Test Message\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"\r\n"
		"--MIME_boundary_01234567\r\n"
		"Content-Disposition: inline\r\n"
		"Content-Transfer-Encoding: 8bit\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"Hello World!\r\n"
		"\r\n"
		"--MIME_boundary_01234567\r\n"
		"Content-Disposition: attachment; filename=sample.dat\r\n"
		"Content-Transfer-Encoding: base64\r\n"
		"Content-Type: application/octet-stream; name=sample\r\n"
		"\r\n"
		"VGhpcyBpcyBzb21lIGJpbmFyeSBkYXRhLiBSZWFsbHku\r\n"
		"--MIME_boundary_01234567--\r\n"
	);
	
	StringPartHandler handler;
	MailMessage message;
	message.read(istr, handler);
	
	assert (handler.data().size() == 2);
	assert (handler.data()[0] == "Hello World!\r\n");
	assert (handler.type()[0] == "text/plain");
	assert (handler.disp()[0] == "inline");

	assert (handler.data()[1] == "This is some binary data. Really.");
	assert (handler.type()[1] == "application/octet-stream; name=sample");
	assert (handler.disp()[1] == "attachment; filename=sample.dat");
}


void MailMessageTest::testReadMultiPartDefaultTransferEncoding()
{
	std::istringstream istr(
		"Content-Type: multipart/mixed; boundary=MIME_boundary_01234567\r\n"
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"From: poco@appinf.com\r\n"
		"Mime-Version: 1.0\r\n"
		"Subject: Test Message\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"\r\n"
		"--MIME_boundary_01234567\r\n"
		"Content-Disposition: inline\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"Hello World!\r\n"
		"\r\n"
		"--MIME_boundary_01234567\r\n"
		"Content-Disposition: attachment; filename=sample.dat\r\n"
		"Content-Transfer-Encoding: base64\r\n"
		"Content-Type: application/octet-stream; name=sample\r\n"
		"\r\n"
		"VGhpcyBpcyBzb21lIGJpbmFyeSBkYXRhLiBSZWFsbHku\r\n"
		"--MIME_boundary_01234567--\r\n"
	);

	StringPartHandler handler;
	MailMessage message;
	message.read(istr, handler);

	assert (handler.data().size() == 2);
	assert (handler.data()[0] == "Hello World!\r\n");
	assert (handler.type()[0] == "text/plain");
	assert (handler.disp()[0] == "inline");

	assert (handler.data()[1] == "This is some binary data. Really.");
	assert (handler.type()[1] == "application/octet-stream; name=sample");
	assert (handler.disp()[1] == "attachment; filename=sample.dat");
}


void MailMessageTest::testReadWriteMultiPart()
{
	std::string msgin(
		"Content-Type: multipart/mixed; boundary=MIME_boundary_31E8A8D61DF53389\r\n"
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"From: poco@appinf.com\r\n"
		"Mime-Version: 1.0\r\n"
		"Subject: Test Message\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"--MIME_boundary_31E8A8D61DF53389\r\n"
		"Content-Disposition: inline\r\n"
		"Content-Transfer-Encoding: 8bit\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"Hello World!\r\n"
		"\r\n"
		"--MIME_boundary_31E8A8D61DF53389\r\n"
		"Content-Disposition: attachment; filename=sample.dat\r\n"
		"Content-ID: abcd1234\r\n"
		"Content-Transfer-Encoding: base64\r\n"
		"Content-Type: application/octet-stream; name=sample\r\n"
		"\r\n"
		"VGhpcyBpcyBzb21lIGJpbmFyeSBkYXRhLiBSZWFsbHku\r\n"
		"--MIME_boundary_31E8A8D61DF53389--\r\n"
	);

	std::istringstream istr(msgin);
	std::ostringstream ostr;
	MailMessage message;

	message.read(istr);
	message.write(ostr);
	
	std::string msgout(ostr.str());
	assert (msgout == msgin);
}


void MailMessageTest::testReadWriteMultiPartStore()
{
	std::string msgin(
		"Content-Type: multipart/mixed; boundary=MIME_boundary_31E8A8D61DF53389\r\n"
		"Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n"
		"From: poco@appinf.com\r\n"
		"Mime-Version: 1.0\r\n"
		"Subject: Test Message\r\n"
		"To: John Doe <john.doe@no.where>\r\n"
		"\r\n"
		"--MIME_boundary_31E8A8D61DF53389\r\n"
		"Content-Disposition: inline\r\n"
		"Content-Transfer-Encoding: 8bit\r\n"
		"Content-Type: text/plain\r\n"
		"\r\n"
		"Hello World!\r\n"
		"\r\n"
		"--MIME_boundary_31E8A8D61DF53389\r\n"
		"Content-Disposition: attachment; filename=sample.dat\r\n"
		"Content-ID: abcd1234\r\n"
		"Content-Transfer-Encoding: base64\r\n"
		"Content-Type: application/octet-stream; name=sample\r\n"
		"\r\n"
		"VGhpcyBpcyBzb21lIGJpbmFyeSBkYXRhLiBSZWFsbHku\r\n"
		"--MIME_boundary_31E8A8D61DF53389--\r\n"
	);

	std::istringstream istr(msgin);
	std::ostringstream ostr;
	FilePartStoreFactory pfsf;
	MailMessage message(&pfsf);

	message.read(istr);
	
	MailMessage::PartVec::const_iterator it = message.parts().begin();
	MailMessage::PartVec::const_iterator end = message.parts().end();
	for (; it != end; ++it)
	{
		FilePartStore* fps = dynamic_cast<FilePartStore*>(it->pSource);
		if (fps && fps->filename().size())
		{
			std::string filename = fps->filename();
			assert (filename == "sample.dat");
			std::string path = fps->path();
			// for security reasons, the filesystem temporary
			// filename is not the same as attachment name
			std::size_t sz = (path.size() > filename.size()) ? filename.size() : path.size();
			assert (0 != icompare(path, path.size() - sz, sz, path));
			
			Poco::FileInputStream fis(path);
			assert (fis.good());
			std::string read;
			std::string line;
			while (std::getline(fis, line)) read += line;

			assert (!read.empty());
			assert (read == "This is some binary data. Really.");
		}
	}
	
	message.write(ostr);
	std::string msgout(ostr.str());
	assert (msgout == msgin);
}


void MailMessageTest::testEncodeWord()
{
	std::string plain("this is pure ASCII");
	std::string encoded = MailMessage::encodeWord(plain, "ISO-8859-1");
	assert (encoded == plain);
	
	plain = "This text contains German Umlauts: \304\326";
	encoded = MailMessage::encodeWord(plain, "ISO-8859-1");
	assert (encoded == "=?ISO-8859-1?q?This_text_contains_German_Umlauts=3A_=C4=D6?=");
	
	plain = "This text contains German Umlauts: \304\326. "
	        "It is also a very long text. Longer than 75 "
	        "characters. Long enough to become three lines "
	        "after being word-encoded.";
	encoded = MailMessage::encodeWord(plain, "ISO-8859-1");
	assert (encoded == "=?ISO-8859-1?q?This_text_contains_German_Umlauts=3A_=C4=D6=2E_It_?=\r\n"
	                   " =?ISO-8859-1?q?is_also_a_very_long_text=2E_Longer_than_75_characters=2E_?=\r\n"
	                   " =?ISO-8859-1?q?Long_enough_to_become_three_lines_after_being_word-encode?=\r\n"
	                   " =?ISO-8859-1?q?d=2E?=");
}


void MailMessageTest::setUp()
{
}


void MailMessageTest::tearDown()
{
}


CppUnit::Test* MailMessageTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MailMessageTest");

	CppUnit_addTest(pSuite, MailMessageTest, testWriteQP);
	CppUnit_addTest(pSuite, MailMessageTest, testWrite8Bit);
	CppUnit_addTest(pSuite, MailMessageTest, testWriteBase64);
	CppUnit_addTest(pSuite, MailMessageTest, testWriteManyRecipients);
	CppUnit_addTest(pSuite, MailMessageTest, testWriteMultiPart);
	CppUnit_addTest(pSuite, MailMessageTest, testReadQP);
	CppUnit_addTest(pSuite, MailMessageTest, testReadDefaultTransferEncoding);
	CppUnit_addTest(pSuite, MailMessageTest, testRead8Bit);
	CppUnit_addTest(pSuite, MailMessageTest, testReadMultiPart);
	CppUnit_addTest(pSuite, MailMessageTest, testReadMultiPartDefaultTransferEncoding);
	CppUnit_addTest(pSuite, MailMessageTest, testReadWriteMultiPart);
	CppUnit_addTest(pSuite, MailMessageTest, testReadWriteMultiPartStore);
	CppUnit_addTest(pSuite, MailMessageTest, testEncodeWord);

	return pSuite;
}
