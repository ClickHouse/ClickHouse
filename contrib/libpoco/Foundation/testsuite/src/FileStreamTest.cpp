//
// FileStreamTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/FileStreamTest.cpp#1 $
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FileStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/FileStream.h"
#include "Poco/File.h"
#include "Poco/TemporaryFile.h"
#include "Poco/Exception.h"


FileStreamTest::FileStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


FileStreamTest::~FileStreamTest()
{
}


void FileStreamTest::testRead()
{
#if defined(POCO_OS_FAMILY_WINDOWS) && !defined(POCO_WIN32_UTF8)
	char tmp[]={'\xc4', '\xd6', '\xdc', '\xe4', '\xf6', '\xfc', '\0'};
	std::string file(tmp);
	file.append(".txt");
#elif defined(POCO_OS_FAMILY_WINDOWS)
	char tmp[]={'\xc3', '\x84', '\xc3', '\x96', '\xc3', '\x9c', '\xc3', '\xa4', '\xc3', '\xb6', '\xc3', '\xbc', '\0'};
	std::string file(tmp);
	file.append(".txt");
#else
	std::string file("testfile.txt");
#endif

	Poco::TemporaryFile::registerForDeletion(file);

	Poco::FileOutputStream fos(file, std::ios::binary);
	fos << "sometestdata";
	fos.close();

	Poco::FileInputStream fis(file);
	assert (fis.good());
	std::string read;
	fis >> read;
	assert (!read.empty());
}


void FileStreamTest::testWrite()
{
#if defined(POCO_OS_FAMILY_WINDOWS) && !defined(POCO_WIN32_UTF8)
	char tmp[]={'\xdf', '\xc4', '\xd6', '\xdc', '\xe4', '\xf6', '\xfc', '\0'};
	std::string file(tmp);
	file = "dummy_" + file + (".txt");
#elif defined(POCO_OS_FAMILY_WINDOWS)
	char tmp[]={'\xc3', '\x9f', '\xc3', '\x84', '\xc3', '\x96', '\xc3', '\x9c', '\xc3', '\xa4', '\xc3', '\xb6', '\xc3', '\xbc', '\0'};
	std::string file(tmp);
	file = "dummy_" + file + ".txt";
#else
	std::string file("dummy_file.txt");
#endif

	Poco::TemporaryFile::registerForDeletion(file);

	Poco::FileOutputStream fos(file);
	assert (fos.good());
	fos << "hiho";
	fos.close();

	Poco::FileInputStream fis(file);
	assert (fis.good());
	std::string read;
	fis >> read;
	assert (read == "hiho");
}


void FileStreamTest::testReadWrite()
{
#if defined(POCO_OS_FAMILY_WINDOWS) && !defined(POCO_WIN32_UTF8)
	char tmp[]={'\xdf', '\xc4', '\xd6', '\xdc', '\xe4', '\xf6', '\xfc', '\0'};
	std::string file(tmp);
	file = "dummy_" + file + (".txt");
#else
	char tmp[]={'\xc3', '\x9f', '\xc3', '\x84', '\xc3', '\x96', '\xc3', '\x9c', '\xc3', '\xa4', '\xc3', '\xb6', '\xc3', '\xbc', '\0'};
	std::string file(tmp);
	file = "dummy_" + file + ".txt";
#endif

	Poco::TemporaryFile::registerForDeletion(file);

	Poco::FileStream fos(file);
	assert (fos.good());
	fos << "hiho";
	fos.seekg(0, std::ios::beg);
	std::string read;
	fos >> read;
	assert (read == "hiho");
}


void FileStreamTest::testOpen()
{
	Poco::FileOutputStream ostr;
	ostr.open("test.txt", std::ios::out);
	assert (ostr.good());
	ostr.close();
}


void FileStreamTest::testOpenModeIn()
{
	Poco::File f("nonexistent.txt");
	if (f.exists())
		f.remove();
	
	try
	{	
		Poco::FileInputStream istr("nonexistent.txt");
		fail("nonexistent file - must throw");
	}
	catch (Poco::Exception&)
	{
	}

	f.createFile();
	Poco::FileInputStream istr("nonexistent.txt");
	assert (istr.good());
}


void FileStreamTest::testOpenModeOut()
{
	Poco::File f("test.txt");
	if (f.exists())
		f.remove();
	
	Poco::FileOutputStream ostr1("test.txt");
	ostr1 << "Hello, world!";
	ostr1.close();

	assert (f.exists());
	assert (f.getSize() != 0);
	
	Poco::FileStream str1("test.txt");
	str1.close();

	assert (f.exists());
	assert (f.getSize() != 0);
		
	Poco::FileOutputStream ostr2("test.txt");
	ostr2.close();

	assert (f.exists());
	assert (f.getSize() == 0);

	f.remove();
}


void FileStreamTest::testOpenModeTrunc()
{
	Poco::File f("test.txt");
	if (f.exists())
		f.remove();
	
	Poco::FileOutputStream ostr1("test.txt");
	ostr1 << "Hello, world!";
	ostr1.close();

	assert (f.exists());
	assert (f.getSize() != 0);
	
	Poco::FileStream str1("test.txt", std::ios::trunc);
	str1.close();

	assert (f.exists());
	assert (f.getSize() == 0);

	f.remove();
}


void FileStreamTest::testOpenModeAte()
{
	Poco::FileOutputStream ostr("test.txt");
	ostr << "0123456789";
	ostr.close();
	
	Poco::FileStream str1("test.txt", std::ios::ate);
	int c = str1.get();
	assert (str1.eof());
	
	str1.clear();
	str1.seekg(0);
	c = str1.get();
	assert (c == '0');
	
	str1.close();

	Poco::FileStream str2("test.txt", std::ios::ate);
	str2 << "abcdef";
	str2.seekg(0);
	std::string s;
	str2 >> s;
	assert (s == "0123456789abcdef");
	str2.close();
}


void FileStreamTest::testOpenModeApp()
{
	Poco::FileOutputStream ostr("test.txt");
	ostr << "0123456789";
	ostr.close();
	
	Poco::FileStream str1("test.txt", std::ios::app);

	str1 << "abc";
	
	str1.seekp(0);
	
	str1 << "def";
	
	str1.close();

	Poco::FileInputStream istr("test.txt");
	std::string s;
	istr >> s;
	assert (s == "0123456789abcdef");
	istr.close();
}


void FileStreamTest::testSeek()
{
	Poco::FileStream str("test.txt", std::ios::trunc);
	str << "0123456789abcdef";
	
	str.seekg(0);
	int c = str.get();
	assert (c == '0');
	
	str.seekg(10);
	assert (str.tellg() == std::streampos(10));
	c = str.get();
	assert (c == 'a');
	assert (str.tellg() == std::streampos(11));
	
	str.seekg(-1, std::ios::end);
	assert (str.tellg() == std::streampos(15));
	c = str.get();
	assert (c == 'f');
	assert (str.tellg() == std::streampos(16));
	
	str.seekg(-1, std::ios::cur);
	assert (str.tellg() == std::streampos(15));
	c = str.get();
	assert (c == 'f');
	assert (str.tellg() == std::streampos(16));
	
	str.seekg(-4, std::ios::cur);
	assert (str.tellg() == std::streampos(12));
	c = str.get();
	assert (c == 'c');
	assert (str.tellg() == std::streampos(13));
	
	str.seekg(1, std::ios::cur);
	assert (str.tellg() == std::streampos(14));
	c = str.get();
	assert (c == 'e');
	assert (str.tellg() == std::streampos(15));
}


void FileStreamTest::testMultiOpen()
{
	Poco::FileStream str("test.txt", std::ios::trunc);
	str << "0123456789\n";
	str << "abcdefghij\n";
	str << "klmnopqrst\n";
	str.close();
	
	std::string s;
	str.open("test.txt", std::ios::in);
	std::getline(str, s);
	assert (s == "0123456789");
	str.close();

	str.open("test.txt", std::ios::in);
	std::getline(str, s);
	assert (s == "0123456789");
	str.close();	
}


void FileStreamTest::setUp()
{
}


void FileStreamTest::tearDown()
{
}


CppUnit::Test* FileStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FileStreamTest");

	CppUnit_addTest(pSuite, FileStreamTest, testRead);
	CppUnit_addTest(pSuite, FileStreamTest, testWrite);
	CppUnit_addTest(pSuite, FileStreamTest, testReadWrite);
	CppUnit_addTest(pSuite, FileStreamTest, testOpen);
	CppUnit_addTest(pSuite, FileStreamTest, testOpenModeIn);
	CppUnit_addTest(pSuite, FileStreamTest, testOpenModeOut);
	CppUnit_addTest(pSuite, FileStreamTest, testOpenModeTrunc);
	CppUnit_addTest(pSuite, FileStreamTest, testOpenModeAte);
	CppUnit_addTest(pSuite, FileStreamTest, testOpenModeApp);
	CppUnit_addTest(pSuite, FileStreamTest, testSeek);
	CppUnit_addTest(pSuite, FileStreamTest, testMultiOpen);

	return pSuite;
}
