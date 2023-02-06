//
// CompressTest.cpp
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: BSL-1.0
//


#include "CompressTest.h"
#include "ZipTest.h"
#include "Poco/Buffer.h"
#include "Poco/Zip/Compress.h"
#include "Poco/Zip/ZipManipulator.h"
#include "Poco/File.h"
#include "Poco/FileStream.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include <iostream>
#undef min
#include <algorithm>


using namespace Poco::Zip;


CompressTest::CompressTest(const std::string& name): CppUnit::TestCase(name)
{
}


CompressTest::~CompressTest()
{
}


void CompressTest::testSingleFile()
{
	Poco::FileOutputStream out(Poco::Path::temp() + "appinf.zip");
	Poco::Path theFile(ZipTest::getTestFile("data", "test.zip"));
	Compress c(out, true);
	c.addFile(theFile, theFile.getFileName());
	ZipArchive a(c.close());
}


void CompressTest::testDirectory()
{
	Poco::FileOutputStream out(Poco::Path::temp() + "pocobin.zip");
	Poco::File aFile("some/");
	if (aFile.exists()) aFile.remove(true);
	Poco::File aDir("some/recursive/dir/");
	aDir.createDirectories();
	Poco::File aDir2("some/other/recursive/dir/");
	aDir2.createDirectories();
	Poco::File aF("some/recursive/dir/test.file");
	aF.createFile();
	Poco::FileOutputStream fos(aF.path());
	fos << "just some test data";
	fos.close();

	Poco::Path theFile(aFile.path());
	theFile.makeDirectory();
	Compress c(out, true);
	c.addRecursive(theFile, ZipCommon::CL_MAXIMUM, false, theFile);
	ZipArchive a(c.close());
	Poco::File(aFile).remove(true);
}


void CompressTest::testManipulator()
{
	{
		Poco::FileOutputStream out(Poco::Path::temp() + "appinf.zip");
		Poco::Path theFile(ZipTest::getTestFile("data", "test.zip"));
		Compress c(out, true);
		c.addFile(theFile, theFile.getFileName());
		ZipArchive a(c.close());
	}
	ZipManipulator zm(Poco::Path::temp() + "appinf.zip", true);
	zm.renameFile("test.zip", "renamedtest.zip");
	zm.addFile("doc/othertest.zip", ZipTest::getTestFile("data", "test.zip"));
	ZipArchive archive=zm.commit();
	assert (archive.findHeader("doc/othertest.zip") != archive.headerEnd());
}


void CompressTest::testManipulatorDel()
{
	{
		Poco::FileOutputStream out(Poco::Path::temp() + "appinf.zip");
		Poco::Path theFile(ZipTest::getTestFile("data", "test.zip"));
		Compress c(out, true);
		c.addFile(theFile, theFile.getFileName());
		ZipArchive a(c.close());
	}
	ZipManipulator zm(Poco::Path::temp() + "appinf.zip", true);
	zm.deleteFile("test.zip");
	zm.addFile("doc/data.zip", ZipTest::getTestFile("data", "data.zip"));
	ZipArchive archive=zm.commit();
	assert (archive.findHeader("test.zip") == archive.headerEnd());
	assert (archive.findHeader("doc/data.zip") != archive.headerEnd());
}


void CompressTest::testManipulatorReplace()
{
	{
		Poco::FileOutputStream out(Poco::Path::temp() + "appinf.zip");
		Poco::Path theFile(ZipTest::getTestFile("data", "test.zip"));
		Compress c(out, true);
		c.addFile(theFile, theFile.getFileName());
		ZipArchive a(c.close());
	}
	ZipManipulator zm(Poco::Path::temp() + "appinf.zip", true);
	zm.replaceFile("test.zip", ZipTest::getTestFile("data", "doc.zip"));

	ZipArchive archive=zm.commit();
	assert (archive.findHeader("test.zip") != archive.headerEnd());
	assert (archive.findHeader("doc.zip") == archive.headerEnd());
}


void CompressTest::testSetZipComment()
{
	std::string comment("Testing...123...");
	Poco::FileOutputStream out(Poco::Path::temp() + "comment.zip");
	Poco::Path theFile(ZipTest::getTestFile("data", "test.zip"));
	Compress c(out, true);
	c.addFile(theFile, theFile.getFileName());
	c.setZipComment(comment);
	ZipArchive a(c.close());
	assert(a.getZipComment() == comment);
}


void CompressTest::createDataFile(const std::string& path, Poco::UInt64 size)
{
	Poco::FileOutputStream out(path.c_str(), std::ios::trunc);
	assert( ! out.fail() );
	Poco::Buffer<char> buffer(MB);
	for(int i = 0; size != 0; i++) {
		std::memset(buffer.begin(), i, buffer.size());
		Poco::UInt64 bytesToWrite = std::min(size, static_cast<Poco::UInt64>(buffer.size()));
		out.write(buffer.begin(), bytesToWrite);
		assert( ! out.fail() );
		size -= bytesToWrite;
	}
	out.flush();
	assert( ! out.fail() );
	out.close();
	assert( ! out.fail() );
}


void CompressTest::testZip64()
{
	typedef std::map<std::string, Poco::UInt64> FileMap;
	std::cout << std::endl;
	FileMap files;
	files["data1.bin"] = static_cast<Poco::UInt64>(KB)*4096+1;
	files["data2.bin"] = static_cast<Poco::UInt64>(KB)*16;
	files["data3.bin"] = static_cast<Poco::UInt64>(KB)*4096-1;

	for(FileMap::const_iterator it = files.begin(); it != files.end(); it++)
	{
		std::cout << '\t' << "createDataFile(" << it->first << ", " << it->second << ");" << std::endl;
		createDataFile(it->first, it->second);
	}
	Poco::FileOutputStream out(Poco::Path::temp() + "zip64.zip", std::ios::trunc);
	Compress c(out, true, true);
	for(FileMap::const_iterator it = files.begin(); it != files.end(); it++)
	{
		const std::string& path = it->first;
		std::cout << '\t' << "addFile(" << path <<  ");" << std::endl;
		c.addFile(path, path, ZipCommon::CM_STORE);
	}
	ZipArchive a(c.close());
	for(FileMap::const_iterator it = files.begin(); it != files.end(); it++)
	{
		const std::string& path = it->first;
		Poco::UInt64 size = it->second;
		ZipArchive::FileHeaders::const_iterator it2 = a.findHeader(path);
		assert (it2 != a.headerEnd());
		const Poco::Zip::ZipLocalFileHeader& file = it2->second;
		assert(file.getUncompressedSize() == size);
		assert(file.getCompressedSize() == size);
	}
	for (FileMap::const_iterator it = files.begin(); it != files.end(); it++)
	{
		Poco::File(it->first).remove();
	}
}


void CompressTest::setUp()
{
}


void CompressTest::tearDown()
{
}


CppUnit::Test* CompressTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("CompressTest");

	CppUnit_addTest(pSuite, CompressTest, testSingleFile);
	CppUnit_addTest(pSuite, CompressTest, testDirectory);
	CppUnit_addTest(pSuite, CompressTest, testManipulator);
	CppUnit_addTest(pSuite, CompressTest, testManipulatorDel);
	CppUnit_addTest(pSuite, CompressTest, testManipulatorReplace);
	CppUnit_addTest(pSuite, CompressTest, testSetZipComment);
	CppUnit_addTest(pSuite, CompressTest, testZip64);

	return pSuite;
}
