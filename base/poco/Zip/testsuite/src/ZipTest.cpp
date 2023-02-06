//
// ZipTest.cpp
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: BSL-1.0
//


#include "ZipTest.h"
#include "Poco/Zip/SkipCallback.h"
#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Zip/ZipArchive.h"
#include "Poco/Zip/ZipStream.h"
#include "Poco/Zip/Decompress.h"
#include "Poco/Zip/ZipCommon.h"
#include "Poco/StreamCopier.h"
#include "Poco/File.h"
#include "Poco/FileStream.h"
#include "Poco/URI.h"
#include "Poco/Path.h"
#include "Poco/Delegate.h"
#include "Poco/StreamCopier.h"
#include "Poco/Environment.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#undef min
#include <algorithm>
#include <iostream>
#include <sstream>


using namespace Poco::Zip;


ZipTest::ZipTest(const std::string& name): CppUnit::TestCase(name)
{
}


ZipTest::~ZipTest()
{
}


void ZipTest::testSkipSingleFile()
{
	std::string testFile = getTestFile("data", "test.zip");
	Poco::FileInputStream inp(testFile);
	assert (inp.good());
	SkipCallback skip;
	ZipLocalFileHeader hdr(inp, false, skip);
	assert (ZipCommon::HS_FAT == hdr.getHostSystem());
	int major = hdr.getMajorVersionNumber();
	int POCO_UNUSED minor = hdr.getMinorVersionNumber();
	assert (major <= 2);
	std::size_t hdrSize = hdr.getHeaderSize();
	assert (hdrSize > 30);
	ZipCommon::CompressionMethod POCO_UNUSED cm = hdr.getCompressionMethod();
	assert (!hdr.isEncrypted());
	Poco::DateTime aDate = hdr.lastModifiedAt();
	Poco::UInt64 POCO_UNUSED cS = hdr.getCompressedSize();
	Poco::UInt64 POCO_UNUSED uS = hdr.getUncompressedSize();
	const std::string& POCO_UNUSED fileName = hdr.getFileName();
}


void ZipTest::testDecompressSingleFile()
{
	std::string testFile = getTestFile("data", "test.zip");
	Poco::FileInputStream inp(testFile);
	assert (inp.good());
	ZipArchive arch(inp);
	ZipArchive::FileHeaders::const_iterator it = arch.findHeader("testfile.txt");
	assert (it != arch.headerEnd());
	ZipInputStream zipin (inp, it->second);
	std::ostringstream out(std::ios::binary);
	Poco::StreamCopier::copyStream(zipin, out);
	assert(!out.str().empty());
}


void ZipTest::testDecompressSingleFileInDir()
{
	std::string testFile = getTestFile("data","test.zip");
	Poco::FileInputStream inp(testFile);
	assert (inp.good());
	ZipArchive arch(inp);
	ZipArchive::FileHeaders::const_iterator it = arch.findHeader("testdir/testfile.txt");
	assert (it != arch.headerEnd());
	ZipInputStream zipin (inp, it->second);
	std::ostringstream out(std::ios::binary);
	Poco::StreamCopier::copyStream(zipin, out);
	assert(!out.str().empty());
}


void ZipTest::testCrcAndSizeAfterData()
{
	std::string testFile = getTestFile("data", "data.zip");
	Poco::FileInputStream inp(testFile);
	assert (inp.good());
	Decompress dec(inp, Poco::Path::temp());
	dec.EError += Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	dec.decompressAllFiles();
	dec.EError -= Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	assert (_errCnt == 0);
	assert (!dec.mapping().empty());
}


void ZipTest::testCrcAndSizeAfterDataWithArchive()
{
	std::string testFile = getTestFile("data", "data.zip");
	Poco::FileInputStream inp(testFile);
	assert (inp.good());
	Poco::Zip::ZipArchive zip(inp);
	inp.clear();
	inp.seekg(0);
	Poco::Zip::ZipArchive::FileHeaders::const_iterator it = zip.headerBegin();
	for ( ; it!=zip.headerEnd(); ++it)
	{
		Poco::Zip::ZipInputStream zipis(inp,it->second);
		Poco::Path path(it->second.getFileName());
		if (path.isFile())
		{
			Poco::FileOutputStream os(Poco::Path::temp() + "test.dat");
			Poco::StreamCopier::copyStream(zipis,os);
		}
	}
}


std::string ZipTest::getTestFile(const std::string& directory, const std::string& file)
{
	std::ostringstream ostr;
	ostr << directory << '/' << file;
	std::string validDir(ostr.str());
	Poco::Path pathPattern(validDir);
	if (Poco::File(pathPattern).exists())
	{
		return validDir;
	}

	ostr.str("");
	ostr << "/Zip/testsuite/" << directory << '/' << file;
	validDir = Poco::Environment::get("POCO_BASE") + ostr.str();
	pathPattern = validDir;

	if (!Poco::File(pathPattern).exists())
	{
		std::cout << "Can't find " << validDir << std::endl;
		throw Poco::NotFoundException("cannot locate directory containing valid Zip test files");
	}
	return validDir;
}


void ZipTest::testDecompress()
{
	std::string testFile = getTestFile("data", "test.zip");
	Poco::FileInputStream inp(testFile);
	assert (inp.good());
	Decompress dec(inp, Poco::Path::temp());
	dec.EError += Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	dec.decompressAllFiles();
	dec.EError -= Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	assert (_errCnt == 0);
	assert (!dec.mapping().empty());
}


void ZipTest::testDecompressFlat()
{
	std::string testFile = getTestFile("data", "test.zip");
	Poco::FileInputStream inp(testFile);
	assert (inp.good());
	Decompress dec(inp, Poco::Path::temp(), true);
	dec.EError += Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	dec.decompressAllFiles();
	dec.EError -= Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	assert (_errCnt == 0);
	assert (!dec.mapping().empty());
}


void ZipTest::testDecompressVuln()
{
	std::string testFile = getTestFile("data", "vuln.zip");
	Poco::FileInputStream inp(testFile);
	assert(inp.good());
	Decompress dec(inp, Poco::Path::temp());
	dec.EError += Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	dec.decompressAllFiles();
	dec.EError -= Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	assert (_errCnt == 1);
	assert (dec.mapping().empty());
}


void ZipTest::testDecompressFlatVuln()
{
	std::string testFile = getTestFile("data", "vuln.zip");
	Poco::FileInputStream inp(testFile);
	assert(inp.good());
	Decompress dec(inp, Poco::Path::temp(), true);
	dec.EError += Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	dec.decompressAllFiles();
	dec.EError -= Poco::Delegate<ZipTest, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string> >(this, &ZipTest::onDecompressError);
	assert (_errCnt == 0);
	assert (!dec.mapping().empty());
}


void ZipTest::verifyDataFile(const std::string& path, Poco::UInt64 size)
{
	Poco::FileInputStream in(path);
	assert( ! in.fail() );
	Poco::Buffer<char> buffer1(MB);
	Poco::Buffer<char> buffer2(MB);
	for (int i = 0; size != 0; i++)
	{
		std::memset(buffer1.begin(), i, buffer1.size());
		std::memset(buffer2.begin(), 0, buffer2.size());
		Poco::UInt64 bytesToRead = std::min(size, static_cast<Poco::UInt64>(buffer2.size()));
		in.read(buffer2.begin(), bytesToRead);
		assert (!in.fail() );
		assert (std::memcmp(buffer1.begin(), buffer2.begin(), static_cast<std::size_t>(bytesToRead)) == 0);
		size -= bytesToRead;
	}
	char c;
	in.read(&c, 1);
	assert ( in.eof() );
}


void ZipTest::testDecompressZip64()
{
	std::map<std::string, Poco::UInt64> files;
	files[Poco::Path::temp() + "data1.bin"] = static_cast<Poco::UInt64>(KB)*4096+1;
	files[Poco::Path::temp() + "data2.bin"] = static_cast<Poco::UInt64>(KB)*16;
	files[Poco::Path::temp() + "data3.bin"] = static_cast<Poco::UInt64>(KB)*4096-1;

	for(std::map<std::string, Poco::UInt64>::const_iterator it = files.begin(); it != files.end(); it++)
	{
		Poco::File file(it->first);
		if(file.exists())
			file.remove();
	}
	Poco::FileInputStream in(Poco::Path::temp() + "zip64.zip");
	Decompress c(in, Poco::Path::temp());
	c.decompressAllFiles();
	for(std::map<std::string, Poco::UInt64>::const_iterator it = files.begin(); it != files.end(); it++)
	{
		verifyDataFile(it->first, it->second);
	}
}


void ZipTest::testValidPath()
{
	assert (ZipCommon::isValidPath("."));
	assert (ZipCommon::isValidPath("file.txt"));
	assert (ZipCommon::isValidPath(".file.txt"));
	assert (ZipCommon::isValidPath("..file.txt"));
	assert (ZipCommon::isValidPath("file.txt.."));
	assert (ZipCommon::isValidPath(".file..txt"));
	assert (ZipCommon::isValidPath("~file..txt"));
	assert (ZipCommon::isValidPath("~file/~"));
	assert (ZipCommon::isValidPath("dir/~"));
	assert (ZipCommon::isValidPath("some"));
	assert (ZipCommon::isValidPath("some/dir"));
	assert (ZipCommon::isValidPath("some/dir/or/another"));
	assert (ZipCommon::isValidPath("some/dir/./another"));
	assert (ZipCommon::isValidPath("some/dir/or/another/file.txt"));
	assert (ZipCommon::isValidPath("s~me\\d.r\\.or..\\an..her\\file.txt"));
	assert (ZipCommon::isValidPath("some\\dir\\or\\another"));
	assert (ZipCommon::isValidPath("some\\dir\\or\\another\\file.txt"));
	assert (ZipCommon::isValidPath("s~me\\d.r/.or..\\an..her\\file.txt"));

	assert (!ZipCommon::isValidPath("/../"));
	assert (!ZipCommon::isValidPath("/"));
	assert (!ZipCommon::isValidPath("\\..\\"));
	assert (!ZipCommon::isValidPath("/..\\"));
	assert (!ZipCommon::isValidPath("\\../"));
	assert (!ZipCommon::isValidPath(".."));
	assert (!ZipCommon::isValidPath("~/"));
	assert (!ZipCommon::isValidPath("~/~"));
	assert (!ZipCommon::isValidPath("/~"));
	assert (!ZipCommon::isValidPath("/file.txt"));
	assert (!ZipCommon::isValidPath("~/file.txt"));
	assert (!ZipCommon::isValidPath("some/dir/or/../another/file.txt"));
	assert (!ZipCommon::isValidPath("C:\\Windows\\system32"));
}


void ZipTest::onDecompressError(const void* pSender, std::pair<const Poco::Zip::ZipLocalFileHeader, const std::string>& info)
{
	++_errCnt;
}


void ZipTest::setUp()
{
	_errCnt = 0;
}


void ZipTest::tearDown()
{
}


CppUnit::Test* ZipTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ZipTest");

	CppUnit_addTest(pSuite, ZipTest, testSkipSingleFile);
	CppUnit_addTest(pSuite, ZipTest, testDecompressSingleFile);
	CppUnit_addTest(pSuite, ZipTest, testDecompressSingleFileInDir);
	CppUnit_addTest(pSuite, ZipTest, testDecompress);
	CppUnit_addTest(pSuite, ZipTest, testDecompressFlat);
	CppUnit_addTest(pSuite, ZipTest, testDecompressVuln);
	CppUnit_addTest(pSuite, ZipTest, testDecompressFlatVuln);
	CppUnit_addTest(pSuite, ZipTest, testCrcAndSizeAfterData);
	CppUnit_addTest(pSuite, ZipTest, testCrcAndSizeAfterDataWithArchive);
	CppUnit_addTest(pSuite, ZipTest, testDecompressZip64);
	CppUnit_addTest(pSuite, ZipTest, testValidPath);

	return pSuite;
}
