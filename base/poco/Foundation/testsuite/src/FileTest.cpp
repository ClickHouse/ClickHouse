//
// FileTest.cpp
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FileTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/File.h"
#include "Poco/TemporaryFile.h"
#include "Poco/Path.h"
#include "Poco/Exception.h"
#include "Poco/Thread.h"
#include <fstream>
#include <set>


using Poco::File;
using Poco::TemporaryFile;
using Poco::Path;
using Poco::Exception;
using Poco::Timestamp;
using Poco::Thread;


FileTest::FileTest(const std::string& name): CppUnit::TestCase(name)
{
}


FileTest::~FileTest()
{
}


void FileTest::testFileAttributes1()
{
	File f("testfile.dat");
	assert (!f.exists());

	try
	{
		bool POCO_UNUSED flag = f.canRead();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		bool POCO_UNUSED flag = f.canWrite();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		bool POCO_UNUSED flag = f.isFile();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		bool POCO_UNUSED flag = f.isDirectory();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		Timestamp POCO_UNUSED ts = f.created();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		Timestamp POCO_UNUSED ts = f.getLastModified();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		Timestamp ts;
		f.setLastModified(ts);
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		File::FileSize POCO_UNUSED fs = f.getSize();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		f.setSize(0);
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		f.setWriteable();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		f.setReadOnly();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		f.copyTo("copy.dat");
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		f.moveTo("copy.dat");
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		f.renameTo("copy.dat");
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}

	try
	{
		f.remove();
		failmsg("file does not exist - must throw exception");
	}
	catch (Exception&)
	{
	}
}


void FileTest::testCreateFile()
{
	File f("testfile.dat");
	bool created = f.createFile();
	assert (created);
	assert (!f.isHidden());
	created = f.createFile();
	assert (!created);
}


void FileTest::testFileAttributes2()
{
	TemporaryFile f;
	bool created = f.createFile();
	Timestamp ts;
	assert (created);

	assert (f.exists());
	assert (f.canRead());
	assert (f.canWrite());
	assert (f.isFile());
	assert (!f.isDirectory());
	Timestamp tsc = f.created();
	Timestamp tsm = f.getLastModified();
	assert (tsc - ts >= -2000000 && tsc - ts <= 2000000);
	assert (tsm - ts >= -2000000 && tsm - ts <= 2000000);

	f.setWriteable(false);
	assert (!f.canWrite());
	assert (f.canRead());

	f.setReadOnly(false);
	assert (f.canWrite());
	assert (f.canRead());

	ts = Timestamp::fromEpochTime(1000000);
	f.setLastModified(ts);
	assert (f.getLastModified() == ts);
}


void FileTest::testFileAttributes3()
{
#if defined(POCO_OS_FAMILY_UNIX)
#if POCO_OS==POCO_OS_CYGWIN
	File f("/dev/tty");
#else
 	File f("/dev/console");
#endif
#elif defined(POCO_OS_FAMILY_WINDOWS) && !defined(_WIN32_WCE)
	File f("CON");
#endif

#if !defined(_WIN32_WCE)
	assert (f.isDevice());
	assert (!f.isFile());
	assert (!f.isDirectory());
#endif
}


void FileTest::testCompare()
{
	File f1("abc.txt");
	File f2("def.txt");
	File f3("abc.txt");

	assert (f1 == f3);
	assert (!(f1 == f2));
	assert (f1 != f2);
	assert (!(f1 != f3));
	assert (!(f1 == f2));
	assert (f1 < f2);
	assert (f1 <= f2);
	assert (!(f2 < f1));
	assert (!(f2 <= f1));
	assert (f2 > f1);
	assert (f2 >= f1);
	assert (!(f1 > f2));
	assert (!(f1 >= f2));

	assert (f1 <= f3);
	assert (f1 >= f3);
}


void FileTest::testRootDir()
{
#if defined(POCO_OS_FAMILY_WINDOWS)
#if defined(_WIN32_WCE)
	File f1("\\");
	File f2("/");
	assert (f1.exists());
	assert (f2.exists());
#else
	File f1("/");
	File f2("c:/");
	File f3("c:\\");
	File f4("\\");
	assert (f1.exists());
	assert (f2.exists());
	assert (f3.exists());
	assert (f4.exists());
#endif
#else
	File f1("/");
	assert (f1.exists());
#endif
}


void FileTest::testSwap()
{
	File f1("abc.txt");
	File f2("def.txt");
	f1.swap(f2);
	assert (f1.path() == "def.txt");
	assert (f2.path() == "abc.txt");
}


void FileTest::testSize()
{
	std::ofstream ostr("testfile.dat");
	ostr << "Hello, world!" << std::endl;
	ostr.close();
	File f("testfile.dat");
	assert (f.getSize() > 0);
	f.setSize(0);
	assert (f.getSize() == 0);
}


void FileTest::testDirectory()
{
	File d("testdir");
	try
	{
		d.remove(true);
	}
	catch (...)
	{
	}
	TemporaryFile::registerForDeletion("testdir");

	bool created = d.createDirectory();
	assert (created);
	assert (d.isDirectory());
	assert (!d.isFile());
	std::vector<std::string> files;
	d.list(files);
	assert (files.empty());

	File f = Path("testdir/file1", Path::PATH_UNIX);
	f.createFile();
	f = Path("testdir/file2", Path::PATH_UNIX);
	f.createFile();
	f = Path("testdir/file3", Path::PATH_UNIX);
	f.createFile();

	d.list(files);
	assert (files.size() == 3);

	std::set<std::string> fs;
	fs.insert(files.begin(), files.end());
	assert (fs.find("file1") != fs.end());
	assert (fs.find("file2") != fs.end());
	assert (fs.find("file3") != fs.end());

	File dd(Path("testdir/testdir2/testdir3", Path::PATH_UNIX));
	dd.createDirectories();
	assert (dd.exists());
	assert (dd.isDirectory());

	File ddd(Path("testdir/testdirB/testdirC/testdirD", Path::PATH_UNIX));
	ddd.createDirectories();
	assert (ddd.exists());
	assert (ddd.isDirectory());

	d.remove(true);
}


void FileTest::testCopy()
{
	std::ofstream ostr("testfile.dat");
	ostr << "Hello, world!" << std::endl;
	ostr.close();

	File f1("testfile.dat");
	TemporaryFile f2;
	f1.setReadOnly().copyTo(f2.path());
	assert (f2.exists());
	assert (!f2.canWrite());
	assert (f1.getSize() == f2.getSize());
	f1.setWriteable().remove();
}


void FileTest::testMove()
{
	std::ofstream ostr("testfile.dat");
	ostr << "Hello, world!" << std::endl;
	ostr.close();

	File f1("testfile.dat");
	File::FileSize sz = f1.getSize();
	TemporaryFile f2;
	f1.moveTo(f2.path());
	assert (f2.exists());
	assert (f2.getSize() == sz);
	assert (f1.exists());
	assert (f1 == f2);
}


void FileTest::testCopyDirectory()
{
	Path pd1("testdir");
	File fd1(pd1);
	try
	{
		fd1.remove(true);
	}
	catch (...)
	{
	}
	fd1.createDirectories();
	Path pd2(pd1, "subdir");
	File fd2(pd2);
	fd2.createDirectories();
	Path pf1(pd1, "testfile1.dat");
	std::ofstream ostr1(pf1.toString().c_str());
	ostr1 << "Hello, world!" << std::endl;
	ostr1.close();
	Path pf2(pd1, "testfile2.dat");
	std::ofstream ostr2(pf2.toString().c_str());
	ostr2 << "Hello, world!" << std::endl;
	ostr2.close();
	Path pf3(pd2, "testfile3.dat");
	std::ofstream ostr3(pf3.toString().c_str());
	ostr3 << "Hello, world!" << std::endl;
	ostr3.close();

	File fd3("testdir2");

	try
	{
		fd3.remove(true);
	}
	catch (...)
	{
	}

	fd1.copyTo("testdir2");

	Path pd1t("testdir2");
	File fd1t(pd1t);
	assert (fd1t.exists());
	assert (fd1t.isDirectory());

	Path pd2t(pd1t, "subdir");
	File fd2t(pd2t);
	assert (fd2t.exists());
	assert (fd2t.isDirectory());

	Path pf1t(pd1t, "testfile1.dat");
	File ff1t(pf1t);
	assert (ff1t.exists());
	assert (ff1t.isFile());

	Path pf2t(pd1t, "testfile2.dat");
	File ff2t(pf2t);
	assert (ff2t.exists());
	assert (ff2t.isFile());

	Path pf3t(pd2t, "testfile3.dat");
	File ff3t(pf3t);
	assert (ff3t.exists());
	assert (ff3t.isFile());

	fd1.remove(true);
	fd3.remove(true);
}


void FileTest::testRename()
{
	std::ofstream ostr("testfile.dat");
	ostr << "Hello, world!" << std::endl;
	ostr.close();

	File f1("testfile.dat");
	File f2("testfile2.dat");
	f1.renameTo(f2.path());

	assert (f2.exists());
	assert (f1.exists());
	assert (f1 == f2);

	f2.remove();
}


void FileTest::testLongPath()
{
#if defined(_WIN32) && defined(POCO_WIN32_UTF8) && !defined(_WIN32_WCE)
	Poco::Path p("longpathtest");
	p.makeAbsolute();
	std::string longpath(p.toString());
	while (longpath.size() < MAX_PATH*4)
	{
		longpath.append("\\");
		longpath.append(64, 'x');
	}

	Poco::File d(longpath);
	d.createDirectories();

	assert (d.exists());
	assert (d.isDirectory());

	Poco::File f(p.toString());
	f.remove(true);	
#endif
}


void FileTest::setUp()
{
	File f("testfile.dat");
	try
	{
		f.remove();
	}
	catch (...)
	{
	}
}


void FileTest::tearDown()
{
	File f("testfile.dat");
	try
	{
		f.remove();
	}
	catch (...)
	{
	}
}


CppUnit::Test* FileTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FileTest");

	CppUnit_addTest(pSuite, FileTest, testCreateFile);
	CppUnit_addTest(pSuite, FileTest, testFileAttributes1);
	CppUnit_addTest(pSuite, FileTest, testFileAttributes2);
	CppUnit_addTest(pSuite, FileTest, testFileAttributes3);
	CppUnit_addTest(pSuite, FileTest, testCompare);
	CppUnit_addTest(pSuite, FileTest, testSwap);
	CppUnit_addTest(pSuite, FileTest, testSize);
	CppUnit_addTest(pSuite, FileTest, testDirectory);
	CppUnit_addTest(pSuite, FileTest, testCopy);
	CppUnit_addTest(pSuite, FileTest, testMove);
	CppUnit_addTest(pSuite, FileTest, testCopyDirectory);
	CppUnit_addTest(pSuite, FileTest, testRename);
	CppUnit_addTest(pSuite, FileTest, testRootDir);
	CppUnit_addTest(pSuite, FileTest, testLongPath);

	return pSuite;
}
