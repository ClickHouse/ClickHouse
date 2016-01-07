//
// FileChannelTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/FileChannelTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FileChannelTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/FileChannel.h"
#include "Poco/Message.h"
#include "Poco/AutoPtr.h"
#include "Poco/TemporaryFile.h"
#include "Poco/Thread.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/Timestamp.h"
#include "Poco/DateTime.h"
#include "Poco/LocalDateTime.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/NumberFormatter.h"
#include "Poco/DirectoryIterator.h"
#include "Poco/Exception.h"
#include <vector>


using Poco::FileChannel;
using Poco::Message;
using Poco::AutoPtr;
using Poco::TemporaryFile;
using Poco::Thread;
using Poco::File;
using Poco::Path;
using Poco::Timestamp;
using Poco::NumberFormatter;
using Poco::DateTime;
using Poco::LocalDateTime;
using Poco::DateTimeFormatter;
using Poco::DateTimeFormat;
using Poco::DirectoryIterator;
using Poco::InvalidArgumentException;


FileChannelTest::FileChannelTest(const std::string& name): CppUnit::TestCase(name)
{
}


FileChannelTest::~FileChannelTest()
{
}


void FileChannelTest::testRotateBySize()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_ROTATION, "2 K");
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 200; ++i)
		{
			pChannel->log(msg);
		}
		File f(name + ".0");
		assert (f.exists());
		f = name + ".1";
		assert (f.exists());
		f = name + ".2";
		assert (!f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testRotateByAge()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_ROTATION, "2 seconds");
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 15; ++i)
		{
			pChannel->log(msg);
			Thread::sleep(300);
		}
		File f(name + ".0");
		assert (f.exists());
		f = name + ".1";
		assert (f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testRotateAtTimeDayUTC()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_TIMES, "utc");
		pChannel->setProperty(FileChannel::PROP_ROTATION, rotation<DateTime>(DAY_HOUR_MIN));
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		int min = DateTime().minute();
		while (DateTime().minute() == min)
		{
			pChannel->log(msg);
			Thread::sleep(1000);
		}
		pChannel->log(msg);
		File f(name + ".0");
		assert (f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testRotateAtTimeDayLocal()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_TIMES, "local");
		pChannel->setProperty(FileChannel::PROP_ROTATION, rotation<LocalDateTime>(DAY_HOUR_MIN));
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		int min = DateTime().minute();
		while (DateTime().minute() == min)
		{
			pChannel->log(msg);
			Thread::sleep(1000);
		}
		pChannel->log(msg);
		File f(name + ".0");
		assert (f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testRotateAtTimeHourUTC()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_TIMES, "utc");
		pChannel->setProperty(FileChannel::PROP_ROTATION, rotation<DateTime>(HOUR_MIN));
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		int min = DateTime().minute();
		while (DateTime().minute() == min)
		{
			pChannel->log(msg);
			Thread::sleep(1000);
		}
		pChannel->log(msg);
		File f(name + ".0");
		assert (f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testRotateAtTimeHourLocal()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_TIMES, "local");
		pChannel->setProperty(FileChannel::PROP_ROTATION, rotation<LocalDateTime>(HOUR_MIN));
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		int min = DateTime().minute();
		while (DateTime().minute() == min)
		{
			pChannel->log(msg);
			Thread::sleep(1000);
		}
		pChannel->log(msg);
		File f(name + ".0");
		assert (f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testRotateAtTimeMinUTC()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_TIMES, "utc");
		pChannel->setProperty(FileChannel::PROP_ROTATION, rotation<DateTime>(MIN));
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		int min = DateTime().minute();
		while (DateTime().minute() == min)
		{
			pChannel->log(msg);
			Thread::sleep(1000);
		}
		pChannel->log(msg);
		File f(name + ".0");
		assert (f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testRotateAtTimeMinLocal()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_TIMES, "local");
		pChannel->setProperty(FileChannel::PROP_ROTATION, rotation<LocalDateTime>(MIN));
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		int min = DateTime().minute();
		while (DateTime().minute() == min)
		{
			pChannel->log(msg);
			Thread::sleep(1000);
		}
		pChannel->log(msg);
		File f(name + ".0");
		assert (f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testArchive()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_ROTATION, "2 K");
		pChannel->setProperty(FileChannel::PROP_ARCHIVE, "number");
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 200; ++i)
		{
			pChannel->log(msg);
		}
		File f(name + ".0");
		assert (f.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testCompress()
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_ROTATION, "1 K");
		pChannel->setProperty(FileChannel::PROP_ARCHIVE, "number");
		pChannel->setProperty(FileChannel::PROP_COMPRESS, "true");
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 200; ++i)
		{
			pChannel->log(msg);
		}
		Thread::sleep(3000); // allow time for background compression
		File f0(name + ".0.gz");
		assert (f0.exists());
		File f1(name + ".1.gz");
		assert (f1.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::purgeAge(const std::string& pa)
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_ROTATION, "1 K");
		pChannel->setProperty(FileChannel::PROP_ARCHIVE, "number");
		pChannel->setProperty(FileChannel::PROP_PURGEAGE, pa);
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 200; ++i)
		{
			pChannel->log(msg);
		}
		File f0(name + ".0");
		assert(f0.exists());
		File f1(name + ".1");
		assert(f1.exists());
		File f2(name + ".2");
		assert(f2.exists());
		
		Thread::sleep(5000);
		for (int i = 0; i < 50; ++i)
		{
			pChannel->log(msg);
		}
		
		assert(!f2.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::noPurgeAge(const std::string& npa)
{
	std::string name = filename();

	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_ROTATION, "1 K");
		pChannel->setProperty(FileChannel::PROP_ARCHIVE, "number");
		pChannel->setProperty(FileChannel::PROP_PURGEAGE, npa);
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 200; ++i)
		{
			pChannel->log(msg);
		}
		File f0(name + ".0");
		assert(f0.exists());
		File f1(name + ".1");
		assert(f1.exists());
		File f2(name + ".2");
		assert(f2.exists());

		Thread::sleep(5000);
		for (int i = 0; i < 50; ++i)
		{
			pChannel->log(msg);
		}

		assert(f2.exists());
	}
	catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testPurgeAge()
{
		purgeAge("5 seconds");
		try
		{
				noPurgeAge("0 seconds");
				fail ("must fail");
		} catch (InvalidArgumentException&)
		{
		}

		noPurgeAge("");
		noPurgeAge("none");
}


void FileChannelTest::purgeCount(const std::string& pc)
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_ROTATION, "1 K");
		pChannel->setProperty(FileChannel::PROP_ARCHIVE, "number");
		pChannel->setProperty(FileChannel::PROP_PURGECOUNT, pc);
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 200; ++i)
		{
			pChannel->log(msg);
			Thread::sleep(50);
		}
		File f0(name + ".0");
		assert(f0.exists());
		File f1(name + ".1");
		assert(f1.exists());
		File f2(name + ".2");
		assert(!f2.exists());
	} catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::noPurgeCount(const std::string& npc)
{
	std::string name = filename();
	try
	{
		AutoPtr<FileChannel> pChannel = new FileChannel(name);
		pChannel->setProperty(FileChannel::PROP_ROTATION, "1 K");
		pChannel->setProperty(FileChannel::PROP_ARCHIVE, "number");
		pChannel->setProperty(FileChannel::PROP_PURGECOUNT, npc);
		pChannel->open();
		Message msg("source", "This is a log file entry", Message::PRIO_INFORMATION);
		for (int i = 0; i < 200; ++i)
		{
			pChannel->log(msg);
			Thread::sleep(50);
		}
		File f0(name + ".0");
		assert(f0.exists());
		File f1(name + ".1");
		assert(f1.exists());
		File f2(name + ".2");
		assert(f2.exists());
	} catch (...)
	{
		remove(name);
		throw;
	}
	remove(name);
}


void FileChannelTest::testPurgeCount()
{
	purgeCount("2");
	try
	{
		noPurgeCount("0");
		fail("must fail");
	} catch (InvalidArgumentException&)
	{
	}

	noPurgeCount("");
	noPurgeCount("none");
}


void FileChannelTest::setUp()
{
}


void FileChannelTest::tearDown()
{
}


void FileChannelTest::remove(const std::string& baseName)
{
	DirectoryIterator it(Path::current());
	DirectoryIterator end;
	std::vector<std::string> files;
	while (it != end)
	{
		if (it.name().find(baseName) == 0)
		{
			files.push_back(it.name());
		}
		++it;
	}
	for (std::vector<std::string>::iterator it = files.begin(); it != files.end(); ++it)
	{
		try
		{
			File f(*it);
			f.remove();
		} catch (...)
		{
		}
	}
}


std::string FileChannelTest::filename() const
{
	std::string name = "log_";
	name.append(DateTimeFormatter::format(Timestamp(), "%Y%m%d%H%M%S"));
	name.append(".log");
	return name;
}


template <class DT>
std::string FileChannelTest::rotation(TimeRotation rtype) const
{
	DT now;
	std::string rotation;

	int day  = now.dayOfWeek();
	int min  = now.minute();
	int hour = now.hour();
	if (++min == 60)
	{
		++hour;
		min = 0;
	}
	if (hour == 24)
	{
		hour = 0;
		++day;
		day %= 7;
	}

	switch (rtype)
	{
	case DAY_HOUR_MIN: // day,hh:m,
		rotation = DateTimeFormat::WEEKDAY_NAMES[day];
		rotation += ',' + NumberFormatter::format0(hour, 2) + ':' + NumberFormatter::format0(min, 2);
		break;
	case HOUR_MIN: // hh:mm
		rotation = NumberFormatter::format0(hour, 2) + ':' + NumberFormatter::format0(min, 2);
		break;
	case MIN: // mm
		rotation = ':' + NumberFormatter::format0(min, 2);
		break;
	default:
		rotation = "";
		break;
	}
	return rotation;
}


CppUnit::Test* FileChannelTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FileChannelTest");

	CppUnit_addTest(pSuite, FileChannelTest, testRotateBySize);
	CppUnit_addTest(pSuite, FileChannelTest, testRotateByAge);
	CppUnit_addTest(pSuite, FileChannelTest, testRotateAtTimeDayUTC);
	CppUnit_addTest(pSuite, FileChannelTest, testRotateAtTimeDayLocal);
	CppUnit_addTest(pSuite, FileChannelTest, testRotateAtTimeHourUTC);
	CppUnit_addTest(pSuite, FileChannelTest, testRotateAtTimeHourLocal);
	CppUnit_addTest(pSuite, FileChannelTest, testRotateAtTimeMinUTC);
	CppUnit_addTest(pSuite, FileChannelTest, testRotateAtTimeMinLocal);
	CppUnit_addTest(pSuite, FileChannelTest, testArchive);
	CppUnit_addTest(pSuite, FileChannelTest, testCompress);
	CppUnit_addTest(pSuite, FileChannelTest, testPurgeAge);
	CppUnit_addTest(pSuite, FileChannelTest, testPurgeCount);

	return pSuite;
}
