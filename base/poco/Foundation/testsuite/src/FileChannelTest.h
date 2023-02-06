//
// FileChannelTest.h
//
// Definition of the FileChannelTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FileChannelTest_INCLUDED
#define FileChannelTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class FileChannelTest: public CppUnit::TestCase
{
public:
	enum TimeRotation 
	{
		DAY_HOUR_MIN = 0,
		HOUR_MIN,
		MIN
	};

	FileChannelTest(const std::string& name);
	~FileChannelTest();

	void testRotateBySize();
	void testRotateByAge();
	void testRotateAtTimeDayUTC();
	void testRotateAtTimeDayLocal();
	void testRotateAtTimeHourUTC();
	void testRotateAtTimeHourLocal();
	void testRotateAtTimeMinUTC();
	void testRotateAtTimeMinLocal();
	void testArchive();
	void testCompress();
	void testPurgeAge();
	void testPurgeCount();
	void testWrongPurgeOption();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
		template <class D> std::string rotation(TimeRotation rtype) const;
		void remove(const std::string& baseName);
		std::string filename() const;

		void purgeAge(const std::string& purgeAge);
		void noPurgeAge(const std::string& purgeAge);
		void purgeCount(const std::string& pc);
		void noPurgeCount(const std::string& pc);
};


#endif // FileChannelTest_INCLUDED
