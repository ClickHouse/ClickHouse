//
// TimerTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/TimerTest.h#1 $
//
// Definition of the TimerTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TimerTest_INCLUDED
#define TimerTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"
#include "Poco/Util/TimerTask.h"
#include "Poco/Event.h"


class TimerTest: public CppUnit::TestCase
{
public:
	TimerTest(const std::string& name);
	~TimerTest();

	void testScheduleTimestamp();
	void testScheduleClock();
	void testScheduleInterval();
	void testScheduleAtFixedRate();
	void testScheduleIntervalTimestamp();
	void testScheduleIntervalClock();
	void testCancel();

	void setUp();
	void tearDown();

	void onTimer(Poco::Util::TimerTask& task);

	static CppUnit::Test* suite();

private:
	Poco::Event _event;
};


#endif // TimerTest_INCLUDED
