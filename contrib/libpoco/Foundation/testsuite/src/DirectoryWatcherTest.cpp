//
// DirectoryWatcherTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/DirectoryWatcherTest.cpp#1 $
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DirectoryWatcherTest.h"


#ifndef POCO_NO_INOTIFY


#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DirectoryWatcher.h"
#include "Poco/Delegate.h"
#include "Poco/FileStream.h"


using Poco::DirectoryWatcher;


DirectoryWatcherTest::DirectoryWatcherTest(const std::string& name): 
	CppUnit::TestCase(name),
	_error(false)
{
}


DirectoryWatcherTest::~DirectoryWatcherTest()
{
}


void DirectoryWatcherTest::testAdded()
{
	DirectoryWatcher dw(path().toString(), DirectoryWatcher::DW_FILTER_ENABLE_ALL, 2);
	
	dw.itemAdded += Poco::delegate(this, &DirectoryWatcherTest::onItemAdded);
	dw.itemRemoved += Poco::delegate(this, &DirectoryWatcherTest::onItemRemoved);
	dw.itemModified += Poco::delegate(this, &DirectoryWatcherTest::onItemModified);
	dw.itemMovedFrom += Poco::delegate(this, &DirectoryWatcherTest::onItemMovedFrom);
	dw.itemMovedTo += Poco::delegate(this, &DirectoryWatcherTest::onItemMovedTo);
	
	Poco::Thread::sleep(1000);
	
	Poco::Path p(path());
	p.setFileName("test.txt");
	Poco::FileOutputStream fos(p.toString());
	fos << "Hello, world!";
	fos.close();
	
	Poco::Thread::sleep(2000*dw.scanInterval());
	
	assert (_events.size() >= 1);
	assert (_events[0].callback == "onItemAdded");
	assert (Poco::Path(_events[0].path).getFileName() == "test.txt");
	assert (_events[0].type == DirectoryWatcher::DW_ITEM_ADDED);
	assert (!_error);
}


void DirectoryWatcherTest::testRemoved()
{
	Poco::Path p(path());
	p.setFileName("test.txt");
	Poco::FileOutputStream fos(p.toString());
	fos << "Hello, world!";
	fos.close();

	DirectoryWatcher dw(path().toString(), DirectoryWatcher::DW_FILTER_ENABLE_ALL, 2);
	
	dw.itemAdded += Poco::delegate(this, &DirectoryWatcherTest::onItemAdded);
	dw.itemRemoved += Poco::delegate(this, &DirectoryWatcherTest::onItemRemoved);
	dw.itemModified += Poco::delegate(this, &DirectoryWatcherTest::onItemModified);
	dw.itemMovedFrom += Poco::delegate(this, &DirectoryWatcherTest::onItemMovedFrom);
	dw.itemMovedTo += Poco::delegate(this, &DirectoryWatcherTest::onItemMovedTo);
	
	Poco::Thread::sleep(1000);
	
	Poco::File f(p.toString());
	f.remove();
	
	Poco::Thread::sleep(2000*dw.scanInterval());
	
	assert (_events.size() >= 1);
	assert (_events[0].callback == "onItemRemoved");
	assert (Poco::Path(_events[0].path).getFileName() == "test.txt");
	assert (_events[0].type == DirectoryWatcher::DW_ITEM_REMOVED);
	assert (!_error);
}


void DirectoryWatcherTest::testModified()
{
	Poco::Path p(path());
	p.setFileName("test.txt");
	Poco::FileOutputStream fos(p.toString());
	fos << "Hello, world!";
	fos.close();

	DirectoryWatcher dw(path().toString(), DirectoryWatcher::DW_FILTER_ENABLE_ALL, 2);
	
	dw.itemAdded += Poco::delegate(this, &DirectoryWatcherTest::onItemAdded);
	dw.itemRemoved += Poco::delegate(this, &DirectoryWatcherTest::onItemRemoved);
	dw.itemModified += Poco::delegate(this, &DirectoryWatcherTest::onItemModified);
	dw.itemMovedFrom += Poco::delegate(this, &DirectoryWatcherTest::onItemMovedFrom);
	dw.itemMovedTo += Poco::delegate(this, &DirectoryWatcherTest::onItemMovedTo);
	
	Poco::Thread::sleep(1000);
	
	Poco::FileOutputStream fos2(p.toString(), std::ios::app);
	fos2 << "Again!";
	fos2.close();
	
	Poco::Thread::sleep(2000*dw.scanInterval());
	
	assert (_events.size() >= 1);
	assert (_events[0].callback == "onItemModified");
	assert (Poco::Path(_events[0].path).getFileName() == "test.txt");
	assert (_events[0].type == DirectoryWatcher::DW_ITEM_MODIFIED);
	assert (!_error);
}


void DirectoryWatcherTest::testMoved()
{
	Poco::Path p(path());
	p.setFileName("test.txt");
	Poco::FileOutputStream fos(p.toString());
	fos << "Hello, world!";
	fos.close();

	DirectoryWatcher dw(path().toString(), DirectoryWatcher::DW_FILTER_ENABLE_ALL, 2);
	
	dw.itemAdded += Poco::delegate(this, &DirectoryWatcherTest::onItemAdded);
	dw.itemRemoved += Poco::delegate(this, &DirectoryWatcherTest::onItemRemoved);
	dw.itemModified += Poco::delegate(this, &DirectoryWatcherTest::onItemModified);
	dw.itemMovedFrom += Poco::delegate(this, &DirectoryWatcherTest::onItemMovedFrom);
	dw.itemMovedTo += Poco::delegate(this, &DirectoryWatcherTest::onItemMovedTo);
	
	Poco::Thread::sleep(1000);
	
	Poco::Path p2(path());
	p2.setFileName("test2.txt");
	Poco::File f(p.toString());
	f.renameTo(p2.toString());
	
	Poco::Thread::sleep(2000*dw.scanInterval());
	
	if (dw.supportsMoveEvents())
	{
		assert (_events.size() >= 2);
		assert (
			(_events[0].callback == "onItemMovedFrom" && _events[1].callback == "onItemMovedTo") ||
			(_events[1].callback == "onItemMovedFrom" && _events[0].callback == "onItemMovedTo")
		);
		assert (
			(Poco::Path(_events[0].path).getFileName() == "test.txt" && Poco::Path(_events[1].path).getFileName() == "test2.txt") ||
			(Poco::Path(_events[1].path).getFileName() == "test.txt" && Poco::Path(_events[0].path).getFileName() == "test2.txt")
		);
		assert (
			(_events[0].type == DirectoryWatcher::DW_ITEM_MOVED_FROM && _events[1].type == DirectoryWatcher::DW_ITEM_MOVED_TO) ||
			(_events[1].type == DirectoryWatcher::DW_ITEM_MOVED_FROM && _events[0].type == DirectoryWatcher::DW_ITEM_MOVED_TO)
		);
	}
	else
	{
		assert (_events.size() >= 2);
		assert (
			(_events[0].callback == "onItemAdded" && _events[1].callback == "onItemRemoved") ||
			(_events[1].callback == "onItemAdded" && _events[0].callback == "onItemRemoved")
		);
		assert (
			(Poco::Path(_events[0].path).getFileName() == "test.txt" && Poco::Path(_events[1].path).getFileName() == "test2.txt") ||
			(Poco::Path(_events[1].path).getFileName() == "test.txt" && Poco::Path(_events[0].path).getFileName() == "test2.txt")
		);
		assert (
			(_events[0].type == DirectoryWatcher::DW_ITEM_ADDED && _events[1].type == DirectoryWatcher::DW_ITEM_REMOVED) ||
			(_events[1].type == DirectoryWatcher::DW_ITEM_ADDED && _events[0].type == DirectoryWatcher::DW_ITEM_REMOVED)
		);
	}
	assert (!_error);
}


void DirectoryWatcherTest::setUp()
{
	_error = false;
	_events.clear();
	
	try
	{
		Poco::File d(path().toString());
		d.remove(true);
	}
	catch (...)
	{
	}

	Poco::File d(path().toString());
	d.createDirectories();
}


void DirectoryWatcherTest::tearDown()
{
	try
	{
		Poco::File d(path().toString());
		d.remove(true);
	}
	catch (...)
	{
	}
}


void DirectoryWatcherTest::onItemAdded(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
	DirEvent de;
	de.callback = "onItemAdded";
	de.path = ev.item.path();
	de.type = ev.event;
	_events.push_back(de);
}


void DirectoryWatcherTest::onItemRemoved(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
	DirEvent de;
	de.callback = "onItemRemoved";
	de.path = ev.item.path();
	de.type = ev.event;
	_events.push_back(de);
}


void DirectoryWatcherTest::onItemModified(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
	DirEvent de;
	de.callback = "onItemModified";
	de.path = ev.item.path();
	de.type = ev.event;
	_events.push_back(de);
}


void DirectoryWatcherTest::onItemMovedFrom(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
	DirEvent de;
	de.callback = "onItemMovedFrom";
	de.path = ev.item.path();
	de.type = ev.event;
	_events.push_back(de);
}


void DirectoryWatcherTest::onItemMovedTo(const Poco::DirectoryWatcher::DirectoryEvent& ev)
{
	DirEvent de;
	de.callback = "onItemMovedTo";
	de.path = ev.item.path();
	de.type = ev.event;
	_events.push_back(de);
}


void DirectoryWatcherTest::onError(const Poco::Exception& exc)
{
	_error = true;
}


Poco::Path DirectoryWatcherTest::path() const
{
	Poco::Path p(Poco::Path::current());
	p.pushDirectory("DirectoryWatcherTest");
	return p;
}


CppUnit::Test* DirectoryWatcherTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DirectoryWatcherTest");

	CppUnit_addTest(pSuite, DirectoryWatcherTest, testAdded);
	CppUnit_addTest(pSuite, DirectoryWatcherTest, testRemoved);
	CppUnit_addTest(pSuite, DirectoryWatcherTest, testModified);
	CppUnit_addTest(pSuite, DirectoryWatcherTest, testMoved);

	return pSuite;
}


#endif // POCO_NO_INOTIFY
