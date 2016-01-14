//
// DummyDelegate.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/DummyDelegate.h#1 $
//
// Definition of DummyDelegate class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DummyDelegate_INCLUDED
#define DummyDelegate_INCLUDED


class DummyDelegate
{
public:
	DummyDelegate();
	virtual ~DummyDelegate();

	void onSimple(const void* pSender, int& i);
	void onSimple2(const void* pSender, int& i);
};


#endif // DummyDelegate_INCLUDED
