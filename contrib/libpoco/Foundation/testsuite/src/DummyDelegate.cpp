//
// DummyDelegate.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/DummyDelegate.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DummyDelegate.h"
#include "Poco/Exception.h"

DummyDelegate::DummyDelegate() {}
DummyDelegate::~DummyDelegate() {}

void DummyDelegate::onSimple(const void* pSender, int& i)
{
	if (i != 0)
	{
		throw Poco::InvalidArgumentException();
	}
	i++;
}
void DummyDelegate::onSimple2(const void* pSender, int& i)
{
	if (i != 1)
	{
		throw Poco::InvalidArgumentException();
	}
	i++;
}

