//
// TestPlugin.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TestPlugin.h#1 $
//
// Definition of the TestPlugin class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TestPlugin_INCLUDED
#define TestPlugin_INCLUDED


#include "Poco/Foundation.h"


class TestPlugin
{
public:
	TestPlugin();
	virtual ~TestPlugin();
	virtual std::string name() const = 0;
};


#endif // TestPlugin_INCLUDED
