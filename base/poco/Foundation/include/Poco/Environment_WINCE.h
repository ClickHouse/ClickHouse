//
// Environment_WINCE.h
//
// Library: Foundation
// Package: Core
// Module:  Environment
//
// Definition of the EnvironmentImpl class for WINCE.
//
// Copyright (c) 2009-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Environment_WINCE_INCLUDED
#define Foundation_Environment_WINCE_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API EnvironmentImpl
{
public:
	typedef UInt8 NodeId[6]; /// Ethernet address.

	static std::string getImpl(const std::string& name);	
	static bool hasImpl(const std::string& name);	
	static void setImpl(const std::string& name, const std::string& value);
	static std::string osNameImpl();	
	static std::string osDisplayNameImpl();
	static std::string osVersionImpl();
	static std::string osArchitectureImpl();
	static std::string nodeNameImpl();
	static void nodeIdImpl(NodeId& id);
	static unsigned processorCountImpl();
	
private:
	static bool envVar(const std::string& name, std::string* value);
	
	static const std::string TEMP;
	static const std::string TMP;
	static const std::string HOMEPATH;
	static const std::string COMPUTERNAME;
	static const std::string OS;
	static const std::string NUMBER_OF_PROCESSORS;
	static const std::string PROCESSOR_ARCHITECTURE;	
};


} // namespace Poco


#endif // Foundation_Environment_WINCE_INCLUDED
