//
// Environment.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Environment.h#2 $
//
// Library: Foundation
// Package: Core
// Module:  Environment
//
// Definition of the Environment class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Environment_INCLUDED
#define Foundation_Environment_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API Environment
	/// This class provides access to environment variables
	/// and some general system information.
{
public:
	typedef UInt8 NodeId[6]; /// Ethernet address.
	
	static std::string get(const std::string& name);
		/// Returns the value of the environment variable
		/// with the given name. Throws a NotFoundException
		/// if the variable does not exist.
		
	static std::string get(const std::string& name, const std::string& defaultValue);
		/// Returns the value of the environment variable
		/// with the given name. If the environment variable
		/// is undefined, returns defaultValue instead.
		
	static bool has(const std::string& name);
		/// Returns true iff an environment variable
		/// with the given name is defined.
		
	static void set(const std::string& name, const std::string& value);
		/// Sets the environment variable with the given name
		/// to the given value.

	static std::string osName();
		/// Returns the operating system name.
		
	static std::string osDisplayName();
		/// Returns the operating system name in a
		/// "user-friendly" way.
		///
		/// Currently this is only implemented for
		/// Windows. There it will return names like
		/// "Windows XP" or "Windows 7/Server 2008 SP2".
		/// On other platforms, returns the same as
		/// osName().
		
	static std::string osVersion();
		/// Returns the operating system version.
		
	static std::string osArchitecture();
		/// Returns the operating system architecture.
		
	static std::string nodeName();
		/// Returns the node (or host) name.
		
	static void nodeId(NodeId& id);
		/// Returns the Ethernet address of the first Ethernet
		/// adapter found on the system.
		///
		/// Throws a SystemException if no Ethernet adapter is available.
		
	static std::string nodeId();
		/// Returns the Ethernet address (format "xx:xx:xx:xx:xx:xx")
		/// of the first Ethernet adapter found on the system.
		///
		/// Throws a SystemException if no Ethernet adapter is available.
		
	static unsigned processorCount();
		/// Returns the number of processors installed in the system.
		///
		/// If the number of processors cannot be determined, returns 1.
		
	static Poco::UInt32 libraryVersion();
		/// Returns the POCO C++ Libraries version as a hexadecimal
		/// number in format 0xAABBCCDD, where
		///    - AA is the major version number,
		///    - BB is the minor version number,
		///    - CC is the revision number, and
		///    - DD is the patch level number.
		///
		/// Some patch level ranges have special meanings:
		///    - Dx mark development releases,
		///    - Ax mark alpha releases, and
		///    - Bx mark beta releases.
};


} // namespace Poco


#endif // Foundation_Environment_INCLUDED
