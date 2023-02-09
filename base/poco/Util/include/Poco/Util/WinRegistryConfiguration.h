//
// WinRegistryConfiguration.h
//
// Library: Util
// Package: Windows
// Module:  WinRegistryConfiguration
//
// Definition of the WinRegistryConfiguration class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_WinRegistryConfiguration_INCLUDED
#define Util_WinRegistryConfiguration_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/String.h"


namespace Poco {
namespace Util {


class Util_API WinRegistryConfiguration: public AbstractConfiguration
	/// An implementation of AbstractConfiguration that stores configuration data
	/// in the Windows registry.
	///
	/// Removing key is not supported. An attempt to remove a key results
	/// in a NotImplementedException being thrown.
{
public:
	WinRegistryConfiguration(const std::string& rootPath, REGSAM extraSam = 0);
		/// Creates the WinRegistryConfiguration. 
		/// The rootPath must start with one of the root key names
		/// like HKEY_CLASSES_ROOT, e.g. HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services.
		/// All further keys are relative to the root path and can be
		/// dot separated, e.g. the path MyService.ServiceName will be converted to
		/// HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\MyService\ServiceName.
        /// The extraSam parameter will be passed along to WinRegistryKey, to control
        /// registry virtualization for example.

protected:
	~WinRegistryConfiguration();
		/// Destroys the WinRegistryConfiguration.

	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);

	std::string convertToRegFormat(const std::string& key, std::string& keyName) const;
		/// Takes a key in the format of A.B.C and converts it to
		/// registry format A\B\C, the last entry is the keyName, the rest is returned as path

	friend class WinConfigurationTest;
private:
	std::string _rootPath;
    REGSAM _extraSam;
};


} } // namespace Poco::Util


#endif // Util_WinRegistryConfiguration_INCLUDED
