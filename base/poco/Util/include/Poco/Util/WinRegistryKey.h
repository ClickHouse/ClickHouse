//
// WinRegistryKey.h
//
// Library: Util
// Package: Windows
// Module:  WinRegistryKey
//
// Definition of the WinRegistryKey class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_WinRegistryKey_INCLUDED
#define Util_WinRegistryKey_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/UnWindows.h"
#include <vector>


namespace Poco {
namespace Util {


class Util_API WinRegistryKey
	/// This class implements a convenient interface to the
	/// Windows Registry.
	///
	/// This class is only available on Windows platforms.
{
public:
	typedef std::vector<std::string> Keys;
	typedef std::vector<std::string> Values;

	enum Type
	{
		REGT_NONE = 0,
		REGT_STRING = 1,
		REGT_STRING_EXPAND = 2,
		REGT_BINARY = 3,
		REGT_DWORD = 4,
		REGT_DWORD_BIG_ENDIAN = 5,
		REGT_LINK = 6,
		REGT_MULTI_STRING = 7,
		REGT_RESOURCE_LIST = 8,
		REGT_FULL_RESOURCE_DESCRIPTOR = 9,
		REGT_RESOURCE_REQUIREMENTS_LIST = 10,
		REGT_QWORD = 11
	};

	WinRegistryKey(const std::string& key, bool readOnly = false, REGSAM extraSam = 0);
		/// Creates the WinRegistryKey.
		///
		/// The key must start with one of the root key names
		/// like HKEY_CLASSES_ROOT, e.g. HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services.
		///
		/// If readOnly is true, then only read access to the registry
		/// is available and any attempt to write to the registry will
		/// result in an exception.
		///
		/// extraSam is used to pass extra flags (in addition to KEY_READ and KEY_WRITE)
		/// to the samDesired argument of RegOpenKeyEx() or RegCreateKeyEx().

	WinRegistryKey(HKEY hRootKey, const std::string& subKey, bool readOnly = false, REGSAM extraSam = 0);
		/// Creates the WinRegistryKey.
		///
		/// If readOnly is true, then only read access to the registry
		/// is available and any attempt to write to the registry will
		/// result in an exception.
		///
		/// extraSam is used to pass extra flags (in addition to KEY_READ and KEY_WRITE)
		/// to the samDesired argument of RegOpenKeyEx() or RegCreateKeyEx().

	~WinRegistryKey();
		/// Destroys the WinRegistryKey.

	void setString(const std::string& name, const std::string& value);
		/// Sets the string value (REG_SZ) with the given name.
		/// An empty name denotes the default value.

	std::string getString(const std::string& name);
		/// Returns the string value (REG_SZ) with the given name.
		/// An empty name denotes the default value.
		///
		/// Throws a NotFoundException if the value does not exist.

	void setStringExpand(const std::string& name, const std::string& value);
		/// Sets the expandable string value (REG_EXPAND_SZ) with the given name.
		/// An empty name denotes the default value.

	std::string getStringExpand(const std::string& name);
		/// Returns the string value (REG_EXPAND_SZ) with the given name.
		/// An empty name denotes the default value.
		/// All references to environment variables (%VAR%) in the string
		/// are expanded.
		///
		/// Throws a NotFoundException if the value does not exist.

	void setBinary(const std::string& name, const std::vector<char>& value);
		/// Sets the string value (REG_BINARY) with the given name.
		/// An empty name denotes the default value.

	std::vector<char> getBinary(const std::string& name);
		/// Returns the string value (REG_BINARY) with the given name.
		/// An empty name denotes the default value.
		///
		/// Throws a NotFoundException if the value does not exist.

	void setInt(const std::string& name, int value);
		/// Sets the numeric (REG_DWORD) value with the given name.
		/// An empty name denotes the default value.

	int getInt(const std::string& name);
		/// Returns the numeric value (REG_DWORD) with the given name.
		/// An empty name denotes the default value.
		///
		/// Throws a NotFoundException if the value does not exist.

#if defined(POCO_HAVE_INT64)

	void setInt64(const std::string& name, Poco::Int64 value);
		/// Sets the numeric (REG_QWORD) value with the given name.
		/// An empty name denotes the default value.

	Poco::Int64 getInt64(const std::string& name);
		/// Returns the numeric value (REG_QWORD) with the given name.
		/// An empty name denotes the default value.
		///
		/// Throws a NotFoundException if the value does not exist.

#endif // POCO_HAVE_INT64

	void deleteValue(const std::string& name);
		/// Deletes the value with the given name.
		///
		/// Throws a NotFoundException if the value does not exist.

	void deleteKey();
		/// Recursively deletes the key and all subkeys.

	bool exists();
		/// Returns true iff the key exists.

	Type type(const std::string& name);
		/// Returns the type of the key value.

	bool exists(const std::string& name);
		/// Returns true iff the given value exists under that key.

	void subKeys(Keys& keys);
		/// Appends all subKey names to keys.

	void values(Values& vals);
		/// Appends all value names to vals;

	bool isReadOnly() const;
		/// Returns true iff the key has been opened for read-only access only.

protected:
	void open();
	void close();
	std::string key() const;
	std::string key(const std::string& valueName) const;
	HKEY handle();
	void handleSetError(const std::string& name);
	static HKEY handleFor(const std::string& rootKey);

private:
	WinRegistryKey();
	WinRegistryKey(const WinRegistryKey&);
	WinRegistryKey& operator = (const WinRegistryKey&);

	HKEY        _hRootKey;
	std::string _subKey;
	HKEY        _hKey;
	bool        _readOnly;
	REGSAM      _extraSam;
};


//
// inlines
//
inline bool WinRegistryKey::isReadOnly() const
{
	return _readOnly;
}


} } // namespace Poco::Util


#endif // Util_WinRegistryKey_INCLUDED
