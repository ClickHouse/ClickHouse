//
// IniFileConfiguration.h
//
// Library: Util
// Package: Configuration
// Module:  IniFileConfiguration
//
// Definition of the IniFileConfiguration class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_IniFileConfiguration_INCLUDED
#define Util_IniFileConfiguration_INCLUDED


#include "Poco/Util/Util.h"


#ifndef POCO_UTIL_NO_INIFILECONFIGURATION


#include "Poco/Util/AbstractConfiguration.h"
#include <map>
#include <istream>


namespace Poco {
namespace Util {


class Util_API IniFileConfiguration: public AbstractConfiguration
	/// This implementation of a Configuration reads properties
	/// from a legacy Windows initialization (.ini) file.
	///
	/// The file syntax is implemented as follows.
	///   - a line starting with a semicolon is treated as a comment and ignored
	///   - a line starting with a square bracket denotes a section key [<key>]
	///   - every other line denotes a property assignment in the form
	///     <value key> = <value>
	///
	/// The name of a property is composed of the section key and the value key,
	/// separated by a period (<section key>.<value key>).
	///
	/// Property names are not case sensitive. Leading and trailing whitespace is
	/// removed from both keys and values.
{
public:
	IniFileConfiguration();
		/// Creates an empty IniFileConfiguration.

	IniFileConfiguration(std::istream& istr);
		/// Creates an IniFileConfiguration and loads the configuration data
		/// from the given stream, which must be in initialization file format.
		
	IniFileConfiguration(const std::string& path);
		/// Creates an IniFileConfiguration and loads the configuration data
		/// from the given file, which must be in initialization file format.
		
	void load(std::istream& istr);
		/// Loads the configuration data from the given stream, which 
		/// must be in initialization file format.
		
	void load(const std::string& path);
		/// Loads the configuration data from the given file, which 
		/// must be in initialization file format.

protected:
	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);
	~IniFileConfiguration();

private:
	void parseLine(std::istream& istr);

	struct ICompare
	{
		bool operator () (const std::string& s1, const std::string& s2) const;
	};
	typedef std::map<std::string, std::string, ICompare> IStringMap;
	
	IStringMap _map;
	std::string _sectionKey;
};


} } // namespace Poco::Util


#endif // POCO_UTIL_NO_INIFILECONFIGURATION


#endif // Util_IniFileConfiguration_INCLUDED
