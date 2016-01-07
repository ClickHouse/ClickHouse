//
// MapConfiguration.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/MapConfiguration.h#1 $
//
// Library: Util
// Package: Configuration
// Module:  MapConfiguration
//
// Definition of the MapConfiguration class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_MapConfiguration_INCLUDED
#define Util_MapConfiguration_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/AbstractConfiguration.h"
#include <map>


namespace Poco {
namespace Util {


class Util_API MapConfiguration: public AbstractConfiguration
	/// An implementation of AbstractConfiguration that stores configuration data in a map.
{
public:
	MapConfiguration();
		/// Creates an empty MapConfiguration.

	void clear();
		/// Clears the configuration.

protected:
	typedef std::map<std::string, std::string> StringMap;
	typedef StringMap::const_iterator iterator;

	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);
	~MapConfiguration();

	iterator begin() const;
	iterator end() const;

private:	
	StringMap _map;
};


} } // namespace Poco::Util


#endif // Util_MapConfiguration_INCLUDED
