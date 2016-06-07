//
// ConfigurationMapper.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/ConfigurationMapper.h#1 $
//
// Library: Util
// Package: Configuration
// Module:  ConfigurationMapper
//
// Definition of the ConfigurationMapper class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_ConfigurationMapper_INCLUDED
#define Util_ConfigurationMapper_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/AbstractConfiguration.h"


namespace Poco {
namespace Util {


class Util_API ConfigurationMapper: public AbstractConfiguration
	/// This configuration maps a property hierarchy into another
	/// hierarchy.
	/// 
	/// For example, given a configuration with the following properties:
	///     config.value1
	///     config.value2
	///     config.sub.value1
	///     config.sub.value2
	/// and a ConfigurationView with fromPrefix == "config" and toPrefix == "root.conf", then
	/// the above properties will be available via the mapper as
	///     root.conf.value1
	///     root.conf.value2
	///     root.conf.sub.value1
	///     root.conf.sub.value2
	///
	/// FromPrefix can be empty, in which case, and given toPrefix == "root",
	/// the properties will be available as
	///     root.config.value1
	///     root.config.value2
	///     root.config.sub.value1
	///     root.config.sub.value2
	///
	/// This is equivalent to the functionality of the ConfigurationView class.
	///
	/// Similarly, toPrefix can also be empty. Given fromPrefix == "config" and
	/// toPrefix == "", the properties will be available as
	///     value1
	///     value2
	///     sub.value1
	///     sub.value2
	///
	/// If both fromPrefix and toPrefix are empty, no mapping is performed.
	///
	/// A ConfigurationMapper is most useful in combination with a
	/// LayeredConfiguration.
{
public:
	ConfigurationMapper(const std::string& fromPrefix, const std::string& toPrefix, AbstractConfiguration* pConfig);
		/// Creates the ConfigurationMapper. The ConfigurationMapper does not take
		/// ownership of the passed configuration.

protected:
	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);
	
	std::string translateKey(const std::string& key) const;
	
	~ConfigurationMapper();

private:
	ConfigurationMapper(const ConfigurationMapper&);
	ConfigurationMapper& operator = (const ConfigurationMapper&);

	std::string _fromPrefix;
	std::string _toPrefix;
	AbstractConfiguration* _pConfig;
};


} } // namespace Poco::Util


#endif // Util_ConfigurationMapper_INCLUDED
