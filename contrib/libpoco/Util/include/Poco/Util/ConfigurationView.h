//
// ConfigurationView.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/ConfigurationView.h#1 $
//
// Library: Util
// Package: Configuration
// Module:  ConfigurationView
//
// Definition of the ConfigurationView class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_ConfigurationView_INCLUDED
#define Util_ConfigurationView_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/AbstractConfiguration.h"


namespace Poco {
namespace Util {


class Util_API ConfigurationView: public AbstractConfiguration
	/// This configuration implements a "view" into a sub-hierarchy 
	/// of another configuration.
	/// 
	/// For example, given a configuration with the following properties:
	///     config.value1
	///     config.value2
	///     config.sub.value1
	///     config.sub.value2
	/// and a ConfigurationView with the prefix "config", then
	/// the above properties will be available via the view as
	///     value1
	///     value2
	///     sub.value1
	///     sub.value2
	///
	/// A ConfigurationView is most useful in combination with a
	/// LayeredConfiguration.
	///
	/// If a property is not found in the view, it is searched in
	/// the original configuration. Given the above example configuration,
	/// the property named "config.value1" will still be found in the view.
	///
	/// The main reason for this is that placeholder expansion (${property})
	/// still works as expected given a ConfigurationView.
{
public:
	ConfigurationView(const std::string& prefix, AbstractConfiguration* pConfig);
		/// Creates the ConfigurationView. The ConfigurationView does not take
		/// ownership of the passed configuration.

protected:
	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);
	
	std::string translateKey(const std::string& key) const;
	
	~ConfigurationView();

private:
	ConfigurationView(const ConfigurationView&);
	ConfigurationView& operator = (const ConfigurationView&);

	std::string _prefix;
	AbstractConfiguration* _pConfig;
};


} } // namespace Poco::Util


#endif // Util_ConfigurationView_INCLUDED
