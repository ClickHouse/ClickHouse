//
// LayeredConfiguration.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/LayeredConfiguration.h#1 $
//
// Library: Util
// Package: Configuration
// Module:  LayeredConfiguration
//
// Definition of the LayeredConfiguration class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_LayeredConfiguration_INCLUDED
#define Util_LayeredConfiguration_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/AutoPtr.h"
#include <list>


namespace Poco {
namespace Util {


class Util_API LayeredConfiguration: public AbstractConfiguration
	/// A LayeredConfiguration consists of a number of AbstractConfigurations.
	///
	/// When reading a configuration property in a LayeredConfiguration,
	/// all added configurations are searched, in order of their priority.
	/// Configurations with lower priority values have precedence.
	///
	/// When setting a property, the property is always written to the first writeable 
	/// configuration (see addWriteable()). 
	/// If no writeable configuration has been added to the LayeredConfiguration, and an
	/// attempt is made to set a property, a RuntimeException is thrown.
	///
	/// Every configuration added to the LayeredConfiguration has a priority value.
	/// The priority determines the position where the configuration is inserted,
	/// with lower priority values coming before higher priority values.
	///
	/// If no priority is specified, a priority of 0 is assumed.
{
public:
	LayeredConfiguration();
		/// Creates the LayeredConfiguration.
		
	void add(AbstractConfiguration* pConfig);
		/// Adds a read-only configuration to the back of the LayeredConfiguration.
		/// The LayeredConfiguration does not take ownership of the given
		/// configuration. In other words, the configuration's reference
		/// count is incremented.

	void add(AbstractConfiguration* pConfig, bool shared);
		/// Adds a read-only configuration to the back of the LayeredConfiguration.
		/// If shared is false, the LayeredConfiguration takes ownership
		/// of the given configuration (and the configuration's reference
		/// count remains unchanged).

	void add(AbstractConfiguration* pConfig, int priority);
		/// Adds a read-only configuration to the LayeredConfiguration.
		/// The LayeredConfiguration does not take ownership of the given
		/// configuration. In other words, the configuration's reference
		/// count is incremented.

	void add(AbstractConfiguration* pConfig, int priority, bool shared);
		/// Adds a read-only configuration the LayeredConfiguration.
		/// If shared is false, the LayeredConfiguration takes ownership
		/// of the given configuration (and the configuration's reference
		/// count remains unchanged).

	void add(AbstractConfiguration* pConfig, int priority, bool writeable, bool shared);
		/// Adds a configuration to the LayeredConfiguration.
		/// If shared is false, the LayeredConfiguration takes ownership
		/// of the given configuration (and the configuration's reference
		/// count remains unchanged).

	void addWriteable(AbstractConfiguration* pConfig, int priority);
		/// Adds a writeable configuration to the LayeredConfiguration.
		/// The LayeredConfiguration does not take ownership of the given
		/// configuration. In other words, the configuration's reference
		/// count is incremented.

	void addWriteable(AbstractConfiguration* pConfig, int priority, bool shared);
		/// Adds a writeable configuration to the LayeredConfiguration.
		/// If shared is false, the LayeredConfiguration takes ownership
		/// of the given configuration (and the configuration's reference
		/// count remains unchanged).

	//@ deprecated
	void addFront(AbstractConfiguration* pConfig);
		/// Adds a read-only configuration to the front of the LayeredConfiguration.
		/// The LayeredConfiguration does not take ownership of the given
		/// configuration. In other words, the configuration's reference
		/// count is incremented.

	//@ deprecated
	void addFront(AbstractConfiguration* pConfig, bool shared);
		/// Adds a read-only configuration to the front of the LayeredConfiguration.
		/// If shared is true, the LayeredConfiguration takes ownership
		/// of the given configuration.
		
	void removeConfiguration(AbstractConfiguration* pConfig);
		/// Removes the given configuration from the LayeredConfiguration.
		///
		/// Does nothing if the given configuration is not part of the
		/// LayeredConfiguration.
		
protected:
	typedef Poco::AutoPtr<AbstractConfiguration> ConfigPtr;
	
	struct ConfigItem
	{
		ConfigPtr pConfig;
		int       priority;
		bool      writeable;
	};

	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);
	
	int lowest() const;
	int highest() const;
	void insert(const ConfigItem& item);
	
	~LayeredConfiguration();

private:
	LayeredConfiguration(const LayeredConfiguration&);
	LayeredConfiguration& operator = (const LayeredConfiguration&);

	typedef std::list<ConfigItem> ConfigList;
	
	ConfigList _configs;
};


} } // namespace Poco::Util


#endif // Util_LayeredConfiguration_INCLUDED
