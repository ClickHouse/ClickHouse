//
// JSONConfiguration.h
//
// $Id$
//
// Library: Util
// Package: Util
// Module:  JSONConfiguration
//
// Definition of the JSONConfiguration class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_JSONConfiguration_INCLUDED
#define Util_JSONConfiguration_INCLUDED


#include "Poco/Util/Util.h"


#ifndef POCO_UTIL_NO_JSONCONFIGURATION


#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/JSON/Object.h"
#include <istream>


namespace Poco {
namespace Util {


class Util_API JSONConfiguration : public AbstractConfiguration
	/// This configuration class extracts configuration properties
	/// from a JSON object. An XPath-like syntax for property
	/// names is supported to allow full access to the JSON object.
	///
	/// Given the following JSON object as an example:
	/// {
	///   "config" : {
	///      "prop1" : "value1",
	///      "prop2" : 10,
	///      "prop3" : [
	///        "element1",
	///        "element2"
	///      ],
	///      "prop4" : {
	///        "prop5" : false,
	///        "prop6" : null
	///      }
	///   }
	/// }
	///	The following property names would be valid and would
	/// yield the shown values:
	///
	/// config.prop1       --> "value1"
	/// config.prop3[1]    --> "element2"
	/// config.prop4.prop5 --> false
{
public:

	JSONConfiguration();
		/// Creates an empty configuration


	JSONConfiguration(const std::string& path);
		/// Creates a configuration and loads the JSON structure from the given file


	JSONConfiguration(std::istream& istr);
		/// Creates a configuration and loads the JSON structure from the given stream


	JSONConfiguration(const JSON::Object::Ptr& object);
		/// Creates a configuration from the given JSON object


	virtual ~JSONConfiguration();
		/// Destructor


	void load(const std::string& path);
		/// Loads the configuration from the given file


	void load(std::istream& istr);
		/// Loads the configuration from the given stream


	void loadEmpty(const std::string& root);
		/// Loads an empty object containing only a root object with the given name.


	void save(std::ostream& ostr, unsigned int indent = 2) const;
		/// Saves the configuration to the given stream


	virtual void setInt(const std::string& key, int value);


	virtual void setBool(const std::string& key, bool value);


	virtual void setDouble(const std::string& key, double value);


	virtual void setString(const std::string& key, const std::string& value);
	

	virtual void removeRaw(const std::string& key);	
	

protected:

	bool getRaw(const std::string & key, std::string & value) const;


	void setRaw(const std::string& key, const std::string& value);


	void enumerate(const std::string& key, Keys& range) const;


private:


	JSON::Object::Ptr findStart(const std::string& key, std::string& lastPart);


	void getIndexes(std::string& name, std::vector<int>& indexes);


	void setValue(const std::string& key, const Poco::DynamicAny& value);


 	JSON::Object::Ptr _object;
};


} } // namespace Poco::Util


#endif // POCO_UTIL_NO_JSONCONFIGURATION


#endif // Util_JSONConfiguration_INCLUDED

