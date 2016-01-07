//
// XMLConfiguration.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/XMLConfiguration.h#2 $
//
// Library: Util
// Package: Configuration
// Module:  XMLConfiguration
//
// Definition of the XMLConfiguration class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_XMLConfiguration_INCLUDED
#define Util_XMLConfiguration_INCLUDED


#include "Poco/Util/Util.h"


#ifndef POCO_UTIL_NO_XMLCONFIGURATION


#include "Poco/Util/MapConfiguration.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/AutoPtr.h"
#include "Poco/DOM/DOMWriter.h"
#include "Poco/SAX/InputSource.h"
#include <istream>


namespace Poco {
namespace Util {


class Util_API XMLConfiguration: public AbstractConfiguration
	/// This configuration class extracts configuration properties
	/// from an XML document. An XPath-like syntax for property
	/// names is supported to allow full access to the XML document.
	/// XML namespaces are not supported. The name of the root element
	/// of the XML document is not significant and ignored. 
	/// Periods in tag names are not supported.
	/// 
	/// Given the following XML document as an example:
	///
	///     <config>
	///         <prop1>value1</prop1>
	///         <prop2>value2</prop2>
	///         <prop3>
	///            <prop4 attr="value3"/>
	///            <prop4 attr="value4"/>
	///         </prop3>
	///         <prop5 id="first">value5</prop5>
	///         <prop5 id="second">value6</prop5>
	///     </config>
	///
	/// The following property names would be valid and would
	/// yield the shown values:
	///
	///     prop1                 -> value1
	///     prop2                 -> value2
	///     prop3.prop4           -> (empty string)
	///     prop3.prop4[@attr]    -> value3
	///     prop3.prop4[1][@attr] -> value4
	///     prop5[0]              -> value5
	///     prop5[1]              -> value6
	///     prop5[@id=first]      -> value5
	///     prop5[@id='second']   -> value6
	///
	/// Enumerating attributes is not supported.
	/// Calling keys("prop3.prop4") will return an empty range.
	///
	/// As a special feature, the delimiter character used to delimit
	/// property names can be changed to something other than period ('.') by
	/// passing the desired character to the constructor. This allows
	/// working with XML documents having element names with periods
	/// in them.
{
public:
	XMLConfiguration();
		/// Creates an empty XMLConfiguration.

	XMLConfiguration(char delim);
		/// Creates an empty XMLConfiguration, using the given
		/// delimiter char instead of the default '.'.

	XMLConfiguration(Poco::XML::InputSource* pInputSource);
		/// Creates an XMLConfiguration and loads the XML document from
		/// the given InputSource.

	XMLConfiguration(Poco::XML::InputSource* pInputSource, char delim);
		/// Creates an XMLConfiguration and loads the XML document from
		/// the given InputSource. Uses the given delimiter char instead
		/// of the default '.'.

	XMLConfiguration(std::istream& istr);
		/// Creates an XMLConfiguration and loads the XML document from
		/// the given stream.

	XMLConfiguration(std::istream& istr, char delim);
		/// Creates an XMLConfiguration and loads the XML document from
		/// the given stream. Uses the given delimiter char instead
		/// of the default '.'.

	XMLConfiguration(const std::string& path);
		/// Creates an XMLConfiguration and loads the XML document from
		/// the given path.

	XMLConfiguration(const std::string& path, char delim);
		/// Creates an XMLConfiguration and loads the XML document from
		/// the given path. Uses the given delimiter char instead
		/// of the default '.'.

	XMLConfiguration(const Poco::XML::Document* pDocument);
		/// Creates the XMLConfiguration using the given XML document.

	XMLConfiguration(const Poco::XML::Document* pDocument, char delim);
		/// Creates the XMLConfiguration using the given XML document.
		/// Uses the given delimiter char instead of the default '.'.
		
	XMLConfiguration(const Poco::XML::Node* pNode);
		/// Creates the XMLConfiguration using the given XML node.

	XMLConfiguration(const Poco::XML::Node* pNode, char delim);
		/// Creates the XMLConfiguration using the given XML node.
		/// Uses the given delimiter char instead of the default '.'.

	void load(Poco::XML::InputSource* pInputSource);
		/// Loads the XML document containing the configuration data
		/// from the given InputSource.

	void load(std::istream& istr);
		/// Loads the XML document containing the configuration data
		/// from the given stream.
		
	void load(const std::string& path);
		/// Loads the XML document containing the configuration data
		/// from the given file.
		
	void load(const Poco::XML::Document* pDocument);
		/// Loads the XML document containing the configuration data
		/// from the given XML document.

	void load(const Poco::XML::Node* pNode);
		/// Loads the XML document containing the configuration data
		/// from the given XML node.
	
	void loadEmpty(const std::string& rootElementName);
		/// Loads an empty XML document containing only the
		/// root element with the given name.
		
	void save(const std::string& path) const;
		/// Writes the XML document containing the configuration data
		/// to the file given by path.

	void save(std::ostream& str) const;
		/// Writes the XML document containing the configuration data
		/// to the given stream.

	void save(Poco::XML::DOMWriter& writer, const std::string& path) const;
		/// Writes the XML document containing the configuration data
		/// to the file given by path, using the given DOMWriter.
		///
		/// This can be used to use a DOMWriter with custom options.

	void save(Poco::XML::DOMWriter& writer, std::ostream& str) const;
		/// Writes the XML document containing the configuration data
		/// to the given stream.
		///
		/// This can be used to use a DOMWriter with custom options.

protected:
	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);
	~XMLConfiguration();

private:
	const Poco::XML::Node* findNode(const std::string& key) const;
	Poco::XML::Node* findNode(const std::string& key);
	Poco::XML::Node* findNode(std::string::const_iterator& it, const std::string::const_iterator& end, Poco::XML::Node* pNode, bool create = false) const;
	static Poco::XML::Node* findElement(const std::string& name, Poco::XML::Node* pNode, bool create);
	static Poco::XML::Node* findElement(int index, Poco::XML::Node* pNode, bool create);
	static Poco::XML::Node* findElement(const std::string& attr, const std::string& value, Poco::XML::Node* pNode);
	static Poco::XML::Node* findAttribute(const std::string& name, Poco::XML::Node* pNode, bool create);

	Poco::XML::AutoPtr<Poco::XML::Node>     _pRoot;
	Poco::XML::AutoPtr<Poco::XML::Document> _pDocument;
	char _delim;
};


} } // namespace Poco::Util


#endif // POCO_UTIL_NO_XMLCONFIGURATION


#endif // Util_XMLConfiguration_INCLUDED
