//
// NamespaceSupport.h
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Namespace support for SAX2.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_NamespaceSupport_INCLUDED
#define SAX_NamespaceSupport_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"
#include <set>
#include <map>
#include <vector>


namespace Poco {
namespace XML {


class XML_API NamespaceSupport
	/// Encapsulate Namespace logic for use by SAX drivers. 
	/// This class encapsulates the logic of Namespace processing: 
	/// it tracks the declarations currently in force for each context and
	/// automatically processes qualified XML 1.0 names into their Namespace 
	/// parts; it can also be used in reverse for generating
	/// XML 1.0 from Namespaces.
	/// Namespace support objects are reusable, but the reset method 
	/// must be invoked between each session.
{
public:
	using PrefixSet = std::set<XMLString>;

	NamespaceSupport();
		/// Creates a NamespaceSupport object.
		
	~NamespaceSupport();
		/// Destroys a NamespaceSupport object.
	
	bool declarePrefix(const XMLString& prefix, const XMLString& namespaceURI);
		/// Declare a Namespace prefix. All prefixes must be declared before they are
		/// referenced. For example, a SAX driver (parser) would scan an element's attributes
		/// in two passes: first for namespace declarations, then a second pass using
		/// processName() to interpret prefixes against (potentially redefined) prefixes.
		/// 
		/// This method declares a prefix in the current Namespace context; the prefix
		/// will remain in force until this context is popped, unless it is shadowed
		/// in a descendant context.
		/// 
		/// To declare the default element Namespace, use the empty string as the prefix.
		/// 
		/// Note that you must not declare a prefix after you've pushed and popped another
		/// Namespace context, or treated the declarations phase as complete by processing
		/// a prefixed name.
		///
		/// Returns true if the prefix was legal, false otherwise.
	
	bool undeclarePrefix(const XMLString& prefix);
		/// Remove the given namespace prefix.
	
	void getDeclaredPrefixes(PrefixSet& prefixes) const;
		/// Return an enumeration of all prefixes declared in this context.
		/// 
		/// The empty (default) prefix will be included in this enumeration; note that
		/// this behaviour differs from that of getPrefix() and getPrefixes().

	const XMLString& getPrefix(const XMLString& namespaceURI) const;
		/// Return one of the prefixes mapped to a Namespace URI.
		/// 
		/// If more than one prefix is currently mapped to the same URI, this method
		/// will make an arbitrary selection; if you want all of the prefixes, use the
		/// getPrefixes() method instead.

	bool isMapped(const XMLString& namespaceURI) const;
		/// Returns true if the given namespaceURI has been mapped to a prefix,
		/// false otherwise.

	void getPrefixes(PrefixSet& prefixes) const;
		/// Return an enumeration of all prefixes whose declarations are active in the
		/// current context. This includes declarations from parent contexts that have
		/// not been overridden.
		/// 
		/// Note: if there is a default prefix, it will not be returned in this enumeration;
		/// check for the default prefix using the getURI with an argument of "".

	void getPrefixes(const XMLString& namespaceURI, PrefixSet& prefixes) const;
		/// Return an enumeration of all prefixes for a given URI whose declarations
		/// are active in the current context. This includes declarations from parent
		/// contexts that have not been overridden.
		/// 
		/// This method returns prefixes mapped to a specific Namespace URI. The xml:
		/// prefix will be included. If you want only one prefix that's mapped to the
		/// Namespace URI, and you don't care which one you get, use the getPrefix() method
		/// instead.
		/// 
		/// Note: the empty (default) prefix is never included in this enumeration;
		/// to check for the presence of a default Namespace, use the getURI() method
		/// with an argument of "".

	const XMLString& getURI(const XMLString& prefix) const;
		/// Look up a prefix and get the currently-mapped Namespace URI.
		/// 
		/// This method looks up the prefix in the current context. Use the empty string
		/// ("") for the default Namespace.

	void pushContext();
		/// Start a new Namespace context. The new context will automatically inherit
		/// the declarations of its parent context, but it will also keep track of which
		/// declarations were made within this context.
		/// 
		/// Event callback code should start a new context once per element. This means
		/// being ready to call this in either of two places. For elements that don't
		/// include namespace declarations, the ContentHandler::startElement() callback
		/// is the right place. For elements with such a declaration, it'd done in the
		/// first ContentHandler::startPrefixMapping() callback. A boolean flag can be
		/// used to track whether a context has been started yet. When either of those
		/// methods is called, it checks the flag to see if a new context needs to be
		/// started. If so, it starts the context and sets the flag. After 
		/// ContentHandler::startElement() does that, it always clears the flag.
		/// 
		/// Normally, SAX drivers would push a new context at the beginning of each
		/// XML element. Then they perform a first pass over the attributes to process
		/// all namespace declarations, making ContentHandler::startPrefixMapping() callbacks.
		/// Then a second pass is made, to determine the namespace-qualified names for
		/// all attributes and for the element name. Finally all the information for
		/// the ContentHandler::startElement() callback is available, so it can then
		/// be made.
		/// 
		/// The Namespace support object always starts with a base context already in
		/// force: in this context, only the "xml" prefix is declared.

	void popContext();
		/// Revert to the previous Namespace context.
		/// 
		/// Normally, you should pop the context at the end of each XML element. After
		/// popping the context, all Namespace prefix mappings that were previously
		/// in force are restored.
		/// 
		/// You must not attempt to declare additional Namespace prefixes after popping
		/// a context, unless you push another context first.

	bool processName(const XMLString& qname, XMLString& namespaceURI, XMLString& localName, bool isAttribute) const;
		/// Process a raw XML 1.0 name. 
		/// This method processes a raw XML 1.0 name in the current context 
		/// by removing the prefix and looking it up among the
		/// prefixes currently declared. The result will be returned in
		/// namespaceURI and localName.
		/// If the raw name has a prefix that has not been declared, then the return
		/// value will be false, otherwise true.
		///
		/// Note that attribute names are processed differently than element names: 
		/// an unprefixed element name will receive the
		/// default Namespace (if any), while an unprefixed attribute name will not.

	void reset();
		/// Reset this Namespace support object for reuse.
		/// 
		/// It is necessary to invoke this method before reusing the Namespace support
		/// object for a new session. If namespace declaration URIs are to be supported,
		/// that flag must also be set to a non-default value.
		/// Reset this Namespace support object for reuse.

	static const XMLString XML_NAMESPACE;
	static const XMLString XML_NAMESPACE_PREFIX;
	static const XMLString XMLNS_NAMESPACE;
	static const XMLString XMLNS_NAMESPACE_PREFIX;

private:
	NamespaceSupport(const NamespaceSupport&);
	NamespaceSupport& operator = (const NamespaceSupport&);

	typedef std::map<XMLString, XMLString> Context;
	typedef std::vector<Context> ContextVec;
	
	ContextVec _contexts;

	static const XMLString EMPTY_STRING;
};


} } // namespace Poco::XML


#endif // SAX_NamespaceSupport_INCLUDED
