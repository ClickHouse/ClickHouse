#pragma once

#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Node.h>
#include <Poco/AutoPtr.h>
#include <base/types.h>

namespace DB:: XMLUtils
{
/// Returns root element of the document.
Poco::XML::Node * getRootNode(Poco::XML::Document * document);

/// Finds the element in the node's subtree by the specified path and returns its inner text
/// trying to parse it as the requested type.
/// Throws an exception if path is not found.
std::string getString(const Poco::XML::Node * node, const std::string & path);
Int64 getInt64(const Poco::XML::Node * node, const std::string & path);
UInt64 getUInt64(const Poco::XML::Node * node, const std::string & path);
int getInt(const Poco::XML::Node * node, const std::string & path);
unsigned getUInt(const Poco::XML::Node * node, const std::string & path);
bool getBool(const Poco::XML::Node * node, const std::string & path);

/// Finds the element in the node's subtree by the specified path and returns its inner text
/// trying to parse it as the requested type.
/// Returns the specified default value if path is not found.
std::string getString(const Poco::XML::Node * node, const std::string & path, const std::string & default_value);
Int64 getInt64(const Poco::XML::Node * node, const std::string & path, Int64 default_value);
UInt64 getUInt64(const Poco::XML::Node * node, const std::string & path, UInt64 default_value);
int getInt(const Poco::XML::Node * node, const std::string & path, int default_value);
unsigned getUInt(const Poco::XML::Node * node, const std::string & path, unsigned default_value);
bool getBool(const Poco::XML::Node * node, const std::string & path, bool default_value);
}
