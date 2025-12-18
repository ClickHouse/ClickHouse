#pragma once

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Node.h>
#include <base/types.h>

#include <string>
#include <string_view>
#include <vector>


namespace DB
{

/// XML Parser wrapper around Poco DOM with namespace-stripping XPath support.
/// This class provides a simplified interface for parsing XML and extracting
/// values using XPath expressions. Namespace prefixes are stripped during
/// element matching, so 'Envelope' matches 'soap:Envelope'.
class PocoXMLParser
{
public:
    class Node;

    PocoXMLParser() = default;

    /// Parse XML document from string.
    /// Returns true on success, false on parse error.
    bool parse(std::string_view xml);

    /// Get the root element of the parsed document.
    /// Returns null node if no document is parsed.
    Node getRootNode() const;

    /// Get the underlying Poco document.
    Poco::XML::Document * getDocument() const { return document_.get(); }

private:
    Poco::XML::DOMParser parser_;
    Poco::AutoPtr<Poco::XML::Document> document_;
};


/// Wrapper around Poco::XML::Node providing convenient access methods.
class PocoXMLParser::Node
{
public:
    Node() = default;
    explicit Node(Poco::XML::Node * node) : node_(node) {}

    bool isNull() const { return node_ == nullptr; }
    bool isElement() const;
    bool isText() const;
    bool isAttribute() const;

    /// Get the node type.
    unsigned short nodeType() const;

    /// Get local name (without namespace prefix).
    /// For 'soap:Envelope', returns 'Envelope'.
    std::string localName() const;

    /// Get qualified name (with namespace prefix if present).
    std::string qname() const;

    /// Get text content (recursively concatenates all text nodes).
    std::string innerText() const;

    /// Get attribute value by name.
    std::string getAttribute(const std::string & name) const;

    /// Check if node has an attribute.
    bool hasAttribute(const std::string & name) const;

    /// Serialize node and subtree to XML string.
    std::string toXML() const;

    /// Get first child node.
    Node firstChild() const;

    /// Get next sibling node.
    Node nextSibling() const;

    /// Get parent node.
    Node parent() const;

    /// Get all child nodes.
    std::vector<Node> children() const;

    /// Get all child element nodes.
    std::vector<Node> childElements() const;

    /// Get underlying Poco node.
    Poco::XML::Node * raw() const { return node_; }

private:
    Poco::XML::Node * node_ = nullptr;
};


/// Extract local name from a qualified name.
/// For 'soap:Envelope', returns 'Envelope'.
/// For 'Envelope', returns 'Envelope'.
std::string extractLocalName(const std::string & qname);

}
