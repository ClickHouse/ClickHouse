#include <Common/XMLParsers/PocoXMLParser.h>

#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/NamedNodeMap.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/Exception.h>
#include <Poco/XML/XMLWriter.h>

#include <sstream>


namespace DB
{

bool PocoXMLParser::parse(std::string_view xml)
{
    try
    {
        document_ = parser_.parseMemory(xml.data(), xml.size());
        return document_ != nullptr;
    }
    catch (const Poco::Exception &)
    {
        document_ = nullptr;
        return false;
    }
}

PocoXMLParser::Node PocoXMLParser::getRootNode() const
{
    if (!document_)
        return Node(nullptr);

    /// Skip comment nodes at the top level to find the root element.
    for (Poco::XML::Node * child = document_->firstChild(); child; child = child->nextSibling())
    {
        if (child->nodeType() == Poco::XML::Node::ELEMENT_NODE)
            return Node(child);
    }

    return Node(nullptr);
}


bool PocoXMLParser::Node::isElement() const
{
    return node_ && node_->nodeType() == Poco::XML::Node::ELEMENT_NODE;
}

bool PocoXMLParser::Node::isText() const
{
    return node_ && (node_->nodeType() == Poco::XML::Node::TEXT_NODE
                     || node_->nodeType() == Poco::XML::Node::CDATA_SECTION_NODE);
}

bool PocoXMLParser::Node::isAttribute() const
{
    return node_ && node_->nodeType() == Poco::XML::Node::ATTRIBUTE_NODE;
}

unsigned short PocoXMLParser::Node::nodeType() const
{
    return node_ ? node_->nodeType() : 0;
}

std::string PocoXMLParser::Node::localName() const
{
    if (!node_)
        return {};
    return extractLocalName(node_->nodeName());
}

std::string PocoXMLParser::Node::qname() const
{
    if (!node_)
        return {};
    return node_->nodeName();
}

std::string PocoXMLParser::Node::innerText() const
{
    if (!node_)
        return {};
    return node_->innerText();
}

std::string PocoXMLParser::Node::getAttribute(const std::string & name) const
{
    if (!node_ || node_->nodeType() != Poco::XML::Node::ELEMENT_NODE)
        return {};

    auto * element = static_cast<Poco::XML::Element *>(node_);
    return element->getAttribute(name);
}

bool PocoXMLParser::Node::hasAttribute(const std::string & name) const
{
    if (!node_ || node_->nodeType() != Poco::XML::Node::ELEMENT_NODE)
        return false;

    auto * element = static_cast<Poco::XML::Element *>(node_);
    return element->hasAttribute(name);
}

std::string PocoXMLParser::Node::toXML() const
{
    if (!node_)
        return {};

    try
    {
        std::ostringstream oss;
        Poco::XML::DOMWriter writer;
        writer.setOptions(Poco::XML::XMLWriter::WRITE_XML_DECLARATION);
        writer.writeNode(oss, node_);
        return oss.str();
    }
    catch (const Poco::Exception &)
    {
        return {};
    }
}

PocoXMLParser::Node PocoXMLParser::Node::firstChild() const
{
    if (!node_)
        return Node(nullptr);
    return Node(node_->firstChild());
}

PocoXMLParser::Node PocoXMLParser::Node::nextSibling() const
{
    if (!node_)
        return Node(nullptr);
    return Node(node_->nextSibling());
}

PocoXMLParser::Node PocoXMLParser::Node::parent() const
{
    if (!node_)
        return Node(nullptr);
    return Node(node_->parentNode());
}

std::vector<PocoXMLParser::Node> PocoXMLParser::Node::children() const
{
    std::vector<Node> result;
    if (!node_)
        return result;

    for (Poco::XML::Node * child = node_->firstChild(); child; child = child->nextSibling())
        result.emplace_back(child);

    return result;
}

std::vector<PocoXMLParser::Node> PocoXMLParser::Node::childElements() const
{
    std::vector<Node> result;
    if (!node_)
        return result;

    for (Poco::XML::Node * child = node_->firstChild(); child; child = child->nextSibling())
    {
        if (child->nodeType() == Poco::XML::Node::ELEMENT_NODE)
            result.emplace_back(child);
    }

    return result;
}


std::string extractLocalName(const std::string & qname)
{
    auto pos = qname.find(':');
    if (pos != std::string::npos)
        return qname.substr(pos + 1);
    return qname;
}

}
