#if !defined(ARCADIA_BUILD)
    #include <Common/config.h>
#endif

#if USE_YAML_CPP
#include "YAMLParser.h"

#include <string>
#include <cstring>
#include <vector>

#include <Poco/DOM/Document.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/AutoPtr.h>
#include <Poco/DOM/NamedNodeMap.h>
#include <Poco/DOM/Text.h>
#include <Common/Exception.h>

#include <yaml-cpp/yaml.h> // Y_IGNORE

#include <common/logger_useful.h>

using namespace Poco::XML;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_PARSE_YAML;
}

/// A prefix symbol in yaml key
/// We add attributes to nodes by using a prefix symbol in the key part.
/// Currently we use @ as a prefix symbol. Note, that @ is reserved
/// by YAML standard, so we need to write a key-value pair like this: "@attribute": attr_value
const char YAML_ATTRIBUTE_PREFIX = '@';

namespace
{

Poco::AutoPtr<Poco::XML::Element> createCloneNode(Poco::XML::Element & original_node)
{
    Poco::AutoPtr<Poco::XML::Element> clone_node = original_node.ownerDocument()->createElement(original_node.nodeName());
    original_node.parentNode()->appendChild(clone_node);
    return clone_node;
}

void processNode(const YAML::Node & node, Poco::XML::Element & parent_xml_element)
{
    auto * xml_document = parent_xml_element.ownerDocument();
    switch (node.Type())
    {
        case YAML::NodeType::Scalar:
        {
            std::string value = node.as<std::string>();
            Poco::AutoPtr<Poco::XML::Text> xml_value = xml_document->createTextNode(value);
            parent_xml_element.appendChild(xml_value);
            break;
        }

        /// We process YAML Sequences as a
        /// list of <key>value</key> tags with same key and different values.
        /// For example, we translate this sequence
        /// seq:
        ///     - val1
        ///     - val2
        ///
        /// into this:
        /// <seq>val1</seq>
        /// <seq>val2</seq>
        case YAML::NodeType::Sequence:
        {
            for (const auto & child_node : node)
                if (parent_xml_element.hasChildNodes())
                {
                    /// We want to process sequences like that:
                    /// seq:
                    ///     - val1
                    ///     - k2: val2
                    ///     - val3
                    ///     - k4: val4
                    ///     - val5
                    /// into xml like this:
                    /// <seq>val1</seq>
                    /// <seq>
                    ///     <k2>val2</k2>
                    /// </seq>
                    /// <seq>val3</seq>
                    /// <seq>
                    ///     <k4>val4</k4>
                    /// </seq>
                    /// <seq>val5</seq>
                    /// So, we create a new parent node with same tag for each child node
                    processNode(child_node, *createCloneNode(parent_xml_element));
                }
                else
                {
                    processNode(child_node, parent_xml_element);
                }
            break;
        }
        case YAML::NodeType::Map:
        {
            for (const auto & key_value_pair : node)
            {
                const auto & key_node = key_value_pair.first;
                const auto & value_node = key_value_pair.second;
                std::string key = key_node.as<std::string>();
                bool is_attribute = (key.starts_with(YAML_ATTRIBUTE_PREFIX) && value_node.IsScalar());
                if (is_attribute)
                {
                    /// we use substr(1) here to remove YAML_ATTRIBUTE_PREFIX from key
                    auto attribute_name = key.substr(1);
                    std::string value = value_node.as<std::string>();
                    parent_xml_element.setAttribute(attribute_name, value);
                }
                else
                {
                    Poco::AutoPtr<Poco::XML::Element> xml_key = xml_document->createElement(key);
                    parent_xml_element.appendChild(xml_key);
                    processNode(value_node, *xml_key);
                }
            }
            break;
        }
        case YAML::NodeType::Null: break;
        case YAML::NodeType::Undefined:
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "YAMLParser has encountered node with undefined type and cannot continue parsing of the file");
        }
    }
}

}

Poco::AutoPtr<Poco::XML::Document> YAMLParser::parse(const String& path)
{
    YAML::Node node_yml;
    try
    {
        node_yml = YAML::LoadFile(path);
    }
    catch (const YAML::ParserException& e)
    {
        /// yaml-cpp cannot parse the file because its contents are incorrect
        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "Unable to parse YAML configuration file {}, {}", path, e.what());
    }
    catch (const YAML::BadFile&)
    {
        /// yaml-cpp cannot open the file even though it exists
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Unable to open YAML configuration file {}", path);
    }
    Poco::AutoPtr<Poco::XML::Document> xml = new Document;
    Poco::AutoPtr<Poco::XML::Element> root_node = xml->createElement("yandex");
    xml->appendChild(root_node);
    try
    {
        processNode(node_yml, *root_node);
    }
    catch (const YAML::TypedBadConversion<std::string>&)
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "YAMLParser has encountered node with key or value which cannot be represented as string and cannot continue parsing of the file");
    }
    return xml;
}

}
#endif
