#pragma once

#include "Poco/DOM/Document.h"
#include "Poco/DOM/NodeList.h"
#include "Poco/DOM/NamedNodeMap.h"


inline char *getTempFileWithContents(const char *fileContents)
{
    auto *file_pointer = std::tmpnam (nullptr);
    auto *file_stream = std::fopen(file_pointer, "w+");
    std::fputs(fileContents, file_stream);
    std::fclose(file_stream);

    return file_pointer;
}

inline std::string xmlNodeAsString(Poco::XML::Node* &pNode)
{
    const auto& node_name = pNode->nodeName();

    Poco::XML::XMLString result = "<" + node_name ;

    auto *attributes = pNode->attributes();
    for (auto i = 0; i<attributes->length();i++)
    {
        auto *item = attributes->item(i);
        auto name = item->nodeName();
        auto text = item->innerText();
        result += (" " + name + "=\"" + text + "\"");
    }

    result += ">";
    if (pNode->hasChildNodes() && pNode->firstChild()->nodeType() != Poco::XML::Node::TEXT_NODE)
    {
        result += "\n";
    }

    attributes->release();

    auto *list = pNode->childNodes();
    for (auto i = 0; i<list->length();i++)
    {
        auto *item = list->item(i);
        auto type = item->nodeType();
        if (type == Poco::XML::Node::ELEMENT_NODE)
        {
            result += xmlNodeAsString(item);
        }
        else if (type == Poco::XML::Node::TEXT_NODE)
        {
            result += item->innerText();
        }

    }
    list->release();
    result += ("</"+ node_name + ">\n");
    return Poco::XML::fromXMLString(result);
}
