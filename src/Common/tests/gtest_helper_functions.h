#pragma once

#include <filesystem>

#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/Config/ConfigProcessor.h>
#include <Poco/AutoPtr.h>
#include "Poco/DOM/Document.h"
#include "Poco/DOM/NodeList.h"
#include "Poco/DOM/NamedNodeMap.h"

const std::string tmp_path = "/tmp/";

inline std::unique_ptr<Poco::File> getFileWithContents(const char *fileName, const char *fileContents)
{
    using namespace DB;
    namespace fs = std::filesystem;
    using File = Poco::File;


    fs::create_directories(fs::path(tmp_path));
    auto config_file = std::make_unique<File>(tmp_path + fileName);

    {
        WriteBufferFromFile out(config_file->path());
        writeString(fileContents, out);
        out.finalize();
    }

    return config_file;
}

inline std::string xmlNodeAsString(Poco::XML::Node *pNode)
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
