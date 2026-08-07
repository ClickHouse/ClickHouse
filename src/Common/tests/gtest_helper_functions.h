#pragma once

#include <filesystem>

#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/Config/ConfigProcessor.h>
#include <Poco/AutoPtr.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/DOM/NamedNodeMap.h>

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

struct EnvironmentProxySetter
{
    static constexpr auto * NO_PROXY = "*";
    static constexpr auto * HTTP_PROXY = "http://proxy_server:3128";
    static constexpr auto * HTTPS_PROXY = "https://proxy_server:3128";

    EnvironmentProxySetter()
    {
        setenv("http_proxy", HTTP_PROXY, 1); // NOLINT(concurrency-mt-unsafe)

        setenv("https_proxy", HTTPS_PROXY, 1); // NOLINT(concurrency-mt-unsafe)

        // Some other tests rely on HTTP clients (e.g, gtest_aws_s3_client), which depend on proxy configuration
        // since in https://github.com/ClickHouse/ClickHouse/pull/63314 the environment proxy resolver reads only once
        // from the environment, the proxy configuration will always be there.
        // The problem is that the proxy server does not exist, causing the test to fail.
        // To work around this issue, `no_proxy` is set to bypass all domains.
        setenv("no_proxy", NO_PROXY, 1); // NOLINT(concurrency-mt-unsafe)
    }

    ~EnvironmentProxySetter()
    {
        unsetenv("http_proxy"); // NOLINT(concurrency-mt-unsafe)
        unsetenv("https_proxy"); // NOLINT(concurrency-mt-unsafe)
        unsetenv("no_proxy"); // NOLINT(concurrency-mt-unsafe)
    }
};
