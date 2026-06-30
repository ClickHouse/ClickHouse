#pragma once

#include "config.h"

#include <base/types.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/AutoPtr.h>

#if USE_YAML_CPP

namespace DB
{

/// Real YAML parser: loads yaml file into a YAML::Node
class YAMLParserImpl
{
public:
    static Poco::AutoPtr<Poco::XML::Document> parse(const String& path);

    /// Parse YAML directly from a string (for example, the contents of a ZooKeeper node referenced
    /// by a structural `<include from_zk=.../>`).
    static Poco::AutoPtr<Poco::XML::Document> parseString(const String & yaml);
};

using YAMLParser = YAMLParserImpl;

}

#else

namespace DB
{

/// Fake YAML parser: throws an exception if we try to parse YAML configs in a build without yaml-cpp
class DummyYAMLParser
{
public:
    static Poco::AutoPtr<Poco::XML::Document> parse(const String & path);
    static Poco::AutoPtr<Poco::XML::Document> parseString(const String & yaml);
};

using YAMLParser = DummyYAMLParser;

}

#endif
