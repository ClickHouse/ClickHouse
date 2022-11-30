#pragma once

#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Node.h>
#include <Poco/AutoPtr.h>
#include <base/types.h>

namespace DB:: XMLUtils
{
Poco::XML::Node * getRootNode(Poco::XML::Document * document);

std::string getString(const Poco::XML::Node * node, const std::string & path, const std::optional<std::string> & default_value = std::nullopt);

Int64 getInt64(const Poco::XML::Node * node, const std::string & path, const std::optional<Int64> & default_value = std::nullopt);

UInt64 getUInt64(const Poco::XML::Node * node, const std::string & path, const std::optional<UInt64> & default_value = std::nullopt);

int getInt(const Poco::XML::Node * node, const std::string & path, const std::optional<int> & default_value = std::nullopt);

unsigned getUInt(const Poco::XML::Node * node, const std::string & path, const std::optional<unsigned> & default_value = std::nullopt);

bool getBool(const Poco::XML::Node * node, const std::string & path, const std::optional<bool> & default_value = std::nullopt);
}
