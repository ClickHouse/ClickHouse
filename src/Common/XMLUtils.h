#pragma once

#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Node.h>
#include <Poco/AutoPtr.h>

namespace DB:: XMLUtils
{
/// Returns root element of the document.
Poco::XML::Node * getRootNode(Poco::XML::Document * document);
}
