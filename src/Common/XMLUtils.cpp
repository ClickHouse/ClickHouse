#include <string>
#include <Common/XMLUtils.h>
#include <Poco/DOM/Document.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB::XMLUtils
{

using namespace Poco::XML;

using XMLDocumentPtr = Poco::AutoPtr<Document>;

Node * getRootNode(Document * document)
{
    for (Node * child = document->firstChild(); child; child = child->nextSibling())
    {
        /// Besides the root element there can be comment nodes on the top level.
        /// Skip them.
        if (child->nodeType() == Node::ELEMENT_NODE)
            return child;
    }

    throw Poco::Exception("No root node in document");
}


/// This class is used to access protected parseXXX static methods from AbstractConfiguration
class ParseHelper : private Poco::Util::AbstractConfiguration
{
public:
    ParseHelper() = delete;

    using Poco::Util::AbstractConfiguration::parseInt;
    using Poco::Util::AbstractConfiguration::parseUInt;
    using Poco::Util::AbstractConfiguration::parseInt64;
    using Poco::Util::AbstractConfiguration::parseUInt64;
    using Poco::Util::AbstractConfiguration::parseBool;

    static std::string parseString(const std::string & s)
    {
        return s;
    }

    template <typename ValueType, bool ReturnDefault, typename ParseFunction>
    static ValueType getValue(const Node * node, const std::string & path,
        const ValueType & default_value, const ParseFunction & parse_function)
    {
        const auto * value_node = node->getNodeByPath(path);
        if (!value_node)
        {
            if constexpr (ReturnDefault)
                return default_value;
            else
                throw Poco::NotFoundException(path);
        }
        return parse_function(value_node->innerText());
    }
};


std::string getString(const Node * node, const std::string & path)
{
    return ParseHelper::getValue<std::string, false>(node, path, {}, ParseHelper::parseString);
}

std::string getString(const Node * node, const std::string & path, const std::string & default_value)
{
    return ParseHelper::getValue<std::string, true>(node, path, default_value, ParseHelper::parseString);
}

Int64 getInt64(const Node * node, const std::string & path)
{
    return ParseHelper::getValue<Int64, false>(node, path, {}, ParseHelper::parseInt64);
}

Int64 getInt64(const Node * node, const std::string & path, Int64 default_value)
{
    return ParseHelper::getValue<Int64, true>(node, path, default_value, ParseHelper::parseInt64);
}

UInt64 getUInt64(const Node * node, const std::string & path)
{
    return ParseHelper::getValue<UInt64, false>(node, path, {}, ParseHelper::parseUInt64);
}

UInt64 getUInt64(const Node * node, const std::string & path, UInt64 default_value)
{
    return ParseHelper::getValue<UInt64, true>(node, path, default_value, ParseHelper::parseUInt64);
}

int getInt(const Node * node, const std::string & path)
{
    return ParseHelper::getValue<int, false>(node, path, {}, ParseHelper::parseInt);
}

int getInt(const Node * node, const std::string & path, int default_value)
{
    return ParseHelper::getValue<int, true>(node, path, default_value, ParseHelper::parseInt);
}

unsigned getUInt(const Node * node, const std::string & path)
{
    return ParseHelper::getValue<unsigned, false>(node, path, {}, ParseHelper::parseUInt);
}

unsigned getUInt(const Node * node, const std::string & path, unsigned default_value)
{
    return ParseHelper::getValue<unsigned, true>(node, path, default_value, ParseHelper::parseUInt);
}

bool getBool(const Node * node, const std::string & path)
{
    return ParseHelper::getValue<bool, false>(node, path, {}, ParseHelper::parseBool);
}

bool getBool(const Node * node, const std::string & path, bool default_value)
{
    return ParseHelper::getValue<bool, true>(node, path, default_value, ParseHelper::parseBool);
}

}
