#include <string>
#include <Common/XMLUtils.h>
#include <Poco/DOM/Document.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB::XMLUtils
{

using namespace Poco;
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
class ParseHelper : private Util::AbstractConfiguration
{
public:
    ParseHelper() = delete;

    using Util::AbstractConfiguration::parseInt;
    using Util::AbstractConfiguration::parseUInt;
    using Util::AbstractConfiguration::parseInt64;
    using Util::AbstractConfiguration::parseUInt64;
    using Util::AbstractConfiguration::parseBool;

    static std::string parseString(const std::string & s)
    {
        return s;
    }

    template <typename ValueType, typename ParseFunction>
    static ValueType getValue(const Node * node, const std::string & path,
        const std::optional<ValueType> & default_value, const ParseFunction & parse_function)
    {
        const auto * value_node = node->getNodeByPath(path);
        if (!value_node)
        {
            if (default_value)
                return *default_value;
            else
                throw Poco::NotFoundException(path);
        }
        return parse_function(value_node->innerText());
    }
};


std::string getString(const Node * node, const std::string & path, const std::optional<std::string> & default_value)
{
    return ParseHelper::getValue<std::string>(node, path, default_value, ParseHelper::parseString);
}

Int64 getInt64(const Node * node, const std::string & path, const std::optional<Int64> & default_value)
{
    return ParseHelper::getValue<Int64>(node, path, default_value, ParseHelper::parseInt64);
}

UInt64 getUInt64(const Node * node, const std::string & path, const std::optional<UInt64> & default_value)
{
    return ParseHelper::getValue<UInt64>(node, path, default_value, ParseHelper::parseUInt64);
}

int getInt(const Node * node, const std::string & path, const std::optional<int> & default_value)
{
    return ParseHelper::getValue<int>(node, path, default_value, ParseHelper::parseInt);
}

unsigned getUInt(const Node * node, const std::string & path, const std::optional<unsigned> & default_value)
{
    return ParseHelper::getValue<unsigned>(node, path, default_value, ParseHelper::parseUInt);
}

bool getBool(const Node * node, const std::string & path, const std::optional<bool> & default_value)
{
    return ParseHelper::getValue<bool>(node, path, default_value, ParseHelper::parseBool);
}

}
