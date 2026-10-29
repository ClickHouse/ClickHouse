#include <Common/XMLUtils.h>
#include <Poco/DOM/Document.h>

namespace DB::XMLUtils
{

using namespace Poco::XML;

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

}
