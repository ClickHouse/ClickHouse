#include <math.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_PIPELINE;
}


/** It's safe to access children without mutex as long as these methods are called before first call to read, readPrefix.
  */


String IBlockInputStream::getTreeID() const
{
    std::stringstream s;
    s << getName();

    if (!children.empty())
    {
        s << "(";
        for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
        {
            if (it != children.begin())
                s << ", ";
            s << (*it)->getTreeID();
        }
        s << ")";
    }

    return s.str();
}


size_t IBlockInputStream::checkDepth(size_t max_depth) const
{
    return checkDepthImpl(max_depth, max_depth);
}

size_t IBlockInputStream::checkDepthImpl(size_t max_depth, size_t level) const
{
    if (children.empty())
        return 0;

    if (level > max_depth)
        throw Exception("Query pipeline is too deep. Maximum: " + toString(max_depth), ErrorCodes::TOO_DEEP_PIPELINE);

    size_t res = 0;
    for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        size_t child_depth = (*it)->checkDepth(level + 1);
        if (child_depth > res)
            res = child_depth;
    }

    return res + 1;
}


void IBlockInputStream::dumpTree(std::ostream & ostr, size_t indent, size_t multiplier) const
{
    ostr << String(indent, ' ') << getName();
    if (multiplier > 1)
        ostr << " × " << multiplier;
    //ostr << ": " << getHeader().dumpStructure();
    ostr << std::endl;
    ++indent;

    /// If the subtree is repeated several times, then we output it once with the multiplier.
    using Multipliers = std::map<String, size_t>;
    Multipliers multipliers;

    for (const auto & child : children)
        ++multipliers[child->getTreeID()];

    for (const auto & child : children)
    {
        String id = child->getTreeID();
        size_t & subtree_multiplier = multipliers[id];
        if (subtree_multiplier != 0)    /// Already printed subtrees are marked with zero in the array of multipliers.
        {
            child->dumpTree(ostr, indent, subtree_multiplier);
            subtree_multiplier = 0;
        }
    }
}

}

