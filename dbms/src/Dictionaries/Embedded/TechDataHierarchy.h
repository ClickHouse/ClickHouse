#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Exception.h>

#include <common/Types.h>

#include <ext/singleton.h>


/** @brief Class that lets you know if a search engine or operating system belongs
  * another search engine or operating system, respectively.
  * Information about the hierarchy of regions is downloaded from the database.
  */
class TechDataHierarchy
{
private:
    UInt8 os_parent[256] {};
    UInt8 se_parent[256] {};

public:
    void reload();

    /// Has corresponding section in configuration file.
    static bool isConfigured(const Poco::Util::AbstractConfiguration & config);


    /// The "belongs" relation.
    bool isOSIn(UInt8 lhs, UInt8 rhs) const
    {
        while (lhs != rhs && os_parent[lhs])
            lhs = os_parent[lhs];

        return lhs == rhs;
    }

    bool isSEIn(UInt8 lhs, UInt8 rhs) const
    {
        while (lhs != rhs && se_parent[lhs])
            lhs = se_parent[lhs];

        return lhs == rhs;
    }


    UInt8 OSToParent(UInt8 x) const
    {
        return os_parent[x];
    }

    UInt8 SEToParent(UInt8 x) const
    {
        return se_parent[x];
    }


    /// To the topmost ancestor.
    UInt8 OSToMostAncestor(UInt8 x) const
    {
        while (os_parent[x])
            x = os_parent[x];
        return x;
    }

    UInt8 SEToMostAncestor(UInt8 x) const
    {
        while (se_parent[x])
            x = se_parent[x];
        return x;
    }
};


class TechDataHierarchySingleton : public ext::singleton<TechDataHierarchySingleton>, public TechDataHierarchy {};
