#pragma once

#include <base/types.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
    /// Returns true if two configurations contains the same keys and values.
    bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left,
                             const Poco::Util::AbstractConfiguration & right);

    /// Config may have multiple keys with one name. For example:
    /// <root>
    ///     <some_key>...</some_key>
    ///     <some_key>...</some_key>
    /// </root>
    /// Returns true if the specified subview of the two configurations contains
    /// the same keys and values for each key with the given name.
    bool isSameConfigurationWithMultipleKeys(const Poco::Util::AbstractConfiguration & left,
                                             const Poco::Util::AbstractConfiguration & right,
                                             const String & root, const String & name);

    /// Returns true if the specified subview of the two configurations contains the same keys and values.
    bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left,
                             const Poco::Util::AbstractConfiguration & right,
                             const String & key);

    /// Returns true if specified subviews of the two configurations contains the same keys and values.
    bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left, const String & left_key,
                             const Poco::Util::AbstractConfiguration & right, const String & right_key);

    inline bool operator==(const Poco::Util::AbstractConfiguration & left, const Poco::Util::AbstractConfiguration & right)
    {
        return isSameConfiguration(left, right);
    }

    inline bool operator!=(const Poco::Util::AbstractConfiguration & left, const Poco::Util::AbstractConfiguration & right)
    {
        return !isSameConfiguration(left, right);
    }
}
