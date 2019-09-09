#pragma once

#include <Core/Types.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
    /// Returns true if two configurations contains the same keys and values.
    bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left,
                             const Poco::Util::AbstractConfiguration & right);

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
