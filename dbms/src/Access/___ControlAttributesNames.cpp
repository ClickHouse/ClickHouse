#if 0
#pragma once


namespace DB
{
namespace ACLAttributesNamespaces
{
    /// Users and roles share the same namespace.
    constexpr int USER = 0;
    constexpr int ROLE = 0;

    constexpr int QUOTA = 1;
    constexpr int ROW_FILTER_POLICY = 2;

    constexpr int MAX = 3;
};
}
#endif
