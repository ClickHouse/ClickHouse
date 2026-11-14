#include <gtest/gtest.h>

#include <Common/DNSResolver.h>
#include <base/scope_guard.h>

using namespace DB;

/// A pooled connection checks its address again only after the counter moved, so every way an entry
/// of a host can change or come back with other addresses must move it.
TEST(DNSResolver, AddressesChangeCounter)
{
    auto & resolver = DNSResolver::instance();
    resolver.dropCache();
    SCOPE_EXIT(resolver.dropCache());

    /// An address literal is resolved without a DNS request.
    const String host = "127.0.0.1";

    /// A new entry, as after an eviction.
    UInt64 counter = resolver.getAddressesChangeCounter();
    resolver.resolveHostAll(host);
    EXPECT_GT(resolver.getAddressesChangeCounter(), counter);

    /// A cached entry.
    counter = resolver.getAddressesChangeCounter();
    resolver.resolveHostAll(host);
    EXPECT_EQ(resolver.getAddressesChangeCounter(), counter);

    /// An update that finds the same addresses.
    resolver.updateCache(0);
    EXPECT_EQ(resolver.getAddressesChangeCounter(), counter);

    /// A removed entry, and the entry added again after it.
    resolver.removeHostFromCache(host);
    EXPECT_GT(resolver.getAddressesChangeCounter(), counter);
    counter = resolver.getAddressesChangeCounter();
    resolver.resolveHostAll(host);
    EXPECT_GT(resolver.getAddressesChangeCounter(), counter);

    /// A dropped cache.
    counter = resolver.getAddressesChangeCounter();
    resolver.dropCache();
    EXPECT_GT(resolver.getAddressesChangeCounter(), counter);
}
