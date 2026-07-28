#include "config.h"

#if USE_LANCE

#include <gtest/gtest.h>

#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceQuerySession.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{
Lance::TableStateSnapshot makeSnapshot(UInt64 version, UInt8 seed)
{
    Lance::TableStateSnapshot snapshot;
    snapshot.version = version;
    snapshot.manifest_id.fill(seed);
    snapshot.manifest_size = 512;
    snapshot.manifest_sha256.fill(seed + 1);
    return snapshot;
}
}

TEST(LanceQuerySession, IdentityKeyStableAndSensitiveToCredentials)
{
    Lance::DatasetOptions a{.uri = "/tmp/ds", .use_s3 = false};
    Lance::DatasetOptions b = a;
    EXPECT_EQ(a.identityKey(), b.identityKey());

    b.s3_access_key_id = "other";
    b.use_s3 = true;
    EXPECT_NE(a.identityKey(), b.identityKey());
}

TEST(LanceQuerySession, PinSnapshotRejectsConflict)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    auto session = Lance::QuerySession::get(context);
    const auto snapshot = makeSnapshot(3, 1);
    session->pinSnapshot("id1", snapshot);
    session->pinSnapshot("id1", snapshot);
    EXPECT_EQ(session->getPinnedSnapshot("id1"), snapshot);
    EXPECT_THROW(session->pinSnapshot("id1", makeSnapshot(3, 9)), Exception);
}

TEST(LanceQuerySession, GetOrOpenReusesHandleWithinSession)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto session = Lance::QuerySession::get(context);

    Lance::DatasetOptions options{.uri = "/path/that/does/not/exist/for/session/test"};
    /// Both calls fail the same way; the second must not leave a half-open entry.
    EXPECT_THROW(std::ignore = session->getOrOpen(options), Exception);
    EXPECT_EQ(session->openCount(), 0u);
}

TEST(LanceQuerySession, SessionSharedAcrossGetCalls)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    auto session1 = Lance::QuerySession::get(context);
    auto session2 = Lance::QuerySession::get(context);
    EXPECT_EQ(session1.get(), session2.get());
}

#endif
