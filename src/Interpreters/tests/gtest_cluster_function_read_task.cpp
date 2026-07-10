#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ClusterFunctionReadTask.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

/// Metadata propagation must not depend on a non-empty ETag: an ETag-less backend (e.g. HDFS)
/// still propagates size/time from the coordinator's listing, and the worker must rebuild it.
/// `has_object_metadata` is the explicit wire marker for "the coordinator had metadata".
TEST(ClusterFunctionReadTask, PropagatesEtaglessObjectMetadata)
{
    getContext(); /// `deserialize` needs the global context for the data-lake section.

    ClusterFunctionReadTaskResponse original;
    original.path = "dir/data.parquet";
    original.has_object_metadata = true;
    original.size_bytes = 12345;
    original.is_size_known = true;
    original.last_modified_epoch_us = 0;
    original.is_last_modified_known = false;

    String buf;
    {
        WriteBufferFromString out(buf);
        original.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);
    }

    ClusterFunctionReadTaskResponse received;
    ReadBufferFromString in(buf);
    received.deserialize(in);

    auto object = received.getObjectInfo();
    ASSERT_TRUE(object);
    auto metadata = object->getObjectMetadata();
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->etag.empty());
    EXPECT_EQ(metadata->size_bytes, 12345u);
    EXPECT_TRUE(metadata->is_size_known);
    /// An unknown modification time must survive the round trip (a known epoch would let stale
    /// cache entries validate against it).
    EXPECT_FALSE(metadata->is_last_modified_known);
    EXPECT_TRUE(object->metadata_propagated_from_coordinator);
}

/// The `skip_object_metadata` placeholder (`is_fetched = false`) carries no usable size/time and
/// must not be propagated - the worker fetches its own metadata for such tasks.
TEST(ClusterFunctionReadTask, DoesNotPropagatePlaceholderMetadata)
{
    const auto & context = getContext().context;

    auto object = std::make_shared<ObjectInfo>("dir/data.parquet");
    ObjectMetadata placeholder;
    placeholder.is_fetched = false;
    object->setObjectMetadata(placeholder);

    ClusterFunctionReadTaskResponse response(object, context);
    EXPECT_FALSE(response.has_object_metadata);
}

/// Without coordinator metadata the worker must fetch its own, as before.
TEST(ClusterFunctionReadTask, NoMetadataWhenCoordinatorHadNone)
{
    getContext();

    ClusterFunctionReadTaskResponse original;
    original.path = "dir/data.parquet";

    String buf;
    {
        WriteBufferFromString out(buf);
        original.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);
    }

    ClusterFunctionReadTaskResponse received;
    ReadBufferFromString in(buf);
    received.deserialize(in);

    auto object = received.getObjectInfo();
    ASSERT_TRUE(object);
    EXPECT_FALSE(object->getObjectMetadata().has_value());
    EXPECT_FALSE(object->metadata_propagated_from_coordinator);
}
