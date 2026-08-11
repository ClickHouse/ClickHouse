#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Databases/DataLake/RestCatalog.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Common/Exception.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <base/types.h>

using namespace DB;

namespace
{
Poco::JSON::Object::Ptr findUpdateByAction(const Poco::JSON::Array::Ptr & updates, const std::string & action)
{
    for (unsigned int i = 0; i < updates->size(); ++i)
    {
        auto o = updates->getObject(i);
        if (o->getValue<std::string>("action") == action)
            return o;
    }
    return nullptr;
}
}

TEST(RestCatalogUpdateMetadataBody, NullSnapshotReturnsNull)
{
    auto body = DataLake::buildUpdateMetadataRequestBody("ns", "t", nullptr);
    EXPECT_FALSE(body);
}

TEST(RestCatalogUpdateMetadataBody, SnapshotUpdateWithParent)
{
    Poco::JSON::Object::Ptr snapshot = new Poco::JSON::Object;
    snapshot->set("snapshot-id", static_cast<Int64>(12345));
    snapshot->set("parent-snapshot-id", static_cast<Int64>(12344));
    snapshot->set(Iceberg::f_timestamp_ms, static_cast<Int64>(1700000000000LL));

    auto body = DataLake::buildUpdateMetadataRequestBody("ns", "t", snapshot);
    ASSERT_TRUE(body);

    ASSERT_TRUE(body->has("requirements"));
    auto req = body->getArray("requirements")->getObject(0);
    EXPECT_EQ(req->getValue<std::string>("type"), "assert-ref-snapshot-id");
    EXPECT_EQ(req->getValue<std::string>("ref"), "main");
    EXPECT_EQ(req->getValue<Int64>("snapshot-id"), 12344);

    auto updates = body->getArray("updates");
    auto add_snap = findUpdateByAction(updates, "add-snapshot");
    ASSERT_TRUE(add_snap);
    EXPECT_EQ(add_snap->getObject("snapshot")->getValue<Int64>("snapshot-id"), 12345);

    auto set_ref = findUpdateByAction(updates, "set-snapshot-ref");
    ASSERT_TRUE(set_ref);
    EXPECT_EQ(set_ref->getValue<Int64>("snapshot-id"), 12345);
}

TEST(RestCatalogUpdateMetadataBody, SnapshotUpdateWithoutParent)
{
    Poco::JSON::Object::Ptr snapshot = new Poco::JSON::Object;
    snapshot->set("snapshot-id", static_cast<Int64>(999));

    auto body = DataLake::buildUpdateMetadataRequestBody("ns", "t", snapshot);
    ASSERT_TRUE(body);
    EXPECT_FALSE(body->has("requirements"));

    auto updates = body->getArray("updates");
    ASSERT_TRUE(findUpdateByAction(updates, "add-snapshot"));
    ASSERT_TRUE(findUpdateByAction(updates, "set-snapshot-ref"));
}

TEST(RestCatalogUpdateMetadataBody, SnapshotUpdateParentMinusOneNoRequirement)
{
    Poco::JSON::Object::Ptr snapshot = new Poco::JSON::Object;
    snapshot->set("snapshot-id", static_cast<Int64>(1));
    snapshot->set("parent-snapshot-id", static_cast<Int64>(-1));

    auto body = DataLake::buildUpdateMetadataRequestBody("ns", "t", snapshot);
    ASSERT_TRUE(body);
    EXPECT_FALSE(body->has("requirements"));
}

TEST(RestCatalogUpdateSchemaBody, EquivalentSchemaDeduplicates)
{
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object;

    Poco::JSON::Array::Ptr schemas = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr schema0 = new Poco::JSON::Object;
    schema0->set(Iceberg::f_schema_id, 0);
    schema0->set(Iceberg::f_type, "struct");
    Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr field1 = new Poco::JSON::Object;
    field1->set(Iceberg::f_id, 1);
    field1->set(Iceberg::f_name, "a");
    field1->set(Iceberg::f_required, false);
    field1->set(Iceberg::f_type, "int");
    fields->add(field1);
    schema0->set(Iceberg::f_fields, fields);
    schemas->add(schema0);

    Poco::JSON::Object::Ptr schema1 = new Poco::JSON::Object;
    schema1->set(Iceberg::f_schema_id, 1);
    schema1->set(Iceberg::f_type, "struct");
    Poco::JSON::Array::Ptr fields1 = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr field1b = new Poco::JSON::Object;
    field1b->set(Iceberg::f_id, 1);
    field1b->set(Iceberg::f_name, "a");
    field1b->set(Iceberg::f_required, false);
    field1b->set(Iceberg::f_type, "int");
    Poco::JSON::Object::Ptr field2b = new Poco::JSON::Object;
    field2b->set(Iceberg::f_id, 2);
    field2b->set(Iceberg::f_name, "b");
    field2b->set(Iceberg::f_required, false);
    field2b->set(Iceberg::f_type, "int");
    fields1->add(field1b);
    fields1->add(field2b);
    schema1->set(Iceberg::f_fields, fields1);
    schemas->add(schema1);
    metadata->set(Iceberg::f_schemas, schemas);

    Poco::JSON::Object::Ptr new_schema = new Poco::JSON::Object;
    new_schema->set(Iceberg::f_schema_id, 2);
    new_schema->set(Iceberg::f_type, "struct");
    Poco::JSON::Array::Ptr new_fields = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr nf1 = new Poco::JSON::Object;
    nf1->set(Iceberg::f_id, 1);
    nf1->set(Iceberg::f_name, "a");
    nf1->set(Iceberg::f_required, false);
    nf1->set(Iceberg::f_type, "int");
    new_fields->add(nf1);
    new_schema->set(Iceberg::f_fields, new_fields);

    auto body = DataLake::buildUpdateSchemaRequestBody("ns", "t", metadata, new_schema, 1, 2);
    ASSERT_TRUE(body);

    auto updates = body->getArray("updates");
    EXPECT_FALSE(findUpdateByAction(updates, "add-schema"));

    auto set_schema = findUpdateByAction(updates, "set-current-schema");
    ASSERT_TRUE(set_schema);
    EXPECT_EQ(set_schema->getValue<Int32>("schema-id"), 0);
}

TEST(RestCatalogUpdateSchemaBody, NormalPathEmitsAddSchema)
{
    Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object;

    Poco::JSON::Array::Ptr schemas = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr schema0 = new Poco::JSON::Object;
    schema0->set(Iceberg::f_schema_id, 0);
    schema0->set(Iceberg::f_type, "struct");
    Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr field1 = new Poco::JSON::Object;
    field1->set(Iceberg::f_id, 1);
    field1->set(Iceberg::f_name, "a");
    field1->set(Iceberg::f_required, false);
    field1->set(Iceberg::f_type, "int");
    fields->add(field1);
    schema0->set(Iceberg::f_fields, fields);
    schemas->add(schema0);
    metadata->set(Iceberg::f_schemas, schemas);

    Poco::JSON::Object::Ptr new_schema = new Poco::JSON::Object;
    new_schema->set(Iceberg::f_schema_id, 1);
    new_schema->set(Iceberg::f_type, "struct");
    Poco::JSON::Array::Ptr new_fields = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr nf1 = new Poco::JSON::Object;
    nf1->set(Iceberg::f_id, 1);
    nf1->set(Iceberg::f_name, "a");
    nf1->set(Iceberg::f_required, false);
    nf1->set(Iceberg::f_type, "int");
    Poco::JSON::Object::Ptr nf2 = new Poco::JSON::Object;
    nf2->set(Iceberg::f_id, 2);
    nf2->set(Iceberg::f_name, "b");
    nf2->set(Iceberg::f_required, false);
    nf2->set(Iceberg::f_type, "string");
    new_fields->add(nf1);
    new_fields->add(nf2);
    new_schema->set(Iceberg::f_fields, new_fields);

    auto body = DataLake::buildUpdateSchemaRequestBody("ns", "t", metadata, new_schema, 0, 5);
    ASSERT_TRUE(body);

    auto updates = body->getArray("updates");

    auto add_schema = findUpdateByAction(updates, "add-schema");
    ASSERT_TRUE(add_schema);
    EXPECT_EQ(add_schema->getValue<Int32>("last-column-id"), 5);
    EXPECT_TRUE(add_schema->has("schema"));
    auto schema_obj = add_schema->getObject("schema");
    EXPECT_TRUE(schema_obj->has("identifier-field-ids"));

    auto set_schema = findUpdateByAction(updates, "set-current-schema");
    ASSERT_TRUE(set_schema);
    EXPECT_EQ(set_schema->getValue<Int32>("schema-id"), -1);

    ASSERT_TRUE(body->has("requirements"));
    auto req = body->getArray("requirements")->getObject(0);
    EXPECT_EQ(req->getValue<std::string>("type"), "assert-current-schema-id");
    EXPECT_EQ(req->getValue<Int32>("current-schema-id"), 0);
}

#endif
