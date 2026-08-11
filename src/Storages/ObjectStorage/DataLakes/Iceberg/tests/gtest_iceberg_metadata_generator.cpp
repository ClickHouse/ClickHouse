#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

using namespace DB;
using namespace DB::Iceberg;

namespace
{

Poco::JSON::Object::Ptr makeMinimalMetadata(Int32 current_schema_id, Int32 last_column_id)
{
    auto metadata = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    metadata->set(f_format_version, 2);
    metadata->set(f_current_schema_id, current_schema_id);
    metadata->set(f_last_column_id, last_column_id);

    auto schemas = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    auto schema = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    schema->set(f_schema_id, current_schema_id);
    schema->set(f_type, "struct");

    auto fields = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    auto field = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    field->set(f_id, 1);
    field->set(f_name, "x");
    field->set(f_required, true);
    field->set(f_type, "int");
    fields->add(field);
    schema->set(f_fields, fields);
    schemas->add(schema);
    metadata->set(f_schemas, schemas);

    return metadata;
}

Poco::JSON::Object::Ptr makeMetadataWithGap()
{
    auto metadata = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    metadata->set(f_format_version, 2);
    metadata->set(f_current_schema_id, 0);
    metadata->set(f_last_column_id, 2);

    auto schemas = Poco::JSON::Array::Ptr(new Poco::JSON::Array);

    auto schema0 = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    schema0->set(f_schema_id, 0);
    schema0->set(f_type, "struct");
    auto fields0 = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    auto field_x = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    field_x->set(f_id, 1);
    field_x->set(f_name, "x");
    field_x->set(f_required, true);
    field_x->set(f_type, "int");
    fields0->add(field_x);
    auto field_y = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    field_y->set(f_id, 2);
    field_y->set(f_name, "y");
    field_y->set(f_required, false);
    field_y->set(f_type, "string");
    fields0->add(field_y);
    schema0->set(f_fields, fields0);
    schemas->add(schema0);

    // Simulate a historical schema with id=5 (higher than current-schema-id=0)
    auto schema5 = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    schema5->set(f_schema_id, 5);
    schema5->set(f_type, "struct");
    auto fields5 = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    fields5->add(field_x);
    schema5->set(f_fields, fields5);
    schemas->add(schema5);

    metadata->set(f_schemas, schemas);
    return metadata;
}

}


TEST(IcebergMetadataGenerator, AddColumnAllocatesSchemaIdAboveMax)
{
    auto metadata = makeMetadataWithGap();
    MetadataGenerator gen(metadata);

    gen.generateAddColumnMetadata("z", makeNullable(std::make_shared<DataTypeInt64>()));

    auto new_schema_id = metadata->getValue<Int32>(f_current_schema_id);
    EXPECT_EQ(new_schema_id, 6);

    auto schemas = metadata->getArray(f_schemas);
    bool found = false;
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(f_schema_id) == 6)
        {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}


TEST(IcebergMetadataGenerator, DropColumnAllocatesSchemaIdAboveMax)
{
    auto metadata = makeMetadataWithGap();
    MetadataGenerator gen(metadata);

    gen.generateDropColumnMetadata("y");

    EXPECT_EQ(metadata->getValue<Int32>(f_current_schema_id), 6);
}


TEST(IcebergMetadataGenerator, DropColumnRejectsIfInSortOrder)
{
    auto metadata = makeMinimalMetadata(0, 1);

    auto sort_orders = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    auto sort_order = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    sort_order->set(f_order_id, static_cast<Int64>(1));
    auto sort_fields = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    auto sf = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    sf->set(f_source_id, 1);
    sf->set("transform", "identity");
    sf->set("direction", "asc");
    sf->set("null-order", "nulls-first");
    sort_fields->add(sf);
    sort_order->set(f_fields, sort_fields);
    sort_orders->add(sort_order);
    metadata->set(f_sort_orders, sort_orders);
    metadata->set(f_default_sort_order_id, static_cast<Int64>(1));

    MetadataGenerator gen(metadata);
    EXPECT_THROW(gen.generateDropColumnMetadata("x"), DB::Exception);
}


TEST(IcebergMetadataGenerator, DropColumnRejectsIfInPartitionSpec)
{
    auto metadata = makeMinimalMetadata(0, 1);

    auto partition_specs = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    auto spec = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    spec->set(f_spec_id, static_cast<Int64>(1));
    auto spec_fields = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    auto pf = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    pf->set(f_source_id, 1);
    pf->set("transform", "identity");
    pf->set("name", "x_part");
    spec_fields->add(pf);
    spec->set(f_fields, spec_fields);
    partition_specs->add(spec);
    metadata->set(f_partition_specs, partition_specs);
    metadata->set(f_default_spec_id, static_cast<Int64>(1));

    MetadataGenerator gen(metadata);
    EXPECT_THROW(gen.generateDropColumnMetadata("x"), DB::Exception);
}


TEST(IcebergMetadataGenerator, ModifyColumnNoopSameType)
{
    auto metadata = makeMinimalMetadata(0, 1);
    MetadataGenerator gen(metadata);

    bool changed = gen.generateModifyColumnMetadata("x", std::make_shared<DataTypeInt32>());
    EXPECT_FALSE(changed);
}


TEST(IcebergMetadataGenerator, ModifyColumnRejectsIndistinguishableType)
{
    auto metadata = makeMinimalMetadata(0, 1);
    MetadataGenerator gen(metadata);

    EXPECT_THROW(gen.generateModifyColumnMetadata("x", std::make_shared<DataTypeUInt32>()), DB::Exception);
}

#endif
