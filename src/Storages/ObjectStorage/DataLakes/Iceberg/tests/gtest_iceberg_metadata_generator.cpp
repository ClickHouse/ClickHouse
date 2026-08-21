#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

using namespace DB;
using namespace DB::Iceberg;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

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

/// Everything a schema-changing ALTER is allowed to touch, so a test can assert that a
/// rejected one touched nothing.
struct SchemaState
{
    Int32 current_schema_id;
    size_t schema_count;
};

SchemaState readSchemaState(const Poco::JSON::Object::Ptr & metadata)
{
    return {metadata->getValue<Int32>(f_current_schema_id), metadata->getArray(f_schemas)->size()};
}

void expectSchemaUnchanged(const Poco::JSON::Object::Ptr & metadata, const SchemaState & before)
{
    const auto after = readSchemaState(metadata);
    EXPECT_EQ(after.current_schema_id, before.current_schema_id);
    EXPECT_EQ(after.schema_count, before.schema_count);
}

void expectDropRejected(const Poco::JSON::Object::Ptr & metadata, const String & column)
{
    const auto before = readSchemaState(metadata);
    MetadataGenerator gen(metadata);
    try
    {
        gen.generateDropColumnMetadata(column);
        FAIL() << "DROP COLUMN " << column << " is referenced by the active table metadata and must be rejected";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS) << e.message();
    }
    expectSchemaUnchanged(metadata, before);
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

    expectDropRejected(metadata, "x");
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

    expectDropRejected(metadata, "x");
}


TEST(IcebergMetadataGenerator, ModifyColumnNoopSameType)
{
    auto metadata = makeMinimalMetadata(0, 1);
    MetadataGenerator gen(metadata);

    bool changed = gen.generateModifyColumnMetadata("x", std::make_shared<DataTypeInt32>(), getContext().context);
    EXPECT_FALSE(changed);
}


TEST(IcebergMetadataGenerator, AddColumnAppliedDetectsCommittedColumn)
{
    auto metadata = makeMetadataWithGap();
    MetadataGenerator gen(metadata);

    auto type = makeNullable(std::make_shared<DataTypeInt64>());
    EXPECT_FALSE(gen.isAddColumnApplied("z", type));

    /// Emulate the commit that the catalog applied while reporting a failure.
    gen.generateAddColumnMetadata("z", type);
    EXPECT_TRUE(gen.isAddColumnApplied("z", type));
}


TEST(IcebergMetadataGenerator, AddColumnAppliedDetectsCommittedComplexColumn)
{
    auto metadata = makeMetadataWithGap();
    MetadataGenerator gen(metadata);

    /// `getIcebergType` numbers the nested fields of a complex type from `last-column-id`, which
    /// the applied commit has already advanced. The detection must look past those ids.
    auto type = makeNullable(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()},
        Names{"a", "b"}));
    EXPECT_FALSE(gen.isAddColumnApplied("t", type));

    gen.generateAddColumnMetadata("t", type);
    EXPECT_TRUE(gen.isAddColumnApplied("t", type));
}


TEST(IcebergMetadataGenerator, AddColumnAppliedRejectsComplexTypeMismatch)
{
    auto metadata = makeMetadataWithGap();
    MetadataGenerator gen(metadata);

    auto type = makeNullable(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()},
        Names{"a", "b"}));
    gen.generateAddColumnMetadata("t", type);

    /// Ignoring the nested ids must not make structurally different types compare equal.
    auto renamed_element = makeNullable(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()},
        Names{"a", "c"}));
    EXPECT_FALSE(gen.isAddColumnApplied("t", renamed_element));

    auto retyped_element = makeNullable(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()},
        Names{"a", "b"}));
    EXPECT_FALSE(gen.isAddColumnApplied("t", retyped_element));
}


TEST(IcebergMetadataGenerator, AddColumnAppliedRejectsTypeMismatch)
{
    auto metadata = makeMetadataWithGap();
    MetadataGenerator gen(metadata);

    /// `y` exists as an optional Iceberg `string`, so the same name with another type is not the
    /// column this ALTER asked for and must still be applied.
    EXPECT_TRUE(gen.isAddColumnApplied("y", makeNullable(std::make_shared<DataTypeString>())));
    EXPECT_FALSE(gen.isAddColumnApplied("y", makeNullable(std::make_shared<DataTypeInt64>())));
    EXPECT_FALSE(gen.isAddColumnApplied("y", std::make_shared<DataTypeString>()));
}


TEST(IcebergMetadataGenerator, DropColumnAppliedDetectsCommittedDrop)
{
    auto metadata = makeMetadataWithGap();
    MetadataGenerator gen(metadata);

    EXPECT_FALSE(gen.isDropColumnApplied("y"));

    gen.generateDropColumnMetadata("y");
    EXPECT_TRUE(gen.isDropColumnApplied("y"));
}


TEST(IcebergMetadataGenerator, RenameColumnAppliedDetectsCommittedRename)
{
    auto metadata = makeMetadataWithGap();
    MetadataGenerator gen(metadata);

    EXPECT_FALSE(gen.isRenameColumnApplied("y", "w"));

    gen.generateRenameColumnMetadata("y", "w");
    EXPECT_TRUE(gen.isRenameColumnApplied("y", "w"));
    /// A rename to a different target name is not what this ALTER asked for.
    EXPECT_FALSE(gen.isRenameColumnApplied("y", "v"));
}

/// The tests below describe the stored schema directly instead of producing it with the
/// generator, so that they check what the Iceberg metadata says rather than whether two
/// functions in this class agree with each other.
namespace
{

Poco::JSON::Object::Ptr makeStructType(const String & field_name, const String & field_type, Int32 field_id)
{
    auto type = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    type->set(f_type, "struct");

    auto fields = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    auto field = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    field->set(f_id, field_id);
    field->set(f_name, field_name);
    field->set(f_required, true);
    field->set(f_type, field_type);
    fields->add(field);
    type->set(f_fields, fields);

    return type;
}

/// Metadata whose current schema holds exactly one field with the given Iceberg type.
Poco::JSON::Object::Ptr makeMetadataWithField(
    const String & name, const Poco::Dynamic::Var & iceberg_type, bool required, Int32 last_column_id = 1)
{
    auto metadata = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    metadata->set(f_format_version, 2);
    metadata->set(f_current_schema_id, 0);
    metadata->set(f_last_column_id, last_column_id);

    auto field = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    field->set(f_id, 1);
    field->set(f_name, name);
    field->set(f_required, required);
    field->set(f_type, iceberg_type);

    auto fields = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    fields->add(field);

    auto schema = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    schema->set(f_schema_id, 0);
    schema->set(f_type, "struct");
    schema->set(f_fields, fields);

    auto schemas = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    schemas->add(schema);
    metadata->set(f_schemas, schemas);

    return metadata;
}

/// The Iceberg type recorded for `name` in the schema `current-schema-id` points at.
Poco::Dynamic::Var findCurrentFieldType(const Poco::JSON::Object::Ptr & metadata, const String & name)
{
    auto current_schema_id = metadata->getValue<Int32>(f_current_schema_id);
    auto schemas = metadata->getArray(f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        auto schema = schemas->getObject(i);
        if (schema->getValue<Int32>(f_schema_id) != current_schema_id)
            continue;
        auto fields = schema->getArray(f_fields);
        for (UInt32 j = 0; j < fields->size(); ++j)
        {
            auto field = fields->getObject(j);
            if (field->getValue<String>(f_name) == name)
                return field->get(f_type);
        }
    }
    return {};
}

void expectModifyRejected(
    const Poco::JSON::Object::Ptr & metadata, const String & column, const DataTypePtr & requested_type)
{
    const auto before = readSchemaState(metadata);
    MetadataGenerator gen(metadata);
    try
    {
        gen.generateModifyColumnMetadata(column, requested_type, getContext().context);
        FAIL() << "MODIFY COLUMN " << column << " " << requested_type->getName()
               << " cannot be recorded in Iceberg and must be rejected";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS) << e.message();
    }
    /// The point of rejecting is that the caller must not go on to persist a ClickHouse
    /// schema the Iceberg metadata does not reflect.
    expectSchemaUnchanged(metadata, before);
}

}

TEST(IcebergMetadataGenerator, ModifyColumnAppliedRecognisesTypeAlreadyInSchema)
{
    /// The schema already says `long`, so a MODIFY to Int64 has taken effect.
    auto metadata = makeMetadataWithField("x", "long", /* required */ true);
    EXPECT_TRUE(MetadataGenerator(metadata).isModifyColumnApplied("x", std::make_shared<DataTypeInt64>()));
}

TEST(IcebergMetadataGenerator, ModifyColumnAppliedRejectsTypeNotYetInSchema)
{
    auto metadata = makeMetadataWithField("x", "int", /* required */ true);
    EXPECT_FALSE(MetadataGenerator(metadata).isModifyColumnApplied("x", std::make_shared<DataTypeInt64>()));
}

TEST(IcebergMetadataGenerator, ModifyColumnAppliedDistinguishesNullability)
{
    auto nullable_int = makeNullable(std::make_shared<DataTypeInt32>());

    auto required_field = makeMetadataWithField("x", "int", /* required */ true);
    EXPECT_FALSE(MetadataGenerator(required_field).isModifyColumnApplied("x", nullable_int));

    auto optional_field = makeMetadataWithField("x", "int", /* required */ false);
    EXPECT_TRUE(MetadataGenerator(optional_field).isModifyColumnApplied("x", nullable_int));
}

TEST(IcebergMetadataGenerator, ModifyColumnAppliedRecognisesTypesIcebergCannotDistinguish)
{
    /// The case this predicate exists for: a catalog applied Int32 -> UInt64 but reported
    /// the commit as failed. Iceberg records Int64 and UInt64 alike as `long`, so a retry
    /// must recognise the change as already present instead of reporting a failure.
    auto metadata = makeMetadataWithField("x", "long", /* required */ true);
    EXPECT_TRUE(MetadataGenerator(metadata).isModifyColumnApplied("x", std::make_shared<DataTypeUInt64>()));
}

TEST(IcebergMetadataGenerator, ModifyColumnAppliedFalseForColumnNotInSchema)
{
    auto metadata = makeMetadataWithField("x", "int", /* required */ true);
    EXPECT_FALSE(MetadataGenerator(metadata).isModifyColumnApplied("absent", std::make_shared<DataTypeInt32>()));
}

TEST(IcebergMetadataGenerator, ModifyColumnRejectsIndistinguishablePrimitiveType)
{
    /// Int32 and UInt32 are both Iceberg `int`, so the requested change is unrecordable.
    auto metadata = makeMetadataWithField("x", "int", /* required */ true);
    expectModifyRejected(metadata, "x", std::make_shared<DataTypeUInt32>());
}

TEST(IcebergMetadataGenerator, ModifyColumnRejectsIndistinguishableComplexType)
{
    /// Same for a nested type: Tuple(a Int32) and Tuple(a UInt32) are one Iceberg struct.
    auto metadata = makeMetadataWithField("t", makeStructType("a", "int", 2), /* required */ true, /* last_column_id */ 2);
    auto requested = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt32>()}, Names{"a"});
    expectModifyRejected(metadata, "t", requested);
}

TEST(IcebergMetadataGenerator, ModifyColumnToTypeAlreadyInSchemaAddsNoSchema)
{
    auto metadata = makeMetadataWithField("x", "int", /* required */ true);
    const auto before = readSchemaState(metadata);

    EXPECT_FALSE(MetadataGenerator(metadata).generateModifyColumnMetadata(
        "x", std::make_shared<DataTypeInt32>(), getContext().context));
    expectSchemaUnchanged(metadata, before);
}

TEST(IcebergMetadataGenerator, ModifyColumnWideningRecordsTheNewTypeInANewSchema)
{
    auto metadata = makeMetadataWithField("x", "int", /* required */ true);
    const auto before = readSchemaState(metadata);

    EXPECT_TRUE(MetadataGenerator(metadata).generateModifyColumnMetadata(
        "x", std::make_shared<DataTypeInt64>(), getContext().context));

    const auto after = readSchemaState(metadata);
    EXPECT_EQ(after.schema_count, before.schema_count + 1);
    EXPECT_NE(after.current_schema_id, before.current_schema_id);

    auto stored_type = findCurrentFieldType(metadata, "x");
    ASSERT_TRUE(stored_type.isString());
    EXPECT_EQ(stored_type.extract<String>(), "long");
}

#endif
