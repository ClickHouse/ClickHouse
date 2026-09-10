#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Common/FieldVisitorToString.h>
#include <Common/assert_cast.h>
#include <gtest/gtest.h>

using namespace DB;

GTEST_TEST(NestedUtils, collect)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();
    DataTypePtr array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());

    const NamesAndTypesList source_columns =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b.arr1", array_type},
        {"b.arr2", array_type}
    };

    auto nested_type = createNested({uint_type, uint_type}, {"arr1", "arr2"});
    const NamesAndTypesList columns_with_subcolumns =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b", "arr1", nested_type, array_type},
        {"b", "arr2", nested_type, array_type}
    };

    const NamesAndTypesList columns_with_nested =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b", nested_type},
    };

    ASSERT_EQ(Nested::convertToSubcolumns(source_columns).toString(), columns_with_subcolumns.toString());
    ASSERT_EQ(Nested::collect(source_columns).toString(), columns_with_nested.toString());
}

/// An EMPTY sample block (no rows, so no nulls) is what schema planning uses, e.g.
/// `StorageHive::read`, so the type it yields must equal the one a null-carrying data block yields.
/// The gtest has no global context, so `allow_nullable_tuple_in_extracted_subcolumns` reads as
/// disabled: a TUPLE element stays plain, a deeper SCALAR leaf is `Nullable`.
GTEST_TEST(NestedUtils, extractSubcolumnFromNullableTuplePreservesTypeOnEmptyBlock)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();
    DataTypePtr string_type = std::make_shared<DataTypeString>();

    /// Nullable(Tuple(a Tuple(x UInt32, y String), b String))
    DataTypePtr inner_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{uint_type, string_type}, Strings{"x", "y"});
    DataTypePtr outer_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{inner_tuple, string_type}, Strings{"a", "b"});
    DataTypePtr nullable_tuple = std::make_shared<DataTypeNullable>(outer_tuple);

    /// Empty block (0 rows): the schema-planning / sample-block case.
    Block block;
    block.insert({nullable_tuple->createColumn(), nullable_tuple, "t"});

    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/false);

    /// Extracting the TUPLE element t.a stays plain Tuple(x UInt32, y String) with the setting off,
    /// NOT nullable leaves.
    auto col_a = extractor.extractColumn("t.a");
    ASSERT_TRUE(col_a.has_value());
    ASSERT_EQ(col_a->type->getName(), "Tuple(x UInt32, y String)");

    /// Directly extracting the deeper SCALAR leaf t.a.x must be Nullable(UInt32), not UInt32.
    auto col_ax = extractor.extractColumn("t.a.x");
    ASSERT_TRUE(col_ax.has_value());
    ASSERT_EQ(col_ax->type->getName(), "Nullable(UInt32)");

    /// Sibling leaf t.a.y must be Nullable(String).
    auto col_ay = extractor.extractColumn("t.a.y");
    ASSERT_TRUE(col_ay.has_value());
    ASSERT_EQ(col_ay->type->getName(), "Nullable(String)");

    /// Top-level scalar leaf t.b must be Nullable(String).
    auto col_b = extractor.extractColumn("t.b");
    ASSERT_TRUE(col_b.has_value());
    ASSERT_EQ(col_b->type->getName(), "Nullable(String)");
}

/// Extracting an element of a NULL-carrying parent must give the same column as extracting it from
/// an empty one gives a type: a null map in the data cannot change the shape of the result.
GTEST_TEST(NestedUtils, extractSubcolumnFromNullableTupleWithNullRowKeepsPlannedType)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();
    DataTypePtr string_type = std::make_shared<DataTypeString>();

    DataTypePtr inner_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{uint_type, string_type}, Strings{"x", "y"});
    DataTypePtr outer_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{inner_tuple, string_type}, Strings{"a", "b"});
    DataTypePtr nullable_tuple = std::make_shared<DataTypeNullable>(outer_tuple);

    auto column = nullable_tuple->createColumn();
    column->insert(Tuple{Tuple{UInt64(10), String("aa")}, String("B")});
    column->insert(Tuple{Tuple{UInt64(99), String("zz")}, String("Z")});
    assert_cast<ColumnNullable &>(*column).getNullMapData()[1] = 1;

    Block block;
    block.insert({std::move(column), nullable_tuple, "t"});

    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/false);

    auto col_a = extractor.extractColumn("t.a");
    ASSERT_TRUE(col_a.has_value());
    ASSERT_EQ(col_a->type->getName(), "Tuple(x UInt32, y String)");
    ASSERT_EQ(col_a->column->size(), 2u);
    Field row0;
    Field row1;
    col_a->column->get(0, row0);
    col_a->column->get(1, row1);
    ASSERT_EQ(applyVisitor(FieldVisitorToString(), row0), "(10, 'aa')");
    /// A `Tuple` cannot represent NULL itself, so what the parent-NULL row carries is decided by the
    /// subcolumn path, not by this class: compare it with that path instead of pinning a value.
    Field direct_row1;
    nullable_tuple->getSubcolumn("a", block.getByName("t").column)->get(1, direct_row1);
    ASSERT_EQ(applyVisitor(FieldVisitorToString(), row1), applyVisitor(FieldVisitorToString(), direct_row1));

    /// The parent NULL reaches a scalar leaf as a real NULL.
    auto col_ax = extractor.extractColumn("t.a.x");
    ASSERT_TRUE(col_ax.has_value());
    ASSERT_EQ(col_ax->type->getName(), "Nullable(UInt32)");
    ASSERT_FALSE(col_ax->column->isNullAt(0));
    ASSERT_TRUE(col_ax->column->isNullAt(1));
}

/// An element DECLARED `Nullable(Tuple(...))` is genuinely nullable, so its real NULL rows must
/// survive extraction even with `allow_nullable_tuple_in_extracted_subcolumns` disabled (as it reads
/// here without a global context), unlike a wrapping synthesized from an outer struct null map, which
/// the setting governs.
GTEST_TEST(NestedUtils, extractGenuinelyNullableTupleDescendantStaysNullable)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();

    /// x Tuple(a Nullable(Tuple(b Nullable(UInt32))))
    DataTypePtr b_type = std::make_shared<DataTypeNullable>(uint_type);
    DataTypePtr inner_tuple = std::make_shared<DataTypeTuple>(DataTypes{b_type}, Strings{"b"});
    DataTypePtr nullable_inner = std::make_shared<DataTypeNullable>(inner_tuple);
    DataTypePtr outer_tuple = std::make_shared<DataTypeTuple>(DataTypes{nullable_inner}, Strings{"a"});

    /// Three rows: (( (10) )), a NULL `a`, (( (30) )).
    auto column = outer_tuple->createColumn();
    column->insert(Tuple{Tuple{UInt64(10)}});
    column->insert(Tuple{Null{}});
    column->insert(Tuple{Tuple{UInt64(30)}});

    Block block;
    block.insert({std::move(column), outer_tuple, "x"});

    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/false);

    auto col_a = extractor.extractColumn("x.a");
    ASSERT_TRUE(col_a.has_value());
    /// Genuinely nullable: stays Nullable regardless of the setting.
    ASSERT_EQ(col_a->type->getName(), "Nullable(Tuple(b Nullable(UInt32)))");
    ASSERT_EQ(col_a->column->size(), 3u);
    ASSERT_FALSE(col_a->column->isNullAt(0));
    /// The real NULL row must survive, not collapse to a default tuple.
    ASSERT_TRUE(col_a->column->isNullAt(1));
    ASSERT_FALSE(col_a->column->isNullAt(2));
}

/// Subcolumn names are case-sensitive, so a mixed-case declared element (`A`) requested as `a` is
/// reachable only through the case-insensitive fallback.
GTEST_TEST(NestedUtils, extractGenuinelyNullableTupleDescendantStaysNullableCaseInsensitive)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();

    /// x Tuple(A Nullable(Tuple(b Nullable(UInt32)))): note the mixed-case element name `A`.
    DataTypePtr b_type = std::make_shared<DataTypeNullable>(uint_type);
    DataTypePtr inner_tuple = std::make_shared<DataTypeTuple>(DataTypes{b_type}, Strings{"b"});
    DataTypePtr nullable_inner = std::make_shared<DataTypeNullable>(inner_tuple);
    DataTypePtr outer_tuple = std::make_shared<DataTypeTuple>(DataTypes{nullable_inner}, Strings{"A"});

    auto column = outer_tuple->createColumn();
    column->insert(Tuple{Tuple{UInt64(10)}});
    column->insert(Tuple{Null{}});
    column->insert(Tuple{Tuple{UInt64(30)}});

    Block block;
    block.insert({std::move(column), outer_tuple, "x"});

    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/true);

    /// A spelling that is no element name of its own resolves case-insensitively.
    auto col_a = extractor.extractColumn("x.a");
    ASSERT_TRUE(col_a.has_value());
    ASSERT_EQ(col_a->type->getName(), "Nullable(Tuple(b Nullable(UInt32)))");
    ASSERT_EQ(col_a->column->size(), 3u);
    ASSERT_FALSE(col_a->column->isNullAt(0));
    ASSERT_TRUE(col_a->column->isNullAt(1));
    ASSERT_FALSE(col_a->column->isNullAt(2));
}

/// Mirror of the previous test for the opposite spelling: an upper-cased suffix (`A`) against a
/// declared lowercase `a`.
GTEST_TEST(NestedUtils, extractGenuinelyNullableTupleDescendantStaysNullableCaseInsensitiveRawSpelling)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();

    /// x Tuple(a Nullable(Tuple(b Nullable(UInt32)))): declared element name is lowercase `a`.
    DataTypePtr b_type = std::make_shared<DataTypeNullable>(uint_type);
    DataTypePtr inner_tuple = std::make_shared<DataTypeTuple>(DataTypes{b_type}, Strings{"b"});
    DataTypePtr nullable_inner = std::make_shared<DataTypeNullable>(inner_tuple);
    DataTypePtr outer_tuple = std::make_shared<DataTypeTuple>(DataTypes{nullable_inner}, Strings{"a"});

    auto column = outer_tuple->createColumn();
    column->insert(Tuple{Tuple{UInt64(10)}});
    column->insert(Tuple{Null{}});
    column->insert(Tuple{Tuple{UInt64(30)}});

    Block block;
    block.insert({std::move(column), outer_tuple, "x"});

    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/true);

    auto col_a = extractor.extractColumn("x.A");
    ASSERT_TRUE(col_a.has_value());
    ASSERT_EQ(col_a->type->getName(), "Nullable(Tuple(b Nullable(UInt32)))");
    ASSERT_EQ(col_a->column->size(), 3u);
    ASSERT_FALSE(col_a->column->isNullAt(0));
    ASSERT_TRUE(col_a->column->isNullAt(1));
    ASSERT_FALSE(col_a->column->isNullAt(2));
}

/// An empty Nullable(Tuple()) has no elements, so extracting `t.x` must simply return no column
/// rather than raising. Regression for the `Nullable(Tuple())` + missing-columns Arrow/ORC read
/// reported on PR #109741.
GTEST_TEST(NestedUtils, extractSubcolumnFromEmptyNullableTupleDoesNotThrow)
{
    DataTypePtr empty_tuple = std::make_shared<DataTypeTuple>(DataTypes{});
    DataTypePtr nullable_empty_tuple = std::make_shared<DataTypeNullable>(empty_tuple);

    Block block;
    block.insert({nullable_empty_tuple->createColumn(), nullable_empty_tuple, "t"});

    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/false);

    std::optional<ColumnWithTypeAndName> col_x;
    ASSERT_NO_THROW(col_x = extractor.extractColumn("t.x"));
    ASSERT_FALSE(col_x.has_value());
}

/// A subcolumn entry's type in storage is the type in metadata, while a plain entry carries the type
/// its caller resolved -- for a MergeTree part being read, the part's own possibly older type. The
/// group's element type must come from the plain entry in either order, or a type-directed
/// `enumerateStreams` walk over these columns casts the part's column to the metadata's class.
GTEST_TEST(NestedUtils, convertToSubcolumnsPrefersColumnOverSubcolumn)
{
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    DataTypePtr array_of_string = std::make_shared<DataTypeArray>(string_type);
    DataTypePtr array_of_nullable_string = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(string_type));
    DataTypePtr array_of_uint8 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());

    /// `b.n` is present in the part as Array(String), while metadata says Array(Nullable(String)).
    const NameAndTypePair plain_member{"b.n", array_of_string};
    const NameAndTypePair null_subcolumn{"b.n", "null", array_of_nullable_string, array_of_uint8};

    auto element_type_of = [](const NamesAndTypesList & input)
    {
        for (const auto & name_type : Nested::convertToSubcolumns(input))
        {
            if (name_type.getNameInStorage() == "b" && name_type.getSubcolumnName() == "n")
                return name_type.type;
        }
        return DataTypePtr{};
    };

    ASSERT_EQ(element_type_of({null_subcolumn, plain_member})->getName(), array_of_string->getName());
    ASSERT_EQ(element_type_of({plain_member, null_subcolumn})->getName(), array_of_string->getName());

    /// A member requested only as a subcolumn still contributes and is still remapped onto the Nested
    /// type -- that is what makes the shared offsets serialization apply to it.
    bool remapped_onto_nested = false;
    for (const auto & name_type : Nested::convertToSubcolumns({{"b.i", array_of_uint8}, null_subcolumn}))
    {
        if (name_type.getSubcolumnName() == "n.null")
            remapped_onto_nested = isNested(name_type.getTypeInStorage());
    }
    ASSERT_TRUE(remapped_onto_nested);
}

/// A `LowCardinality(T)` element cannot sit inside `Nullable`, so the parent struct null map has to
/// go into its dictionary as `LowCardinality(Nullable(T))`.
GTEST_TEST(NestedUtils, extractLowCardinalityLeafFromNullableTupleBecomesLowCardinalityNullable)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();
    DataTypePtr lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    /// Nullable(Tuple(lc LowCardinality(String), v UInt32))
    DataTypePtr outer_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{lc_string, uint_type}, Strings{"lc", "v"});
    DataTypePtr nullable_tuple = std::make_shared<DataTypeNullable>(outer_tuple);

    /// Empty block (0 rows): the schema-planning / sample-block case must plan the same type as a
    /// null-carrying block below.
    Block empty_block;
    empty_block.insert({nullable_tuple->createColumn(), nullable_tuple, "t"});
    NestedColumnExtractHelper empty_extractor(empty_block, /*case_insentive_=*/false);

    auto planned_lc = empty_extractor.extractColumn("t.lc");
    ASSERT_TRUE(planned_lc.has_value());
    ASSERT_EQ(planned_lc->type->getName(), "LowCardinality(Nullable(String))");

    /// Data-carrying counterpart: row 1 has the parent struct NULL, and it carries a non-default
    /// payload so a lost null map surfaces as 'zz' or '' rather than NULL.
    auto column = nullable_tuple->createColumn();
    column->insert(Tuple{"a", 1u});
    column->insert(Tuple{"zz", 99u});
    assert_cast<ColumnNullable &>(*column).getNullMapData()[1] = 1;
    column->insert(Tuple{"c", 3u});

    Block block;
    block.insert({std::move(column), nullable_tuple, "t"});
    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/false);

    auto col_lc = extractor.extractColumn("t.lc");
    ASSERT_TRUE(col_lc.has_value());
    ASSERT_EQ(col_lc->type->getName(), "LowCardinality(Nullable(String))");
    ASSERT_EQ(col_lc->column->size(), 3u);
    ASSERT_FALSE(col_lc->column->isNullAt(0));
    ASSERT_TRUE(col_lc->column->isNullAt(1));
    ASSERT_FALSE(col_lc->column->isNullAt(2));
    ASSERT_EQ(std::string(col_lc->column->getDataAt(0)), "a");
    ASSERT_EQ(std::string(col_lc->column->getDataAt(2)), "c");

    /// The sibling scalar leaf keeps the plain Nullable promotion.
    auto col_v = extractor.extractColumn("t.v");
    ASSERT_TRUE(col_v.has_value());
    ASSERT_EQ(col_v->type->getName(), "Nullable(UInt32)");
    ASSERT_TRUE(col_v->column->isNullAt(1));
}

/// Element names only have to be unique case-sensitively, so `A` and `a` can be siblings. Each is
/// its own match; only a spelling that is neither folds onto one of them, in declaration order.
GTEST_TEST(NestedUtils, extractCaseCollidingElementPairsColumnWithItsOwnDeclaredType)
{
    DataTypePtr nullable_uint = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>());
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();

    /// Tuple(A Nullable(Tuple(b Nullable(UInt32))), a Tuple(b UInt32)) -- `A` is genuinely nullable,
    /// its lowercase sibling `a` is not.
    DataTypePtr upper_inner = std::make_shared<DataTypeTuple>(DataTypes{nullable_uint}, Strings{"b"});
    DataTypePtr upper_elem = std::make_shared<DataTypeNullable>(upper_inner);
    DataTypePtr lower_elem = std::make_shared<DataTypeTuple>(DataTypes{uint_type}, Strings{"b"});
    DataTypePtr outer_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{upper_elem, lower_elem}, Strings{"A", "a"});

    auto column = outer_tuple->createColumn();
    column->insert(Tuple{Tuple{10u}, Tuple{20u}});
    column->insert(Tuple{Null{}, Tuple{21u}});
    column->insert(Tuple{Tuple{30u}, Tuple{40u}});

    Block block;
    block.insert({std::move(column), outer_tuple, "x"});

    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/true);

    /// Each declared spelling names its own element, the way `SELECT x.<name>` does, so `a` must
    /// answer with `a`'s non-nullable type even though `A` is listed first.
    auto lower = extractor.extractColumn("x.a");
    ASSERT_TRUE(lower.has_value());
    ASSERT_EQ(lower->type->getName(), "Tuple(b UInt32)");

    auto upper = extractor.extractColumn("x.A");
    ASSERT_TRUE(upper.has_value());
    ASSERT_EQ(upper->type->getName(), "Nullable(Tuple(b Nullable(UInt32)))");
    ASSERT_EQ(upper->column->size(), 3u);
    ASSERT_FALSE(upper->column->isNullAt(0));
    ASSERT_TRUE(upper->column->isNullAt(1));
    ASSERT_FALSE(upper->column->isNullAt(2));
}

GTEST_TEST(NestedUtils, extractPathsOfRootWhoseSubcolumnsCannotBeListed)
{
    DataTypePtr json_type = DataTypeFactory::instance().get("JSON(max_dynamic_paths=8, a UInt32)");
    auto column = json_type->createColumn();
    column->insert(Object{{"a", Field{1u}}, {"b", Field{2u}}});

    Block block;
    block.insert({std::move(column), json_type, "j"});

    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/false);

    auto typed = extractor.extractColumn("j.a");
    ASSERT_TRUE(typed.has_value());
    ASSERT_EQ(typed->name, "j.a");
    ASSERT_EQ(typed->type->getName(), "UInt32");
    ASSERT_EQ(applyVisitor(FieldVisitorToString(), (*typed->column)[0]), "1");

    /// `b` is carried by the row rather than declared, and `c` is neither, yet both are addressable
    /// paths of a JSON column. Both are unreachable through a listing of the root's subcolumns.
    auto dynamic = extractor.extractColumn("j.b");
    ASSERT_TRUE(dynamic.has_value());
    ASSERT_EQ(dynamic->name, "j.b");
    ASSERT_EQ(applyVisitor(FieldVisitorToString(), (*dynamic->column)[0]), "2");

    auto absent = extractor.extractColumn("j.c");
    ASSERT_TRUE(absent.has_value());
    ASSERT_EQ(absent->name, "j.c");
    ASSERT_EQ(absent->column->size(), 1u);
    ASSERT_TRUE(absent->column->isNullAt(0));

    /// Such a path has no declared spelling to map onto, so case-insensitive matching must reach it
    /// as it came.
    NestedColumnExtractHelper case_insensitive_extractor(block, /*case_insentive_=*/true);
    auto folded = case_insensitive_extractor.extractColumn("j.b");
    ASSERT_TRUE(folded.has_value());
    ASSERT_EQ(folded->name, "j.b");
    ASSERT_EQ(applyVisitor(FieldVisitorToString(), (*folded->column)[0]), "2");

    /// `A` is no path of its own here, but every spelling resolves against a JSON root, so the
    /// declared `a` must be matched before the request is resolved as an absent dynamic path.
    auto folded_typed = case_insensitive_extractor.extractColumn("j.A");
    ASSERT_TRUE(folded_typed.has_value());
    ASSERT_EQ(folded_typed->type->getName(), "UInt32");
    ASSERT_EQ(applyVisitor(FieldVisitorToString(), (*folded_typed->column)[0]), "1");
}
