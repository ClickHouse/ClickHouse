#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
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
/// The gtest has no global context, so `allow_nullable_tuple_in_extracted_subcolumns` reads as its
/// default off: a TUPLE element stays plain, a deeper SCALAR leaf is `Nullable`.
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

    /// Extracting the TUPLE element t.a stays plain Tuple(x UInt32, y String) with the setting off
    /// (default-row semantics on parent NULL), NOT nullable leaves.
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

/// Data-carrying counterpart of the previous test: the parent-NULL row of an extracted plain Tuple
/// must hold type defaults, not whatever the child columns happen to carry under a NULL.
GTEST_TEST(NestedUtils, extractTupleElementFromNullableTupleWithNullRowGivesDefaults)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();
    DataTypePtr string_type = std::make_shared<DataTypeString>();

    DataTypePtr inner_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{uint_type, string_type}, Strings{"x", "y"});
    DataTypePtr outer_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{inner_tuple, string_type}, Strings{"a", "b"});
    DataTypePtr nullable_tuple = std::make_shared<DataTypeNullable>(outer_tuple);

    /// Two rows: (( (10,'aa'), 'B' )) and a NULL row whose nested payload is NON-default
    /// ((99,'zz'),'Z'). ColumnNullable does not guarantee the payload under a NULL row is the type
    /// default, so if the terminal unwrap merely dropped the null map (getNestedColumnPtr) it would
    /// surface (99,'zz') instead of (0,''). Inserting a real value BEFORE flagging the row null
    /// reproduces the Arrow/ORC/Hive case: without the default-materializing unwrap this row0/row1
    /// assertion fails.
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
    /// Parent-NULL row must be the tuple default, not garbage.
    ASSERT_EQ(applyVisitor(FieldVisitorToString(), row1), "(0, '')");
}

/// An element DECLARED `Nullable(Tuple(...))` is genuinely nullable, so its real NULL rows must
/// survive extraction even with `allow_nullable_tuple_in_extracted_subcolumns` off (its default
/// here), unlike a synthetic wrapping from an outer struct null map, which the setting governs.
/// The declared subcolumn type of the root, not the null map contents, tells the two apart.
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

/// Readers lowercase the requested name before it reaches the extractor, while the declared-type
/// lookup matches case-sensitively, so a mixed-case declared element (`A`) requested as `a` must
/// still be found, or the genuinely nullable descendant is wrongly treated as synthetic.
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

    /// Requested lowercased, as the reader passes it.
    auto col_a = extractor.extractColumn("x.a");
    ASSERT_TRUE(col_a.has_value());
    ASSERT_EQ(col_a->type->getName(), "Nullable(Tuple(b Nullable(UInt32)))");
    ASSERT_EQ(col_a->column->size(), 3u);
    ASSERT_FALSE(col_a->column->isNullAt(0));
    ASSERT_TRUE(col_a->column->isNullAt(1));
    ASSERT_FALSE(col_a->column->isNullAt(2));
}

/// Mirror of the previous test for the opposite spelling: `StorageHive::read` does NOT lowercase the
/// request, so a non-lowercased suffix (`A`) reaches the lookup against a declared lowercase `a`.
/// The fallback must fold BOTH sides, not just the declared name.
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

    /// Requested with the original (non-lowercased) spelling, as StorageHive passes it.
    auto col_a = extractor.extractColumn("x.A");
    ASSERT_TRUE(col_a.has_value());
    ASSERT_EQ(col_a->type->getName(), "Nullable(Tuple(b Nullable(UInt32)))");
    ASSERT_EQ(col_a->column->size(), 3u);
    ASSERT_FALSE(col_a->column->isNullAt(0));
    ASSERT_TRUE(col_a->column->isNullAt(1));
    ASSERT_FALSE(col_a->column->isNullAt(2));
}

/// An empty Nullable(Tuple()) has no elements to descend into. ColumnTuple::create rejects an empty
/// column list, so unwrapping such a parent must not construct one; extracting a missing subcolumn
/// like `t.x` must simply return no column instead of raising a LOGICAL_ERROR. Regression for the
/// `Nullable(Tuple())` + missing-columns Arrow/ORC read reported on PR #109741.
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
/// go into its dictionary as `LowCardinality(Nullable(T))`. That is the same policy the direct
/// subcolumn path applies via `makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe`, and
/// `applyParentNullMapToExtractedSubcolumn` accepts exactly that representation.
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

/// Two element names differing only in case are legal (`checkTupleNames` compares case-sensitively),
/// so under case-insensitive extraction the requested name matches both. The column is taken with
/// `Block::findByName(..., case_insentive)`, which returns the first such element, so the declared
/// type has to be resolved in the same order: pairing one element's column with the other element's
/// nullability turns a genuine NULL into a non-NULL tuple of NULLs.
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

    /// The readers lowercase the requested spelling, so `x.a` is what arrives here.
    NestedColumnExtractHelper extractor(block, /*case_insentive_=*/true);
    auto col = extractor.extractColumn("x.a");
    ASSERT_TRUE(col.has_value());

    /// `A` wins the case-insensitive lookup, so the extracted type must be `A`'s nullable one and
    /// its real NULL row must stay NULL, rather than being unwrapped using `a`'s non-nullable type.
    ASSERT_EQ(col->type->getName(), "Nullable(Tuple(b Nullable(UInt32)))");
    ASSERT_EQ(col->column->size(), 3u);
    ASSERT_FALSE(col->column->isNullAt(0));
    ASSERT_TRUE(col->column->isNullAt(1));
    ASSERT_FALSE(col->column->isNullAt(2));
}
