#include <Columns/ColumnString.h>
#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>

#include <Common/Arena.h>
#include "Core/Field.h"
#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnObject, CreateEmpty)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=20, a.b UInt32, a.c Array(String))");
    auto col = type->createColumn();
    const auto & col_object = assert_cast<const ColumnObject &>(*col);
    const auto & typed_paths = col_object.getTypedPaths();
    ASSERT_TRUE(typed_paths.contains("a.b"));
    ASSERT_EQ(typed_paths.at("a.b")->getName(), "UInt32");
    ASSERT_TRUE(typed_paths.contains("a.c"));
    ASSERT_EQ(typed_paths.at("a.c")->getName(), "Array(String)");
    ASSERT_TRUE(col_object.getDynamicPaths().empty());
    ASSERT_TRUE(col_object.getSharedDataOffsets().empty());
    ASSERT_TRUE(col_object.getSharedDataPathsAndValues().first->empty());
    ASSERT_TRUE(col_object.getSharedDataPathsAndValues().second->empty());
    ASSERT_EQ(col_object.getMaxDynamicTypes(), 10);
    ASSERT_EQ(col_object.getMaxDynamicPaths(), 20);
}

TEST(ColumnObject, GetName)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=20, b.d UInt32, a.b Array(String))");
    auto col = type->createColumn();
    ASSERT_EQ(col->getName(), "Object(max_dynamic_paths=20, max_dynamic_types=10, a.b Array(String), b.d UInt32)");
}

Field deserializeFieldFromSharedData(ColumnString * values, size_t n)
{
    auto data = values->getDataAt(n);
    ReadBufferFromMemory buf(data.data, data.size);
    Field res;
    std::make_shared<SerializationDynamic>()->deserializeBinary(res, buf, FormatSettings());
    return res;
}

TEST(ColumnObject, InsertField)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, b.d UInt32, a.b Array(String))");
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    const auto & typed_paths = col_object.getTypedPaths();
    const auto & dynamic_paths = col_object.getDynamicPaths();
    const auto & shared_data_nested_column = col_object.getSharedDataNestedColumn();
    const auto & shared_data_offsets = col_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = col_object.getSharedDataPathsAndValues();
    Object empty_object;
    col_object.insert(empty_object);
    ASSERT_EQ(col_object[0], (Object{{"a.b", Array{}}, {"b.d", Field(0u)}}));
    ASSERT_EQ(typed_paths.at("a.b")->size(), 1);
    ASSERT_TRUE(typed_paths.at("a.b")->isDefaultAt(0));
    ASSERT_EQ(typed_paths.at("b.d")->size(), 1);
    ASSERT_TRUE(typed_paths.at("b.d")->isDefaultAt(0));
    ASSERT_TRUE(dynamic_paths.empty());
    ASSERT_EQ(shared_data_nested_column.size(), 1);
    ASSERT_TRUE(shared_data_nested_column.isDefaultAt(0));

    Object object1 = {{"a.b", Array{String("Hello"), String("World")}}, {"a.c", Field(42)}};
    col_object.insert(object1);
    ASSERT_EQ(col_object[1], (Object{{"a.b", Array{String("Hello"), String("World")}}, {"b.d", Field(0u)}, {"a.c", Field(42)}}));
    ASSERT_EQ(typed_paths.at("a.b")->size(), 2);
    ASSERT_EQ((*typed_paths.at("a.b"))[1], (Array{String("Hello"), String("World")}));
    ASSERT_EQ(typed_paths.at("b.d")->size(), 2);
    ASSERT_TRUE(typed_paths.at("b.d")->isDefaultAt(1));
    ASSERT_EQ(dynamic_paths.size(), 1);
    ASSERT_TRUE(dynamic_paths.contains("a.c"));
    ASSERT_EQ(dynamic_paths.at("a.c")->size(), 2);
    ASSERT_TRUE(dynamic_paths.at("a.c")->isDefaultAt(0));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[1], Field(42));
    ASSERT_EQ(shared_data_nested_column.size(), 2);
    ASSERT_TRUE(shared_data_nested_column.isDefaultAt(1));

    Object object2 = {{"b.d", Field(142u)}, {"a.c", Field(43)}, {"a.d", Field("str")}, {"a.e", Field(242)}, {"a.f", Array{Field(42), Field(43)}}};
    col_object.insert(object2);
    ASSERT_EQ(col_object[2], (Object{{"a.b", Array{}}, {"b.d", Field(142u)}, {"a.c", Field(43)}, {"a.d", Field("str")}, {"a.e", Field(242)}, {"a.f", Array{Field(42), Field(43)}}}));
    ASSERT_EQ(typed_paths.at("a.b")->size(), 3);
    ASSERT_TRUE(typed_paths.at("a.b")->isDefaultAt(2));
    ASSERT_EQ(typed_paths.at("b.d")->size(), 3);
    ASSERT_EQ((*typed_paths.at("b.d"))[2], Field(142u));
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_TRUE(dynamic_paths.contains("a.c"));
    ASSERT_EQ(dynamic_paths.at("a.c")->size(), 3);
    ASSERT_EQ((*dynamic_paths.at("a.c"))[2], Field(43));
    ASSERT_TRUE(dynamic_paths.contains("a.d"));
    ASSERT_EQ(dynamic_paths.at("a.d")->size(), 3);
    ASSERT_EQ((*dynamic_paths.at("a.d"))[2], Field("str"));

    ASSERT_EQ(shared_data_nested_column.size(), 3);
    ASSERT_EQ(shared_data_offsets[2] - shared_data_offsets[1], 2);
    ASSERT_EQ((*shared_data_paths)[0], "a.e");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 0), Field(242));
    ASSERT_EQ((*shared_data_paths)[1], "a.f");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 1), (Array({Field(42), Field(43)})));

    Object object3 = {{"b.a", Field("Str")}, {"b.b", Field(2)}, {"b.c", Field(Tuple{Field(42), Field("Str")})}};
    col_object.insert(object3);
    ASSERT_EQ(col_object[3], (Object{{"a.b", Array{}}, {"b.d", Field(0u)}, {"b.a", Field("Str")}, {"b.b", Field(2)}, {"b.c", Field(Tuple{Field(42), Field("Str")})}}));
    ASSERT_EQ(typed_paths.at("a.b")->size(), 4);
    ASSERT_TRUE(typed_paths.at("a.b")->isDefaultAt(3));
    ASSERT_EQ(typed_paths.at("b.d")->size(), 4);
    ASSERT_TRUE(typed_paths.at("b.d")->isDefaultAt(3));
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_EQ(dynamic_paths.at("a.c")->size(), 4);
    ASSERT_TRUE(dynamic_paths.at("a.c")->isDefaultAt(3));
    ASSERT_EQ(dynamic_paths.at("a.d")->size(), 4);
    ASSERT_TRUE(dynamic_paths.at("a.d")->isDefaultAt(3));

    ASSERT_EQ(shared_data_nested_column.size(), 4);
    ASSERT_EQ(shared_data_offsets[3] - shared_data_offsets[2], 3);
    ASSERT_EQ((*shared_data_paths)[2], "b.a");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 2), Field("Str"));
    ASSERT_EQ((*shared_data_paths)[3], "b.b");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 3), Field(2));
    ASSERT_EQ((*shared_data_paths)[4], "b.c");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 4), Field(Tuple{Field(42), Field("Str")}));

    Object object4 = {{"c.c", Field(Null())}, {"c.d", Field(Null())}};
    col_object.insert(object4);
    ASSERT_TRUE(shared_data_nested_column.isDefaultAt(4));
}

TEST(ColumnObject, InsertFrom)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, b.d UInt32, a.b Array(String))");
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    col_object.insert(Object{{"a.a", Field(42)}});

    const auto & typed_paths = col_object.getTypedPaths();
    const auto & dynamic_paths = col_object.getDynamicPaths();
    const auto & shared_data_nested_column = col_object.getSharedDataNestedColumn();
    const auto & shared_data_offsets = col_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = col_object.getSharedDataPathsAndValues();

    auto src_col1 = type->createColumn();
    auto & src_col_object1 = assert_cast<ColumnObject &>(*src_col1);
    src_col_object1.insert(Object{{"b.d", Field(43u)}, {"a.c", Field("Str1")}});
    col_object.insertFrom(src_col_object1, 0);
    ASSERT_EQ((*typed_paths.at("a.b"))[1], Field(Array{}));
    ASSERT_EQ((*typed_paths.at("b.d"))[1], Field(43u));
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_EQ((*dynamic_paths.at("a.a"))[1], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[1], Field("Str1"));
    ASSERT_TRUE(shared_data_nested_column.isDefaultAt(1));

    auto src_col2 = type->createColumn();
    auto & src_col_object2 = assert_cast<ColumnObject &>(*src_col2);
    src_col_object2.insert(Object{{"a.b", Array{"Str4", "Str5"}}, {"b.d", Field(44u)}, {"a.d", Field("Str2")}, {"a.e", Field("Str3")}});
    col_object.insertFrom(src_col_object2, 0);
    ASSERT_EQ((*typed_paths.at("a.b"))[2], Field(Array{"Str4", "Str5"}));
    ASSERT_EQ((*typed_paths.at("b.d"))[2], Field(44u));
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_EQ((*dynamic_paths.at("a.a"))[2], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[2], Field(Null()));
    ASSERT_EQ(shared_data_offsets[2] - shared_data_offsets[1], 2);
    ASSERT_EQ((*shared_data_paths)[0], "a.d");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 0), Field("Str2"));
    ASSERT_EQ((*shared_data_paths)[1], "a.e");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 1), Field("Str3"));

    auto src_col3 = type->createColumn();
    auto & src_col_object3 = assert_cast<ColumnObject &>(*src_col3);
    src_col_object3.insert(Object{{"a.h", Field("Str6")}, {"h.h", Field("Str7")}});
    src_col_object3.insert(Object{{"a.a", Field("Str10")}, {"a.c", Field(45u)}, {"a.h", Field("Str6")}, {"h.h", Field("Str7")}, {"a.f", Field("Str8")}, {"a.g", Field("Str9")}, {"a.i", Field("Str11")}, {"a.u", Field(Null())}});
    col_object.insertFrom(src_col_object3, 1);
    ASSERT_EQ((*typed_paths.at("a.b"))[3], Field(Array{}));
    ASSERT_EQ((*typed_paths.at("b.d"))[3], Field(0u));
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_EQ((*dynamic_paths.at("a.a"))[3], Field("Str10"));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[3], Field(45u));
    ASSERT_EQ(shared_data_offsets[3] - shared_data_offsets[2], 5);
    ASSERT_EQ((*shared_data_paths)[2], "a.f");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 2), Field("Str8"));
    ASSERT_EQ((*shared_data_paths)[3], "a.g");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 3), Field("Str9"));
    ASSERT_EQ((*shared_data_paths)[4], "a.h");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 4), Field("Str6"));
    ASSERT_EQ((*shared_data_paths)[5], "a.i");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 5), Field("Str11"));
    ASSERT_EQ((*shared_data_paths)[6], "h.h");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 6), Field("Str7"));
}


TEST(ColumnObject, InsertRangeFrom)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, b.d UInt32, a.b Array(String))");
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    col_object.insert(Object{{"a.a", Field(42)}});

    const auto & typed_paths = col_object.getTypedPaths();
    const auto & dynamic_paths = col_object.getDynamicPaths();
    const auto & shared_data_nested_column = col_object.getSharedDataNestedColumn();
    const auto & shared_data_offsets = col_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = col_object.getSharedDataPathsAndValues();

    auto src_col1 = type->createColumn();
    auto & src_col_object1 = assert_cast<ColumnObject &>(*src_col1);
    src_col_object1.insert(Object{{"b.d", Field(43u)}, {"a.c", Field("Str1")}});
    src_col_object1.insert(Object{{"a.b", Field(Array{"Str1", "Str2"})}, {"a.a", Field("Str1")}});
    src_col_object1.insert(Object{{"b.d", Field(45u)}, {"a.c", Field("Str2")}});
    col_object.insertRangeFrom(src_col_object1, 0, 3);
    ASSERT_EQ((*typed_paths.at("a.b"))[1], Field(Array{}));
    ASSERT_EQ((*typed_paths.at("a.b"))[2], Field(Array{"Str1", "Str2"}));
    ASSERT_EQ((*typed_paths.at("a.b"))[3], Field(Array{}));
    ASSERT_EQ((*typed_paths.at("b.d"))[1], Field(43u));
    ASSERT_EQ((*typed_paths.at("b.d"))[2], Field(0u));
    ASSERT_EQ((*typed_paths.at("b.d"))[3], Field(45u));
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_EQ((*dynamic_paths.at("a.a"))[1], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.a"))[2], Field("Str1"));
    ASSERT_EQ((*dynamic_paths.at("a.a"))[3], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[1], Field("Str1"));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[2], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[3], Field("Str2"));
    ASSERT_TRUE(shared_data_nested_column.isDefaultAt(1));
    ASSERT_TRUE(shared_data_nested_column.isDefaultAt(2));
    ASSERT_TRUE(shared_data_nested_column.isDefaultAt(3));

    auto src_col2 = type->createColumn();
    auto & src_col_object2 = assert_cast<ColumnObject &>(*src_col2);
    src_col_object2.insert(Object{{"a.b", Array{"Str4", "Str5"}}, {"a.d", Field("Str2")}, {"a.e", Field("Str3")}});
    src_col_object2.insert(Object{{"b.d", Field(44u)}, {"a.d", Field("Str22")}, {"a.e", Field("Str33")}});
    src_col_object2.insert(Object{{"a.b", Array{"Str44", "Str55"}}, {"a.d", Field("Str222")}, {"a.e", Field("Str333")}});
    col_object.insertRangeFrom(src_col_object2, 0, 3);
    ASSERT_EQ((*typed_paths.at("a.b"))[4], Field(Array{"Str4", "Str5"}));
    ASSERT_EQ((*typed_paths.at("a.b"))[5], Field(Array{}));
    ASSERT_EQ((*typed_paths.at("a.b"))[6], Field(Array{"Str44", "Str55"}));
    ASSERT_EQ((*typed_paths.at("b.d"))[4], Field(0u));
    ASSERT_EQ((*typed_paths.at("b.d"))[5], Field(44u));
    ASSERT_EQ((*typed_paths.at("b.d"))[6], Field(0u));
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_EQ((*dynamic_paths.at("a.a"))[4], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.a"))[5], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.a"))[6], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[4], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[5], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[6], Field(Null()));
    ASSERT_EQ(shared_data_offsets[4] - shared_data_offsets[3], 2);
    ASSERT_EQ((*shared_data_paths)[0], "a.d");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 0), Field("Str2"));
    ASSERT_EQ((*shared_data_paths)[1], "a.e");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 1), Field("Str3"));
    ASSERT_EQ(shared_data_offsets[5] - shared_data_offsets[4], 2);
    ASSERT_EQ((*shared_data_paths)[2], "a.d");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 2), Field("Str22"));
    ASSERT_EQ((*shared_data_paths)[3], "a.e");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 3), Field("Str33"));
    ASSERT_EQ(shared_data_offsets[6] - shared_data_offsets[5], 2);
    ASSERT_EQ((*shared_data_paths)[4], "a.d");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 4), Field("Str222"));
    ASSERT_EQ((*shared_data_paths)[5], "a.e");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 5), Field("Str333"));

    auto src_col3 = type->createColumn();
    auto & src_col_object3 = assert_cast<ColumnObject &>(*src_col3);
    src_col_object3.insert(Object{{"a.h", Field("Str6")}, {"h.h", Field("Str7")}});
    src_col_object3.insert(Object{{"a.h", Field("Str6")}, {"h.h", Field("Str7")}, {"a.f", Field("Str8")}, {"a.g", Field("Str9")}, {"a.i", Field("Str11")}});
    src_col_object3.insert(Object{{"a.a", Field("Str10")}});
    src_col_object3.insert(Object{{"a.h", Field("Str6")}, {"a.c", Field(45u)}, {"h.h", Field("Str7")}, {"a.i", Field("Str11")}});
    col_object.insertRangeFrom(src_col_object3, 1, 3);
    ASSERT_EQ((*typed_paths.at("a.b"))[7], Field(Array{}));
    ASSERT_EQ((*typed_paths.at("a.b"))[8], Field(Array{}));
    ASSERT_EQ((*typed_paths.at("a.b"))[9], Field(Array{}));
    ASSERT_EQ((*typed_paths.at("b.d"))[7], Field(0u));
    ASSERT_EQ((*typed_paths.at("b.d"))[8], Field(0u));
    ASSERT_EQ((*typed_paths.at("b.d"))[9], Field(0u));
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_EQ((*dynamic_paths.at("a.a"))[7], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.a"))[8], Field("Str10"));
    ASSERT_EQ((*dynamic_paths.at("a.a"))[9], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[7], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[8], Field(Null()));
    ASSERT_EQ((*dynamic_paths.at("a.c"))[9], Field(45u));
    ASSERT_EQ(shared_data_offsets[7] - shared_data_offsets[6], 5);
    ASSERT_EQ((*shared_data_paths)[6], "a.f");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 6), Field("Str8"));
    ASSERT_EQ((*shared_data_paths)[7], "a.g");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 7), Field("Str9"));
    ASSERT_EQ((*shared_data_paths)[8], "a.h");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 8), Field("Str6"));
    ASSERT_EQ((*shared_data_paths)[9], "a.i");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 9), Field("Str11"));
    ASSERT_EQ((*shared_data_paths)[10], "h.h");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 10), Field("Str7"));
    ASSERT_EQ(shared_data_offsets[8] - shared_data_offsets[7], 0);
    ASSERT_EQ(shared_data_offsets[9] - shared_data_offsets[8], 3);
    ASSERT_EQ((*shared_data_paths)[11], "a.h");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 11), Field("Str6"));
    ASSERT_EQ((*shared_data_paths)[12], "a.i");
    ASSERT_EQ(deserializeFieldFromSharedData(shared_data_values, 12), Field("Str11"));
}

TEST(ColumnObject, SerializeDeserializerFromArena)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, b.d UInt32, a.b Array(String))");
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    col_object.insert(Object{{"b.d", Field(42u)}, {"a.b", Array{"Str1", "Str2"}}, {"a.a", Tuple{"Str3", 441u}}, {"a.c", Field("Str4")}, {"a.d", Array{Field(45), Field(46)}}, {"a.e", Field(47)}});
    col_object.insert(Object{{"b.a", Field(48)}, {"b.b", Array{Field(49), Field(50)}}});
    col_object.insert(Object{{"b.d", Field(442u)}, {"a.b", Array{"Str11", "Str22"}}, {"a.a", Tuple{"Str33", 444u}}, {"a.c", Field("Str44")}, {"a.d", Array{Field(445), Field(446)}}, {"a.e", Field(447)}});

    Arena arena;
    const char * pos = nullptr;
    auto ref1 = col_object.serializeValueIntoArena(0, arena, pos);
    col_object.serializeValueIntoArena(1, arena, pos);
    col_object.serializeValueIntoArena(2, arena, pos);

    auto col2 = type->createColumn();
    auto & col_object2 = assert_cast<ColumnObject &>(*col);
    pos = col_object2.deserializeAndInsertFromArena(ref1.data);
    pos = col_object2.deserializeAndInsertFromArena(pos);
    col_object2.deserializeAndInsertFromArena(pos);

    ASSERT_EQ(col_object2[0], (Object{{"b.d", Field(42u)}, {"a.b", Array{"Str1", "Str2"}}, {"a.a", Tuple{"Str3", 441u}}, {"a.c", Field("Str4")}, {"a.d", Array{Field(45), Field(46)}}, {"a.e", Field(47)}}));
    ASSERT_EQ(col_object2[1], (Object{{"b.d", Field{0u}}, {"a.b", Array{}}, {"b.a", Field(48)}, {"b.b", Array{Field(49), Field(50)}}}));
    ASSERT_EQ(col_object2[2], (Object{{"b.d", Field(442u)}, {"a.b", Array{"Str11", "Str22"}}, {"a.a", Tuple{"Str33", 444u}}, {"a.c", Field("Str44")}, {"a.d", Array{Field(445), Field(446)}}, {"a.e", Field(447)}}));
}

TEST(ColumnObject, SkipSerializedInArena)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, b.d UInt32, a.b Array(String))");
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    col_object.insert(Object{{"b.d", Field(42u)}, {"a.b", Array{"Str1", "Str2"}}, {"a.a", Tuple{"Str3", 441u}}, {"a.c", Field("Str4")}, {"a.d", Array{Field(45), Field(46)}}, {"a.e", Field(47)}});
    col_object.insert(Object{{"b.a", Field(48)}, {"b.b", Array{Field(49), Field(50)}}});
    col_object.insert(Object{{"b.d", Field(442u)}, {"a.b", Array{"Str11", "Str22"}}, {"a.a", Tuple{"Str33", 444u}}, {"a.c", Field("Str44")}, {"a.d", Array{Field(445), Field(446)}}, {"a.e", Field(447)}});

    Arena arena;
    const char * pos = nullptr;
    auto ref1 = col_object.serializeValueIntoArena(0, arena, pos);
    col_object.serializeValueIntoArena(1, arena, pos);
    auto ref3 = col_object.serializeValueIntoArena(2, arena, pos);

    const char * end = ref3.data + ref3.size;
    auto col2 = type->createColumn();
    pos = col2->skipSerializedInArena(ref1.data);
    pos = col2->skipSerializedInArena(pos);
    pos = col2->skipSerializedInArena(pos);
    ASSERT_EQ(pos, end);
}

TEST(ColumnObject, rollback)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.a UInt32, a.b UInt32)");
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    const auto & typed_paths = col_object.getTypedPaths();
    const auto & dynamic_paths = col_object.getDynamicPaths();
    const auto & shared_data = col_object.getSharedDataColumn();

    auto assert_sizes = [&](size_t size)
    {
        for (const auto & [name, column] : typed_paths)
            ASSERT_EQ(column->size(), size);

        for (const auto & [name, column] : dynamic_paths)
            ASSERT_EQ(column->size(), size);

        ASSERT_EQ(shared_data.size(), size);
    };

    auto checkpoint = col_object.getCheckpoint();

    col_object.insert(Object{{"a.a", Field{1u}}});
    col_object.updateCheckpoint(*checkpoint);

    col_object.insert(Object{{"a.b", Field{2u}}});
    col_object.insert(Object{{"a.a", Field{3u}}});

    col_object.rollback(*checkpoint);

    assert_sizes(1);
    ASSERT_EQ(typed_paths.size(), 2);
    ASSERT_EQ(dynamic_paths.size(), 0);

    ASSERT_EQ((*typed_paths.at("a.a"))[0], Field{1u});
    ASSERT_EQ((*typed_paths.at("a.b"))[0], Field{0u});

    col_object.insert(Object{{"a.c", Field{"ccc"}}});

    checkpoint = col_object.getCheckpoint();

    col_object.insert(Object{{"a.d", Field{"ddd"}}});
    col_object.insert(Object{{"a.e", Field{"eee"}}});

    assert_sizes(4);
    ASSERT_EQ(typed_paths.size(), 2);
    ASSERT_EQ(dynamic_paths.size(), 2);

    ASSERT_EQ((*typed_paths.at("a.a"))[0], Field{1u});
    ASSERT_EQ((*dynamic_paths.at("a.c"))[1], Field{"ccc"});
    ASSERT_EQ((*dynamic_paths.at("a.d"))[2], Field{"ddd"});

    col_object.rollback(*checkpoint);

    assert_sizes(2);
    ASSERT_EQ(typed_paths.size(), 2);
    ASSERT_EQ(dynamic_paths.size(), 1);

    ASSERT_EQ((*typed_paths.at("a.a"))[0], Field{1u});
    ASSERT_EQ((*dynamic_paths.at("a.c"))[1], Field{"ccc"});
}
