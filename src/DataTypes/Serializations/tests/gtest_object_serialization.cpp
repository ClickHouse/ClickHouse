#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <IO/ReadBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(ObjectSerialization, FieldBinarySerialization)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32, a.c Array(String))");
    auto serialization = type->getDefaultSerialization();
    Object object1 = Object{{"a.c", Array{"Str1", "Str2"}}, {"a.d", Field(42)}, {"a.e", Tuple{Field(43), "Str3"}}};
    WriteBufferFromOwnString ostr;
    serialization->serializeBinary(object1, ostr, FormatSettings());
    ReadBufferFromString istr(ostr.str());
    Field object2;
    serialization->deserializeBinary(object2, istr, FormatSettings());
    ASSERT_EQ(object1, object2.safeGet<Object>());
}


TEST(ObjectSerialization, ColumnBinarySerialization)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32, a.c Array(String))");
    auto serialization = type->getDefaultSerialization();
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    col_object.insert(Object{{"a.c", Array{"Str1", "Str2"}}, {"a.d", Field(42)}, {"a.e", Tuple{Field(43), "Str3"}}});
    WriteBufferFromOwnString ostr1;
    serialization->serializeBinary(col_object, 0, ostr1, FormatSettings());
    ReadBufferFromString istr1(ostr1.str());
    serialization->deserializeBinary(col_object, istr1, FormatSettings());
    ASSERT_EQ(col_object[0], col_object[1]);
    col_object.insert(Object{{"a.c", Array{"Str1", "Str2"}}, {"a.e", Field(42)}, {"b.d", Field(42)}, {"b.e", Tuple{Field(43), "Str3"}}, {"b.g", Field("Str4")}});
    WriteBufferFromOwnString ostr2;
    serialization->serializeBinary(col_object, 2, ostr2, FormatSettings());
    ReadBufferFromString istr2(ostr2.str());
    serialization->deserializeBinary(col_object, istr2, FormatSettings());
    ASSERT_EQ(col_object[2], col_object[3]);
}

TEST(ObjectSerialization, JSONSerialization)
{
    auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32, a.c Array(String))");
    auto serialization = type->getDefaultSerialization();
    auto col = type->createColumn();
    auto & col_object = assert_cast<ColumnObject &>(*col);
    col_object.insert(Object{{"a.c", Array{"Str1", "Str2"}}, {"a.d", Field(42)}, {"a.e", Tuple{Field(43), "Str3"}}});
    col_object.insert(Object{{"a.c", Array{"Str1", "Str2"}}, {"a", Tuple{Field(43), "Str3"}}, {"a.b.c", Field(42)}, {"a.b.e", Field(43)}, {"b.c.d.e", Field(42)}, {"b.c.d.g", Field(43)}, {"b.c.h.r", Field(44)}, {"c.g.h.t", Array{Field("Str"), Field("Str2")}}, {"h", Field("Str")}, {"j", Field("Str")}});
    WriteBufferFromOwnString buf1;
    serialization->serializeTextJSON(col_object, 1, buf1, FormatSettings());
    ASSERT_EQ(buf1.str(), R"({"a":[43,"Str3"],"a":{"b":0,"b":{"c":42,"e":43},"c":["Str1","Str2"]},"b":{"c":{"d":{"e":42,"g":43},"h":{"r":44}}},"c":{"g":{"h":{"t":["Str","Str2"]}}},"h":"Str","j":"Str"})");
    WriteBufferFromOwnString buf2;
    serialization->serializeTextJSONPretty(col_object, 1, buf2, FormatSettings(), 0);
    ASSERT_EQ(buf2.str(), R"({
    "a" : [
        43,
        "Str3"
    ],
    "a" : {
        "b" : 0,
        "b" : {
            "c" : 42,
            "e" : 43
        },
        "c" : [
            "Str1",
            "Str2"
        ]
    },
    "b" : {
        "c" : {
            "d" : {
                "e" : 42,
                "g" : 43
            },
            "h" : {
                "r" : 44
            }
        }
    },
    "c" : {
        "g" : {
            "h" : {
                "t" : [
                    "Str",
                    "Str2"
                ]
            }
        }
    },
    "h" : "Str",
    "j" : "Str"
})");

}
