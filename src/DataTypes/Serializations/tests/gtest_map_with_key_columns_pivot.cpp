#include <Columns/ColumnMap.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

ColumnPtr makeMapColumn(const DataTypePtr & type, const std::vector<Map> & rows)
{
    auto column = type->createColumn();
    for (const auto & row : rows)
        column->insert(row);
    return std::move(column);
}

Field firstValueForKey(const Map & row, const Field & key, const Field & default_value)
{
    for (const auto & elem : row)
    {
        const auto & tuple = elem.safeGet<Tuple>();
        if (tuple[0] == key)
            return tuple[1];
    }
    return default_value;
}

void expectPivot(
    const DataTypePtr & type,
    const std::vector<Map> & rows,
    const std::vector<Field> & keys,
    const Field & default_value)
{
    const auto & map_type = assert_cast<const DataTypeMap &>(*type);
    auto column = makeMapColumn(type, rows);
    auto pivoted = SerializationMapWithKeyColumns::pivot(*column, map_type.getKeyType(), map_type.getValueType(), keys);
    ASSERT_EQ(pivoted.size(), keys.size());

    for (size_t key_i = 0; key_i < keys.size(); ++key_i)
    {
        EXPECT_EQ(pivoted[key_i].key, keys[key_i]);
        ASSERT_EQ(pivoted[key_i].values->size(), rows.size());
        ASSERT_EQ(pivoted[key_i].presence.size(), rows.size());
        for (size_t row = 0; row < rows.size(); ++row)
        {
            bool present = false;
            for (const auto & elem : rows[row])
            {
                if (elem.safeGet<Tuple>()[0] == keys[key_i])
                {
                    present = true;
                    break;
                }
            }
            EXPECT_EQ(pivoted[key_i].presence[row], present ? 1 : 0) << "key " << key_i << " row " << row;
            EXPECT_EQ((*pivoted[key_i].values)[row], firstValueForKey(rows[row], keys[key_i], default_value))
                << "key " << key_i << " row " << row;
        }
    }
}

}

TEST(MapWithKeyColumnsPivot, FirstMatchAndMissingRows)
{
    auto type = DataTypeFactory::instance().get("Map(String, UInt64)");
    expectPivot(
        type,
        {
            Map{Tuple{Field("a"), Field(UInt64(1))}, Tuple{Field("a"), Field(UInt64(9))}, Tuple{Field("b"), Field(UInt64(2))}},
            Map{Tuple{Field("b"), Field(UInt64(3))}},
            Map{},
        },
        {Field("a"), Field("b"), Field("missing")},
        Field(UInt64(0)));
}

TEST(MapWithKeyColumnsPivot, NullableString)
{
    auto type = DataTypeFactory::instance().get("Map(String, Nullable(String))");
    expectPivot(
        type,
        {
            Map{Tuple{Field("a"), Field()}, Tuple{Field("b"), Field("x")}},
            Map{Tuple{Field("a"), Field("")}},
        },
        {Field("a"), Field("b")},
        Field());
}

TEST(MapWithKeyColumnsPivot, ArrayString)
{
    auto type = DataTypeFactory::instance().get("Map(String, Array(String))");
    expectPivot(
        type,
        {
            Map{Tuple{Field("a"), Field(Array{Field("x"), Field("y")})}},
            Map{},
        },
        {Field("a")},
        Field(Array{}));
}

TEST(MapWithKeyColumnsPivot, TupleValue)
{
    auto type = DataTypeFactory::instance().get("Map(String, Tuple(UInt8, String))");
    expectPivot(
        type,
        {
            Map{Tuple{Field("a"), Field(Tuple{Field(UInt8(1)), Field("t")})}},
            Map{},
        },
        {Field("a")},
        Field(Tuple{Field(UInt8(0)), Field("")}));
}

TEST(MapWithKeyColumnsPivot, NestedMap)
{
    auto type = DataTypeFactory::instance().get("Map(String, Map(String, UInt8))");
    expectPivot(
        type,
        {
            Map{Tuple{Field("a"), Field(Map{Tuple{Field("inner"), Field(UInt8(7))}})}},
            Map{},
        },
        {Field("a")},
        Field(Map{}));
}
