#include <Common/FieldVisitorsAccurateComparison.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Columns/ColumnObject.h>
#include <Common/FieldVisitorToString.h>

#include <Common/randomSeed.h>
#include <fmt/core.h>
#include <pcg_random.hpp>
#include <gtest/gtest.h>
#include <random>

using namespace DB;

static pcg64 rng(randomSeed());

Field getRandomField(size_t type)
{
    switch (type)
    {
        case 0:
            return rng();
        case 1:
            return std::uniform_real_distribution<>(0.0, 1.0)(rng);
        case 2:
            return std::string(rng() % 10, 'a' + rng() % 26);
        default:
            return Field();
    }
}

std::pair<ColumnObject::Subcolumn, std::vector<Field>> generate(size_t size)
{
    bool has_defaults = rng() % 3 == 0;
    size_t num_defaults = has_defaults ? rng() % size : 0;

    ColumnObject::Subcolumn subcolumn(num_defaults, false);
    std::vector<Field> fields;

    while (subcolumn.size() < size)
    {
        size_t part_size = rng() % (size - subcolumn.size()) + 1;
        size_t field_type = rng() % 3;

        for (size_t i = 0; i < part_size; ++i)
        {
            fields.push_back(getRandomField(field_type));
            subcolumn.insert(fields.back());
        }
    }

    std::vector<Field> result_fields;
    for (size_t i = 0; i < num_defaults; ++i)
        result_fields.emplace_back();

    result_fields.insert(result_fields.end(), fields.begin(), fields.end());
    return {std::move(subcolumn), std::move(result_fields)};
}

void checkFieldsAreEqual(ColumnObject::Subcolumn subcolumn, const std::vector<Field> & fields)
{
    ASSERT_EQ(subcolumn.size(), fields.size());
    for (size_t i = 0; i < subcolumn.size(); ++i)
    {
        Field field;
        subcolumn.get(i, field); // Also check 'get' method.
        if (!applyVisitor(FieldVisitorAccurateEquals(), field, fields[i]))
        {
            std::cerr << fmt::format("Wrong value at position {}, expected {}, got {}",
                i, applyVisitor(FieldVisitorToString(), fields[i]), applyVisitor(FieldVisitorToString(), field));
            ASSERT_TRUE(false);
        }
    }
}

constexpr size_t T = 1000;
constexpr size_t N = 1000;

TEST(ColumnObject, InsertRangeFrom)
{
    for (size_t t = 0; t < T; ++t)
    {
        auto [subcolumn_dst, fields_dst] = generate(N);
        auto [subcolumn_src, fields_src] = generate(N);

        ASSERT_EQ(subcolumn_dst.size(), fields_dst.size());
        ASSERT_EQ(subcolumn_src.size(), fields_src.size());

        const auto & type_dst = subcolumn_dst.getLeastCommonType();
        const auto & type_src = subcolumn_src.getLeastCommonType();
        auto type_res = getLeastSupertype(DataTypes{type_dst, type_src}, true);

        size_t from = rng() % subcolumn_src.size();
        size_t to = rng() % subcolumn_src.size();
        if (from > to)
            std::swap(from, to);
        ++to;

        for (auto & field : fields_dst)
        {
            if (field.isNull())
                field = type_res->getDefault();
            else
                field = convertFieldToTypeOrThrow(field, *type_res);
        }

        for (size_t i = from; i < to; ++i)
        {
            if (fields_src[i].isNull())
                fields_dst.push_back(type_res->getDefault());
            else
                fields_dst.push_back(convertFieldToTypeOrThrow(fields_src[i], *type_res));

        }

        subcolumn_dst.insertRangeFrom(subcolumn_src, from, to - from);
        checkFieldsAreEqual(subcolumn_dst, fields_dst);
    }
}
