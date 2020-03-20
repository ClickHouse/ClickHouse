#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>

#include <Common/WeakHash.h>

#include <unordered_map>

using namespace DB;

template <typename T>
void checkColumn(const WeakHash32::Container & hash, const PaddedPODArray<T> & eq_class, size_t allowed_collisions = 0)
{
    /// Check equal rows has equal hash.
    {
        std::unordered_map<T, UInt32> map;

        for (size_t i = 0; i < eq_class.size(); ++i)
        {
            auto & val = eq_class[i];
            auto it = map.find(val);

            if (it == map.end())
                map[val] = hash[i];
            else
                ASSERT_EQ(it->second, hash[i]);
        }
    }

    /// Check have not many collisions.
    {
        std::unordered_map<UInt32, T> map;
        size_t num_collisions = 0;

        for (size_t i = 0; i < eq_class.size(); ++i)
        {
            auto & val = eq_class[i];
            auto it = map.find(hash[i]);

            if (it == map.end())
                map[hash[i]] = val;
            else if (it->second != val)
                ++num_collisions;
        }

        ASSERT_LE(num_collisions, allowed_collisions);
    }
}

TEST(WeakHash32, ColumnVectorU8)
{
    auto col = ColumnUInt8::create();
    auto & data = col->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (size_t i = 0; i < 265; ++i)
            data.push_back(i);
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), col->getData());
}

TEST(WeakHash32, ColumnVectorI8)
{
    auto col = ColumnInt8::create();
    auto & data = col->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (int i = -128; i < 128; ++i)
            data.push_back(i);
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), col->getData());
}

TEST(WeakHash32, ColumnVectorU16)
{
    auto col = ColumnUInt16::create();
    auto & data = col->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (size_t i = 0; i < 65536; ++i)
            data.push_back(i);
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), col->getData());
}

TEST(WeakHash32, ColumnVectorI16)
{
    auto col = ColumnInt16::create();
    auto & data = col->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (int i = 32768; i < 32768; ++i)
            data.push_back(i);
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), col->getData());
}

TEST(WeakHash32, ColumnVectorU32)
{
    auto col = ColumnUInt32::create();
    auto & data = col->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (uint64_t i = 0; i < 65536; ++i)
            data.push_back(i << 16u);
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), col->getData());
}

TEST(WeakHash32, ColumnVectorI32)
{
    auto col = ColumnInt32::create();
    auto & data = col->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (int64_t i = -32768; i < 32768; ++i)
            data.push_back(i << 16);
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), col->getData());
}

TEST(WeakHash32, ColumnVectorU64)
{
    auto col = ColumnUInt64::create();
    auto & data = col->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (uint64_t i = 0; i < 65536; ++i)
            data.push_back(i << 32u);
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), col->getData());
}

TEST(WeakHash32, ColumnVectorI64)
{
    auto col = ColumnInt64::create();
    auto & data = col->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (int64_t i = -32768; i < 32768; ++i)
            data.push_back(i << 32);
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), col->getData());
}

TEST(WeakHash32, ColumnString_1)
{
    auto col = ColumnString::create();
    auto eq = ColumnUInt32::create();
    auto & data = eq->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        for (int64_t i = 0; i < 65536; ++i)
        {
            data.push_back(i);
            auto str = std::to_string(i);
            col->insertData(str.data(), str.size());
        }
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), data);
}

TEST(WeakHash32, ColumnString_2)
{
    auto col = ColumnString::create();
    auto eq = ColumnUInt32::create();
    auto & data = eq->getData();

    /*
     * a
     * aa
     * aaa
     * ...
     * b
     * bb
     * bbb
     */
    for (int _i [[maybe_unused]] : {1, 2})
    {
        size_t max_size = 3000;
        char letter = 'a';
        for (int64_t i = 0; i < 65536; ++i)
        {
            data.push_back(i);
            size_t s = (i % max_size) + 1;
            std::string str(s, letter);
            col->insertData(str.data(), str.size());

            if (s == max_size)
                ++letter;
        }
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), data);
}

TEST(WeakHash32, ColumnString_3)
{
    auto col = ColumnString::create();
    auto eq = ColumnUInt32::create();
    auto & data = eq->getData();

    /*
     * a
     * a\0
     * a\0\0
     * ...
     * b
     * b\0
     * b\0\0
    */
    for (int _i [[maybe_unused]] : {1, 2})
    {
        size_t max_size = 3000;
        char letter = 'a';
        for (int64_t i = 0; i < 65536; ++i)
        {
            data.push_back(i);
            size_t s = (i % max_size) + 1;
            std::string str(s,'\0');
            str[0] = letter;
            col->insertData(str.data(), str.size());

            if (s == max_size)
                ++letter;
        }
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), data);
}

TEST(WeakHash32, ColumnFixedString)
{
    size_t max_size = 3000;
    auto col = ColumnFixedString::create(max_size);
    auto eq = ColumnUInt32::create();
    auto & data = eq->getData();

    for (int _i [[maybe_unused]] : {1, 2})
    {
        char letter = 'a';
        for (int64_t i = 0; i < 65536; ++i)
        {
            data.push_back(i);
            size_t s = (i % max_size) + 1;
            std::string str(letter, s);
            col->insertData(str.data(), str.size());

            if (s == max_size)
                ++letter;
        }
    }

    WeakHash32 hash(col->size());
    col->updateWeakHash32(hash);

    checkColumn(hash.getData(), data);
}

TEST(WeakHash32, ColumnArray)
{
    size_t max_size = 3000;
    auto val = ColumnUInt32::create();
    auto off = ColumnUInt64::create();
    auto eq = ColumnUInt32::create();
    auto & eq_data = eq->getData();
    auto & val_data = val->getData();
    auto & off_data = off->getData();

    /* [1]
     * [1, 1]
     * [1, 1, 1]
     * ...
     * [2]
     * [2, 2]
     * [2, 2, 2]
     * ...
     */
    for (int _i [[maybe_unused]] : {1, 2})
    {
        UInt32 cur = 0;
        UInt64 cur_off = 0;
        for (int64_t i = 0; i < 65536; ++i)
        {
            eq_data.push_back(i);
            size_t s = (i % max_size) + 1;

            cur_off += s;
            off_data.push_back(cur_off);

            for (size_t j = 0; j < s; ++j)
                val_data.push_back(cur);

            if (s == max_size)
                ++cur;
        }
    }

    auto col_arr = ColumnArray::create(std::move(val), std::move(off));

    WeakHash32 hash(col_arr->size());
    col_arr->updateWeakHash32(hash);

    checkColumn(hash.getData(), eq_data);
}

TEST(WeakHash32, ColumnArrayArray)
{
    size_t max_size = 3000;
    auto val = ColumnUInt32::create();
    auto off = ColumnUInt64::create();
    auto off2 = ColumnUInt64::create();
    auto eq = ColumnUInt32::create();
    auto & eq_data = eq->getData();
    auto & val_data = val->getData();
    auto & off_data = off->getData();
    auto & off2_data = off->getData();

    /* [[0]]
     * [[0], [0]]
     * [[0], [0], [0]]
     * ...
     * [[0, 0]]
     * [[0, 0], [0, 0]]
     * [[0, 0], [0, 0], [0, 0]]
     * ...
     */
    for (int _i [[maybe_unused]] : {1, 2})
    {
        UInt32 cur = 1;
        UInt64 cur_off = 0;
        UInt64 cur_off2 = 0;
        for (int64_t i = 0; i < 65536; ++i)
        {
            eq_data.push_back(i);
            size_t s = (i % max_size) + 1;

            cur_off2 += s;
            off2_data.push_back(cur_off2);

            for (size_t j = 0; j < s; ++j)
            {
                for (size_t k = 0; k < cur; ++k)
                    val_data.push_back(0);

                cur_off += cur;
                off_data.push_back(cur_off);
            }

            if (s == max_size)
                ++cur;
        }
    }

    auto col_arr = ColumnArray::create(std::move(val), std::move(off));
    auto col_arr_arr = ColumnArray::create(std::move(col_arr), std::move(off2));

    WeakHash32 hash(col_arr_arr->size());
    col_arr->updateWeakHash32(hash);

    checkColumn(hash.getData(), eq_data);
}
