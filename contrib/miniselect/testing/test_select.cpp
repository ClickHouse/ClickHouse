/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "test_common.h"

using ::testing::Eq;

namespace miniselect {
namespace {

struct IndirectLess {
  // Non const comparator with deleted copy.
  template <class P>
  bool operator()(const P &x, const P &y) const {
    return *x < *y;
  }
  IndirectLess(const IndirectLess &) = default;
  IndirectLess &operator=(const IndirectLess &) = default;
  IndirectLess(IndirectLess &&) = default;
  IndirectLess &operator=(IndirectLess &&) = default;
};

template <typename Selector>
class SelectTest : public ::testing::Test {
 public:
  using Base = Selector;

  static void TestSelects(size_t N, size_t M) {
    ASSERT_NE(N, 0);
    ASSERT_GT(N, M);
    SCOPED_TRACE(N);
    SCOPED_TRACE(M);
    std::vector<int> array(N);
    for (size_t i = 0; i < N; ++i) {
      array[i] = i;
    }
    auto array_smaller = array;
    std::mt19937_64 mersenne_engine;
    std::shuffle(array.begin(), array.end(), mersenne_engine);
    Selector::Select(array.begin(), array.begin() + M, array.end(),
                     std::greater<int>());
    EXPECT_EQ(array[M], N - M - 1);
    for (size_t i = 0; i < M; ++i) {
      EXPECT_GE(array[i], array[M]);
    }
    for (size_t i = M; i < N; ++i) {
      EXPECT_LE(array[i], array[M]);
    }
    std::shuffle(array_smaller.begin(), array_smaller.end(), mersenne_engine);
    Selector::Select(array_smaller.begin(), array_smaller.begin() + M,
                     array_smaller.end());
    EXPECT_EQ(array_smaller[M], M);
    for (size_t i = 0; i < M; ++i) {
      EXPECT_LE(array_smaller[i], array_smaller[M]);
    }
    for (size_t i = M; i < N; ++i) {
      EXPECT_GE(array_smaller[i], array_smaller[M]);
    }
  }

  static void TestSelects(size_t N) {
    TestSelects(N, 0);
    TestSelects(N, 1);
    TestSelects(N, 2);
    TestSelects(N, 3);
    TestSelects(N, N / 2 - 1);
    TestSelects(N, N / 2);
    TestSelects(N, N / 2 + 1);
    TestSelects(N, N - 2);
    TestSelects(N, N - 1);
  }

  static void TestManySelects() {
    TestSelects(10);
    TestSelects(256);
    TestSelects(257);
    TestSelects(499);
    TestSelects(500);
    TestSelects(997);
    TestSelects(1000);
    TestSelects(1000 * 100);
    TestSelects(1009);
    TestSelects(1009 * 109);
  }

  static void TestCustomComparators() {
    std::vector<std::unique_ptr<int>> v(1000);
    for (int i = 0; static_cast<std::size_t>(i) < v.size(); ++i) {
      v[i] = std::make_unique<int>(i);
    }
    Selector::Select(v.begin(), v.begin() + v.size() / 2, v.end(),
                     IndirectLess{});
    EXPECT_EQ(*v[v.size() / 2], v.size() / 2);
    for (size_t i = 0; i < v.size() / 2; ++i) {
      ASSERT_NE(v[i], nullptr);
      EXPECT_LE(*v[i], v.size() / 2);
    }
    for (size_t i = v.size() / 2; i < v.size(); ++i) {
      ASSERT_NE(v[i], nullptr);
      EXPECT_GE(*v[i], v.size() / 2);
    }
  }

  static void TestRepeat(size_t N, size_t M) {
    ASSERT_NE(N, 0);
    ASSERT_GT(N, M);
    SCOPED_TRACE(N);
    SCOPED_TRACE(M);
    std::mt19937_64 mersenne_engine(10);
    std::vector<bool> array(N);
    for (size_t i = 0; i < M; ++i) {
      array[i] = false;
    }
    for (size_t i = M; i < N; ++i) {
      array[i] = true;
    }
    std::shuffle(array.begin(), array.end(), mersenne_engine);
    Selector::Select(array.begin(), array.begin() + M, array.end());
    EXPECT_EQ(array[M], true);
    for (size_t i = 0; i < M; ++i) {
      EXPECT_EQ(array[i], false);
    }
    for (size_t i = M; i < N; ++i) {
      EXPECT_EQ(array[i], true);
    }
    std::shuffle(array.begin(), array.end(), mersenne_engine);
    Selector::Select(array.begin(), array.begin() + M / 2, array.end());
    EXPECT_EQ(array[M / 2], false);
    for (size_t i = 0; i < M / 2; ++i) {
      EXPECT_EQ(array[i], false);
    }
    std::shuffle(array.begin(), array.end(), mersenne_engine);
    Selector::Select(array.begin(), array.begin() + M - 1, array.end());
    EXPECT_EQ(array[M - 1], false);
    for (size_t i = 0; i < M - 1; ++i) {
      EXPECT_EQ(array[i], false);
    }
  }

  static void TestRepeats(size_t N) {
    TestRepeat(N, 1);
    TestRepeat(N, 2);
    TestRepeat(N, 3);
    TestRepeat(N, N / 2 - 1);
    TestRepeat(N, N / 2);
    TestRepeat(N, N / 2 + 1);
    TestRepeat(N, N - 2);
    TestRepeat(N, N - 1);
  }

  static void TestManyRepeats() {
    TestRepeats(10);
    TestRepeats(100);
    TestRepeats(257);
    TestRepeats(1000);
    TestRepeats(100000);
  }
};

TYPED_TEST_SUITE(SelectTest, algorithms::All);

TYPED_TEST(SelectTest, TestSmall) {
  std::vector<std::string> v = {"ab", "aaa", "ab"};
  TypeParam::Select(v.begin(), v.begin() + 1, v.end());
  EXPECT_THAT(v, Eq(std::vector<std::string>{"aaa", "ab", "ab"}));
  v = {"aba"};
  TypeParam::Select(v.begin(), v.begin(), v.end());
  EXPECT_THAT(v, Eq(std::vector<std::string>{"aba"}));
  v.clear();
  TypeParam::Select(v.begin(), v.end(), v.end());
  EXPECT_TRUE(v.empty());
}

TYPED_TEST(SelectTest, TestAnotherSmall) {
  std::vector<std::string> v = {"ab", "ab", "aaa"};
  TypeParam::Select(v.begin(), v.begin() + 1, v.end());
  EXPECT_THAT(v, Eq(std::vector<std::string>{"aaa", "ab", "ab"}));
}

TYPED_TEST(SelectTest, TestEmptySmall) {
  std::vector<std::string> v = {"", ""};
  TypeParam::Select(v.begin(), v.begin() + 1, v.end());
  EXPECT_THAT(v, Eq(std::vector<std::string>{"", ""}));
}

TYPED_TEST(SelectTest, TestBasic) { TestFixture::TestManySelects(); }

TYPED_TEST(SelectTest, TestComparators) {
  TestFixture::TestCustomComparators();
}

TYPED_TEST(SelectTest, TestRepeats) { TestFixture::TestManyRepeats(); }

TYPED_TEST(SelectTest, TestLast) {
  std::vector<int> array(100);
  for (size_t i = 0; i < 100; ++i) {
    array[i] = i;
  }
  auto array_smaller = array;
  std::mt19937_64 mersenne_engine;
  std::shuffle(array.begin(), array.end(), mersenne_engine);
  auto copy_array = array;
  // Should be no effect.
  size_t cmp = 0;
  TypeParam::Select(array.begin(), array.end(), array.end(),
                    [&cmp](const auto &lhs, const auto &rhs) {
                      ++cmp;
                      return lhs < rhs;
                    });
  EXPECT_EQ(cmp, 0);
  EXPECT_EQ(copy_array, array);
}

}  // namespace
}  // namespace miniselect

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
