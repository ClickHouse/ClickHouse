/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
#pragma once

#include <algorithm>
#include <random>
#include <vector>

namespace miniselect {
namespace datagens {

struct Random {
  static std::vector<uint32_t> Gen(size_t size) {
    std::random_device rnd_device;
    std::mt19937_64 mersenne_engine{rnd_device()};
    std::vector<uint32_t> v;
    v.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      v.push_back(i);
    }
    std::shuffle(v.begin(), v.end(), mersenne_engine);
    return v;
  }
};

struct Shuffled16 {
  static std::vector<uint32_t> Gen(size_t size) {
    std::random_device rnd_device;
    std::mt19937_64 mersenne_engine{rnd_device()};
    std::vector<uint32_t> v;
    v.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      v.push_back(i % 16);
    }
    std::shuffle(v.begin(), v.end(), mersenne_engine);
    return v;
  }
};

struct Random01 {
  static std::vector<uint32_t> Gen(size_t size) {
    std::random_device rnd_device;
    std::mt19937_64 mersenne_engine{rnd_device()};
    std::vector<uint32_t> v;
    v.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      v.push_back(i % 2);
    }
    std::shuffle(v.begin(), v.end(), mersenne_engine);
    return v;
  }
};

struct Ascending {
  static std::vector<uint32_t> Gen(size_t size) {
    std::vector<uint32_t> v;
    v.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      v.push_back(i);
    }
    return v;
  }
};

struct Descending {
  static std::vector<uint32_t> Gen(size_t size) {
    std::vector<uint32_t> v;
    v.reserve(size);
    for (int i = size - 1; i >= 0; --i) {
      v.push_back(i);
    }
    return v;
  }
};

struct PipeOrgan {
  static std::vector<uint32_t> Gen(size_t size) {
    std::vector<uint32_t> v;
    v.reserve(size);
    for (size_t i = 0; i < size / 2; ++i) {
      v.push_back(i);
    }
    for (size_t i = size / 2; i < size; ++i) {
      v.push_back(size - i);
    }
    return v;
  }
};

struct PushFront {
  static std::vector<uint32_t> Gen(size_t size) {
    std::vector<uint32_t> v;
    v.reserve(size);
    for (size_t i = 1; i < size; ++i) {
      v.push_back(i);
    }
    v.push_back(0);
    return v;
  }
};

struct PushMiddle {
  static std::vector<uint32_t> Gen(size_t size) {
    std::vector<uint32_t> v;
    v.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      if (i != size / 2) {
        v.push_back(i);
      }
    }
    v.push_back(size / 2);
    return v;
  }
};

struct Median3Killer {
  static std::vector<uint32_t> Gen(size_t size) {
    size_t k = size / 2;
    std::vector<uint32_t> v;
    v.reserve(size);
    for (size_t i = 1; i < k + 1; ++i) {
      if (i & 1) {
        v.push_back(i);
      } else {
        v.push_back(k + i - 1);
      }
    }
    for (size_t i = 1; i < k + 1; ++i) {
      v.push_back(2 * i);
    }
    return v;
  }
};

#define BENCH_IMPL(BENCH, GEN, IMPL)   \
  BENCHMARK_TEMPLATE(BENCH, GEN, IMPL) \
      ->Unit(benchmark::kMicrosecond)  \
      ->Arg(kSize - 10)                \
      ->Arg(kSize / 2)                 \
      ->Arg(10000)                     \
      ->Arg(1000)                      \
      ->Arg(100)                       \
      ->Arg(10)                        \
      ->Arg(1)

#define BENCH_GENS(BENCH, IMPL)                  \
  BENCH_IMPL(BENCH, datagens::Random, IMPL);     \
  BENCH_IMPL(BENCH, datagens::Shuffled16, IMPL); \
  BENCH_IMPL(BENCH, datagens::Random01, IMPL);   \
  BENCH_IMPL(BENCH, datagens::Ascending, IMPL);  \
  BENCH_IMPL(BENCH, datagens::Descending, IMPL); \
  BENCH_IMPL(BENCH, datagens::PipeOrgan, IMPL);  \
  BENCH_IMPL(BENCH, datagens::PushMiddle, IMPL); \
  BENCH_IMPL(BENCH, datagens::PushFront, IMPL);  \
  BENCH_IMPL(BENCH, datagens::Median3Killer, IMPL)

#define BENCH(NAME)                               \
  BENCH_GENS(NAME, algorithms::FloydRivest);      \
  BENCH_GENS(NAME, algorithms::MedianOfNinthers); \
  BENCH_GENS(NAME, algorithms::MedianOfMedians);  \
  BENCH_GENS(NAME, algorithms::MedianOf3Random);  \
  BENCH_GENS(NAME, algorithms::PDQ);              \
  BENCH_GENS(NAME, algorithms::PDQBranchless);    \
  BENCH_GENS(NAME, algorithms::STD)

}  // namespace datagens
}  // namespace miniselect
