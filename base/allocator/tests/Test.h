#pragma once

/// A tiny test framework for the allocator tests. It does not depend on the allocator under test.
///
///     TEST(SizeClasses, Lookup)
///     {
///         CHECK_EQ(jemalloc::sz::sizeToIndex(1), 0u);
///     }
///
/// Every test file is a separate executable; `main` is provided by `TestMain.cpp`.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <type_traits>

namespace allocator_test
{

struct TestCase
{
    const char * suite;
    const char * name;
    void (*function)();
    TestCase * next;
};

inline TestCase * & testList()
{
    static TestCase * list = nullptr;
    return list;
}

inline int & failureCount()
{
    static int count = 0;
    return count;
}

struct Registrar
{
    Registrar(TestCase & test_case)
    {
        /// Append, so that tests run in the order of definition.
        TestCase ** tail = &testList();
        while (*tail)
            tail = &(*tail)->next;
        *tail = &test_case;
    }
};

template <typename T>
void printValue(const T & value)
{
    if constexpr (std::is_same_v<T, bool>)
        std::fprintf(stderr, "%s", value ? "true" : "false");
    else if constexpr (std::is_pointer_v<T> && (std::is_same_v<std::remove_cv_t<std::remove_pointer_t<T>>, char>))
        std::fprintf(stderr, "\"%s\"", value ? value : "(null)");
    else if constexpr (std::is_pointer_v<T> || std::is_null_pointer_v<T>)
        std::fprintf(stderr, "%p", static_cast<const void *>(value));
    else if constexpr (std::is_enum_v<T>)
        std::fprintf(stderr, "%lld", static_cast<long long>(value));
    else if constexpr (std::is_floating_point_v<T>)
        std::fprintf(stderr, "%.17g", static_cast<double>(value));
    else if constexpr (std::is_signed_v<T>)
        std::fprintf(stderr, "%lld", static_cast<long long>(value));
    else if constexpr (std::is_unsigned_v<T>)
        std::fprintf(stderr, "%llu (0x%llx)", static_cast<unsigned long long>(value), static_cast<unsigned long long>(value));
    else
        std::fprintf(stderr, "<value>");
}

[[noreturn]] inline void abortTest()
{
    std::fflush(stderr);
    std::abort();
}

}

#define ALLOCATOR_TEST_CONCAT_IMPL(a, b) a##b
#define ALLOCATOR_TEST_CONCAT(a, b) ALLOCATOR_TEST_CONCAT_IMPL(a, b)

#define TEST(suite, name) \
    static void ALLOCATOR_TEST_CONCAT(test_##suite##_, name)(); \
    static ::allocator_test::TestCase ALLOCATOR_TEST_CONCAT(test_case_##suite##_, name) \
        = {#suite, #name, &ALLOCATOR_TEST_CONCAT(test_##suite##_, name), nullptr}; \
    static ::allocator_test::Registrar ALLOCATOR_TEST_CONCAT(test_registrar_##suite##_, name)( \
        ALLOCATOR_TEST_CONCAT(test_case_##suite##_, name)); \
    static void ALLOCATOR_TEST_CONCAT(test_##suite##_, name)()

/// Non-fatal check: reports and continues.
#define CHECK(cond) \
    do \
    { \
        if (!(cond)) \
        { \
            std::fprintf(stderr, "%s:%d: CHECK(%s) failed\n", __FILE__, __LINE__, #cond); \
            ++::allocator_test::failureCount(); \
        } \
    } while (false)

#define CHECK_OP(a, b, op) \
    do \
    { \
        const auto & check_a_ = (a); \
        const auto & check_b_ = (b); \
        if (!(check_a_ op check_b_)) \
        { \
            std::fprintf(stderr, "%s:%d: CHECK(%s %s %s) failed: ", __FILE__, __LINE__, #a, #op, #b); \
            ::allocator_test::printValue(check_a_); \
            std::fprintf(stderr, " vs "); \
            ::allocator_test::printValue(check_b_); \
            std::fprintf(stderr, "\n"); \
            ++::allocator_test::failureCount(); \
        } \
    } while (false)

#define CHECK_EQ(a, b) CHECK_OP(a, b, ==)
#define CHECK_NE(a, b) CHECK_OP(a, b, !=)
#define CHECK_LT(a, b) CHECK_OP(a, b, <)
#define CHECK_LE(a, b) CHECK_OP(a, b, <=)
#define CHECK_GT(a, b) CHECK_OP(a, b, >)
#define CHECK_GE(a, b) CHECK_OP(a, b, >=)

#define CHECK_STREQ(a, b) \
    do \
    { \
        const char * check_a_ = (a); \
        const char * check_b_ = (b); \
        if (!check_a_ || !check_b_ || std::strcmp(check_a_, check_b_) != 0) \
        { \
            std::fprintf(stderr, "%s:%d: CHECK_STREQ(%s, %s) failed:\n  \"%s\"\n  \"%s\"\n", __FILE__, __LINE__, #a, #b, \
                check_a_ ? check_a_ : "(null)", check_b_ ? check_b_ : "(null)"); \
            ++::allocator_test::failureCount(); \
        } \
    } while (false)

/// Fatal check: aborts the whole test executable.
#define REQUIRE(cond) \
    do \
    { \
        if (!(cond)) \
        { \
            std::fprintf(stderr, "%s:%d: REQUIRE(%s) failed\n", __FILE__, __LINE__, #cond); \
            ::allocator_test::abortTest(); \
        } \
    } while (false)
