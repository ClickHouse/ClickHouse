#pragma once

#include <Common/Wyrd/Testing/Run.h>

#include <gtest/gtest.h>

#define WYRD_TEST(test_suite_name, test_name, ...) \
    static void wyrdTestBody_##test_suite_name##_##test_name(); \
    TEST(test_suite_name, test_name) \
    { \
        ::DB::Wyrd::runTest(wyrdTestBody_##test_suite_name##_##test_name, ::DB::Wyrd::TestParams{__VA_ARGS__}); \
    } \
    static void wyrdTestBody_##test_suite_name##_##test_name()
