#pragma once

#include <Common/Wyrd/Namespace.h>
#include <Common/Wyrd/Testing/Run.h>

#include <gtest/gtest.h>

#include <functional>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

bool registerTest(const char * suite, const char * name, const char * file, int line, std::function<void()> body, TestParams params);

}

#define WYRD_TEST(test_suite_name, test_name, ...) \
    static void wyrdTestBody_##test_suite_name##_##test_name(); \
    [[maybe_unused]] static const bool wyrdTestRegistered_##test_suite_name##_##test_name = ::DB::Wyrd::registerTest( \
        #test_suite_name, #test_name, __FILE__, __LINE__, wyrdTestBody_##test_suite_name##_##test_name, ::DB::Wyrd::TestParams{__VA_ARGS__}); \
    static void wyrdTestBody_##test_suite_name##_##test_name()
