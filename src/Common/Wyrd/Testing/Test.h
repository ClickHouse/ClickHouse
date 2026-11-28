#pragma once

#include <Common/Wyrd/Testing/Run.h>

#include <gtest/gtest.h>

#include <functional>

#if defined(WYRD_ENABLED)

namespace DB::Wyrd
{

bool registerTest(const char * suite, const char * name, const char * file, int line, std::function<void()> body, TestParams params);
bool registerAdversaryTest(const char * suite, const char * name, const char * file, int line, std::function<void()> body, TestParams params);
bool registerSimulationTest(const char * suite, const char * name, const char * file, int line, std::function<void()> body, TestParams params);

}

#define WYRD_TEST_WITH_REGISTRATION(registration, test_suite_name, test_name, ...) \
    static void wyrdTestBody_##test_suite_name##_##test_name(); \
    [[maybe_unused]] static const bool wyrdTestRegistered_##test_suite_name##_##test_name = ::DB::Wyrd::registration( \
        #test_suite_name, #test_name, __FILE__, __LINE__, wyrdTestBody_##test_suite_name##_##test_name, ::DB::Wyrd::TestParams{__VA_ARGS__}); \
    static void wyrdTestBody_##test_suite_name##_##test_name()

#define WYRD_TEST(...) WYRD_TEST_WITH_REGISTRATION(registerTest, __VA_ARGS__)
#define WYRD_ADVERSARY_TEST(...) WYRD_TEST_WITH_REGISTRATION(registerAdversaryTest, __VA_ARGS__)
#define WYRD_SIMULATION_TEST(...) WYRD_TEST_WITH_REGISTRATION(registerSimulationTest, __VA_ARGS__)

#else

#define WYRD_TEST(test_suite_name, test_name, ...) TEST(test_suite_name, test_name)

#define WYRD_ADVERSARY_TEST(test_suite_name, test_name, ...) [[maybe_unused]] static void wyrdTestBody_##test_suite_name##_##test_name()
#define WYRD_SIMULATION_TEST(test_suite_name, test_name, ...) [[maybe_unused]] static void wyrdTestBody_##test_suite_name##_##test_name()

#endif
