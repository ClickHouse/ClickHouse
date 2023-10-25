#include "config.h"

#if USE_FDB

#include <gtest/gtest.h>
#include <Common/EventNotifier.h>

namespace
{
// Required by AsyncTrxTracker
struct EventNotifierEnvironment : public ::testing::Environment
{
    void SetUp() override { DB::EventNotifier::init(); }
    void TearDown() override { DB::EventNotifier::shutdown(); }
};

const auto * env = testing::AddGlobalTestEnvironment(new EventNotifierEnvironment());
}
#endif

