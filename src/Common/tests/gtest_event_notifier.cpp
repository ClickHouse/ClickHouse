#include <gtest/gtest.h>

#include <Common/EventNotifier.h>
#include <Common/ZooKeeper/IKeeper.h>


TEST(EventNotifier, SimpleTest)
{
    using namespace DB;

    size_t result = 1;

    auto handler3 = EventNotifier::instance().subscribe(Coordination::Error::ZSESSIONEXPIRED, [&result](){ result *= 3; });

    {
      auto handler5 = EventNotifier::instance().subscribe(Coordination::Error::ZSESSIONEXPIRED, [&result](){ result *= 5; });
    }

    auto handler7 = EventNotifier::instance().subscribe(Coordination::Error::ZSESSIONEXPIRED, [&result](){ result *= 7; });

    EventNotifier::instance().notify(Coordination::Error::ZSESSIONEXPIRED);
    ASSERT_EQ(result, 21);

    result = 1;
    handler3.reset();
    EventNotifier::instance().notify(Coordination::Error::ZSESSIONEXPIRED);
    ASSERT_EQ(result, 7);

    auto handler11 = EventNotifier::instance().subscribe(Coordination::Error::ZSESSIONEXPIRED, [&result](){ result *= 11; });

    result = 1;
    handler7.reset();
    EventNotifier::instance().notify(Coordination::Error::ZSESSIONEXPIRED);
    ASSERT_EQ(result, 11);

    EventNotifier::HandlerPtr handler13;
    {
      handler13 = EventNotifier::instance().subscribe(Coordination::Error::ZSESSIONEXPIRED, [&result](){ result *= 13; });
    }

    result = 1;
    EventNotifier::instance().notify(Coordination::Error::ZSESSIONEXPIRED);
    ASSERT_EQ(result, 143);

    result = 1;
    handler11.reset();
    handler13.reset();
    EventNotifier::instance().notify(Coordination::Error::ZSESSIONEXPIRED);
    ASSERT_EQ(result, 1);
}
