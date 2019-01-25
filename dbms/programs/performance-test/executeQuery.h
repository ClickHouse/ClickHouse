#pragma once
#include <string>
#include "TestStats.h"
#include "TestStopConditions.h"
#include <Common/InterruptListener.h>
#include <Client/Connection.h>

namespace DB
{
void executeQuery(
    Connection & connection,
    const std::string & query,
    TestStats & statistics,
    TestStopConditions & stop_conditions,
    InterruptListener & interrupt_listener);
}
