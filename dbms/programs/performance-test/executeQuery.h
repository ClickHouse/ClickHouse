#pragma once
#include <string>
#include "TestStats.h"
#include "TestStopConditions.h"
#include <Common/InterruptListener.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Client/Connection.h>

namespace DB
{
void executeQuery(
    Connections & connections,
    const std::string & query,
    TestStatsPtrs & statistics,
    TestStopConditions & stop_conditions,
    InterruptListener & interrupt_listener,
    Context & context,
    const Settings & settings,
    size_t connection_index);
}
