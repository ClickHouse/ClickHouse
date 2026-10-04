#pragma once

#include <functional>

namespace DB::Runtime::Wyrd
{

/// Runs a test body under the runtime of the wyrd framework
void runTest(std::function<void()> body);

}
