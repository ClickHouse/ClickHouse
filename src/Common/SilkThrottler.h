#pragma once

#if defined(OS_LINUX)

#include <Common/Throttler.h>

#include <silk/fibers/fiber.h>

namespace Silk
{

class Throttler final : public DB::Throttler
{
public:
    using DB::Throttler::Throttler;

protected:
    void sleep(UInt64 nanoseconds) override
    {
        silk::FiberScheduler::sleep(nanoseconds);
    }
};

}

#endif
