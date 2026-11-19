#pragma once

#include <Common/Wyrd/Facade/Atomic.h>
#include <Common/Wyrd/Namespace.h>

#include <base/types.h>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

class Gate
{
public:
    void pass();
    void open();
    void close();

private:
    Atomic<UInt32> is_open{0};
};

}
