#include <Common/checkStackSize.h>
#include <Common/Exception.h>
#include <ext/scope_guard.h>

#include <pthread.h>
#include <cstdint>
#include <sstream>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_PTHREAD_ATTR;
        extern const int LOGICAL_ERROR;
        extern const int TOO_DEEP_RECURSION;
    }
}


static thread_local void * stack_address = nullptr;
static thread_local size_t max_stack_size = 0;

void checkStackSize()
{
    using namespace DB;

    if (!stack_address)
    {
        pthread_attr_t attr;
        if (0 != pthread_getattr_np(pthread_self(), &attr))
            throwFromErrno("Cannot pthread_getattr_np", ErrorCodes::CANNOT_PTHREAD_ATTR);

        SCOPE_EXIT({ pthread_attr_destroy(&attr); });

        if (0 != pthread_attr_getstack(&attr, &stack_address, &max_stack_size))
            throwFromErrno("Cannot pthread_getattr_np", ErrorCodes::CANNOT_PTHREAD_ATTR);
    }

    const void * frame_address = __builtin_frame_address(0);
    uintptr_t int_frame_address = reinterpret_cast<uintptr_t>(frame_address);
    uintptr_t int_stack_address = reinterpret_cast<uintptr_t>(stack_address);

    /// We assume that stack grows towards lower addresses. And that it starts to grow from the end of a chunk of memory of max_stack_size.
    if (int_frame_address > int_stack_address + max_stack_size)
        throw Exception("Logical error: frame address is greater than stack begin address", ErrorCodes::LOGICAL_ERROR);

    size_t stack_size = int_stack_address + max_stack_size - int_frame_address;

    /// Just check if we have already eat more than a half of stack size. It's a bit overkill (a half of stack size is wasted).
    /// It's safe to assume that overflow in multiplying by two cannot occur.
    if (stack_size * 2 > max_stack_size)
    {
        std::stringstream message;
        message << "Stack size too large"
            << ". Stack address: " << stack_address
            << ", frame address: " << frame_address
            << ", stack size: " << stack_size
            << ", maximum stack size: " << max_stack_size;
        throw Exception(message.str(), ErrorCodes::TOO_DEEP_RECURSION);
    }
}
