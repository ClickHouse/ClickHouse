#ifdef MIGRATION_WRONG_ABI
#    include <__config>
#    undef STD_EXCEPTION_HAS_STACK_TRACE
#endif

#include <cstddef>
#include <exception>
#include <new>

#define MIGRATION_JOIN_IMPL(prefix, suffix) prefix##suffix
#define MIGRATION_JOIN(prefix, suffix) MIGRATION_JOIN_IMPL(prefix, suffix)
#define MIGRATION_API(suffix) MIGRATION_JOIN(MIGRATION_PREFIX, suffix)

extern "C" std::size_t MIGRATION_API(_exception_size)(int kind)
{
    switch (kind)
    {
        case 0: return sizeof(std::exception);
        case 1: return sizeof(std::bad_exception);
        case 2: return sizeof(std::bad_alloc);
    }
    return 0;
}

extern "C" std::size_t MIGRATION_API(_exception_align)(int kind)
{
    switch (kind)
    {
        case 0: return __alignof__(std::exception);
        case 1: return __alignof__(std::bad_exception);
        case 2: return __alignof__(std::bad_alloc);
    }
    return 0;
}

extern "C" void MIGRATION_API(_raise)(int kind)
{
    switch (kind)
    {
        case 0: throw std::exception();
        case 1: throw std::bad_exception();
        case 2: throw std::bad_alloc();
        case 3:
        {
            volatile std::size_t impossible = static_cast<std::size_t>(-1);
            void * allocation = ::operator new(impossible);
            ::operator delete(allocation);
            return;
        }
    }
}

static bool valid_exception(const std::exception & error)
{
#ifndef MIGRATION_WRONG_ABI
    const int count = error.get_stack_trace_size();
    return error.what() != 0 && error.what()[0] != '\0'
        && error.get_stack_trace_frames() != 0 && count > 0 && count <= 32;
#else
    return error.what() != 0 && error.what()[0] != '\0';
#endif
}

extern "C" int MIGRATION_API(_catch)(int kind, void (*raise)(int))
{
    try
    {
        raise(kind);
        return 10;
    }
    catch (const std::bad_alloc & error)
    {
        return (kind == 2 || kind == 3) && valid_exception(error) ? 0 : 11;
    }
    catch (const std::bad_exception & error)
    {
        return kind == 1 && valid_exception(error) ? 0 : 12;
    }
    catch (const std::exception & error)
    {
        return kind == 0 && valid_exception(error) ? 0 : 13;
    }
    catch (...)
    {
        return 14;
    }
}
