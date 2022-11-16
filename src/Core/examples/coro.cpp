#include <cassert>
#include <iostream>
#include <string>
#include <optional>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>

#include <coroutine>
#include <libunwind.h>

#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif

template <typename T>
struct suspend_value // NOLINT(readability-identifier-naming)
{
    constexpr bool await_ready() const noexcept { return true; } // NOLINT(readability-identifier-naming)
    constexpr void await_suspend(std::coroutine_handle<>) const noexcept {} // NOLINT(readability-identifier-naming)
    constexpr T await_resume() const noexcept // NOLINT(readability-identifier-naming)
    {
        std::cout << "  ret " << val << std::endl;
        return val;
    }

    T val;
};

struct PromiseStack
{
    PromiseStack * prev_promise = nullptr;
};

thread_local PromiseStack * promise_stack = nullptr;

template <typename T>
struct Task
{
    struct promise_type : public PromiseStack // NOLINT(readability-identifier-naming)
    {
        using coro_handle = std::coroutine_handle<promise_type>;
        auto get_return_object() { return coro_handle::from_promise(*this); } // NOLINT(readability-identifier-naming)
        auto initial_suspend()
        {
            unw_context_t context;
            unw_cursor_t cursor;
            if (unw_getcontext(&context) == 0 && unw_init_local(&cursor, &context) == 0) {

                unw_word_t ip;
                if (unw_step(&cursor) > 0) {
                    if (unw_get_reg(&cursor, UNW_REG_IP, &ip) == 0) {
                        addr = reinterpret_cast<void *>(static_cast<uintptr_t>(ip));
                    }
                }
            }

            prev_promise = promise_stack;
            promise_stack = this;

            addr_tag = reinterpret_cast<void *>(0xABABABABABA);

            return std::suspend_never();
        } // NOLINT(readability-identifier-naming)
        auto final_suspend() noexcept // NOLINT(readability-identifier-naming)
        {
            promise_stack = prev_promise;
            return suspend_value<T>{*r->value};
        }
        //void return_void() {}
        void return_value(T value_) { r->value = value_; } // NOLINT(readability-identifier-naming)
        void unhandled_exception() // NOLINT(readability-identifier-naming)
        {
            DB::tryLogCurrentException("Logger");
            r->exception = std::current_exception(); // NOLINT(bugprone-throw-keyword-missing)
        }

        explicit promise_type(std::string tag_) : tag(tag_) {}
        ~promise_type() { std::cout << "~promise_type " << tag << std::endl; }
        //coro_handle self;
        void * addr;
        void * addr_tag;
        coro_handle next;
        std::string tag;
        Task * r = nullptr;
    };

    using coro_handle = std::coroutine_handle<promise_type>;

    bool await_ready() const noexcept { return false; } // NOLINT(readability-identifier-naming)
    void await_suspend(coro_handle g) noexcept // NOLINT(readability-identifier-naming)
    {
        std::cout << "  await_suspend " << my.promise().tag << std::endl;
        std::cout << "  g tag " << g.promise().tag << std::endl;
        g.promise().next = my;
        //g.promise().self = g;
    }
    T await_resume() noexcept // NOLINT(readability-identifier-naming)
    {
        std::cout << "  await_res " << my.promise().tag << std::endl;
        return *value;
    }

    Task(coro_handle handle) : my(handle), tag(handle.promise().tag) // NOLINT(google-explicit-constructor)
    {
        assert(handle);
        my.promise().r = this;
        std::cout << "    Task " << tag << std::endl;
    }
    Task(Task &) = delete;
    Task(Task &&rhs) noexcept : my(rhs.my), tag(rhs.tag)
    {
        rhs.my = {};
        std::cout << "    Task&& " << tag << std::endl;
    }
    static bool resumeImpl(Task *r)
    {
        if (r->value)
            return false;

        auto & next = r->my.promise().next;

        if (next)
        {
            if (resumeImpl(next.promise().r))
                return true;
            next = {};
        }

        if (!r->value)
        {
            r->my.resume();
            if (r->exception)
                std::rethrow_exception(r->exception);
        }
        return !r->value;
    }

    static void tmp() {}

    bool resume()
    {
        return resumeImpl(this);
    }

    T res()
    {
        return *value;
    }

    ~Task()
    {
        std::cout << "    ~Task " << tag << std::endl;
    }

    coro_handle my;
private:
    std::string tag;
    std::optional<T> value;
    std::exception_ptr exception;
};

struct co_gethandle_awaitable
{
    co_gethandle_awaitable()
        : m_hWaiter(nullptr)
    {
    }
    bool await_ready() const noexcept
    {
        return m_hWaiter != nullptr;
    }
    bool await_suspend(Task<int>::coro_handle hWaiter) noexcept
    {
        m_hWaiter = hWaiter;
       return false;
    }
    auto await_resume() noexcept
    {
        return m_hWaiter;
    }

    Task<int>::coro_handle m_hWaiter;
};

void print_ptrs(void * addr, int before, int after)
{
    char * ptr = static_cast<char *>(addr);
    for (int i = before; i <= after; ++i)
        std::cout << *reinterpret_cast<void **>(static_cast<void *>(ptr + i * 8)) << std::endl;
}

void fixTrace(StackTrace::FramePointers & frames, size_t offset, size_t size)
{
    if (promise_stack == nullptr)
        return;

    constexpr auto start = Task<int>::resumeImpl;
    constexpr auto end = Task<int>::tmp;
    PromiseStack * last_promise = promise_stack->prev_promise;
    for (size_t i = offset; i < size && last_promise; ++i)
    {
        uintptr_t frame = reinterpret_cast<uintptr_t>(frames[i]);
        std::cerr << reinterpret_cast<uintptr_t>(start) << ' ' << frame << ' ' << reinterpret_cast<uintptr_t>(end) << std::endl;
        if (reinterpret_cast<uintptr_t>(start) <= frame && frame <= reinterpret_cast<uintptr_t>(end))
        {
            frames[i] = *reinterpret_cast<void **>(reinterpret_cast<char *>(last_promise) - sizeof(void *) * 2);
            last_promise = last_promise->prev_promise;
        }
    }
}

Task<int> boo([[maybe_unused]] std::string tag)
{
    std::cout << "x" << std::endl;
    co_await std::suspend_always();

    auto my_handle = co_await co_gethandle_awaitable();

    auto st = StackTrace();
    auto frames = st.getFramePointers();
    std::cout << StackTrace::toStringStatic(frames, st.getOffset(), st.getSize());
    std::cout << "----------\n";
    //frames[2] = my_handle.promise().addr;
    std::cout << StackTrace::toStringStatic(frames, st.getOffset(), st.getSize());
    std::cout << "-=--------\n";
    //frames[2] = *reinterpret_cast<void **>(static_cast<char *>(*reinterpret_cast<void **>(&my_handle)) + sizeof(void *) * 2);
    //std::cout << StackTrace::toStringStatic(frames, st.getOffset(), st.getSize());
    std::cerr << "stack next : " << static_cast<void *>(promise_stack->prev_promise) << std::endl;
    std::cerr << "resume_impl_addr : " << reinterpret_cast<void *>(Task<int>::resumeImpl) << std::endl;
    std::cerr << "tmp_addr : " << reinterpret_cast<void *>(Task<int>::tmp) << std::endl;
    print_ptrs(*reinterpret_cast<void **>(&my_handle), -5, 10);
    std::cerr << "....\n";
    print_ptrs(reinterpret_cast<void *>(promise_stack->prev_promise), -5, 10);
    //frames[2] = *reinterpret_cast<void **>(reinterpret_cast<char *>(promise_stack->prev_promise) - sizeof(void *) * 2);
    fixTrace(frames, st.getOffset(), st.getSize());
    std::cout << StackTrace::toStringStatic(frames, st.getOffset(), st.getSize());

    std::cout << "y" << std::endl;
    co_return 1;
}

Task<int> bar([[maybe_unused]] std::string tag)
{
    std::cout << "a" << std::endl;
    int res1 = co_await boo("boo1");
    std::cout << "b " << res1 << std::endl;
    int res2 = co_await boo("boo2");
    if (res2 == 1)
        throw DB::Exception(1, "hello");
    std::cout << "c " << res2 << std::endl;
    co_return res1 + res2;  // 1 + 1 = 2
}

Task<int> foo([[maybe_unused]] std::string tag)
{
    std::cout << "Hello" << std::endl;
    auto res1 = co_await bar("bar1");
    std::cout << "Coro " << res1 << std::endl;
    auto res2 = co_await bar("bar2");
    std::cout << "World " << res2 << std::endl;
    co_return res1 * res2; // 2 * 2 = 4
}

int main()
{
    //__builtin_coro_resume(reinterpret_cast<void *>(0xABCABCABC));
    Poco::AutoPtr<Poco::ConsoleChannel> app_channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(app_channel);
    Poco::Logger::root().setLevel("trace");

    LOG_INFO(&Poco::Logger::get(""), "Starting");

    try
    {
        auto t = foo("foo");
        std::cout << ".. started" << std::endl;
        while (t.resume())
            std::cout << ".. yielded" << std::endl;
        std::cout << ".. done: " << t.res() << std::endl;
    }
    catch (DB::Exception & e)
    {
        std::cout << "Got exception " << e.what() << std::endl;
        std::cout << e.getStackTraceString() << std::endl;
    }
}
