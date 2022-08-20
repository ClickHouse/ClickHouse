#include <cassert>
#include <iostream>
#include <string>
#include <optional>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>

#if defined(__clang__)
#include <experimental/coroutine>

namespace std // NOLINT(cert-dcl58-cpp)
{
    using namespace experimental::coroutines_v1;
}

#if __has_warning("-Wdeprecated-experimental-coroutine")
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-experimental-coroutine"
#endif

#else
#include <coroutine>
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

template <typename T>
struct Task
{
    struct promise_type // NOLINT(readability-identifier-naming)
    {
        using coro_handle = std::coroutine_handle<promise_type>;
        auto get_return_object() { return coro_handle::from_promise(*this); } // NOLINT(readability-identifier-naming)
        auto initial_suspend() { return std::suspend_never(); } // NOLINT(readability-identifier-naming)
        auto final_suspend() noexcept { return suspend_value<T>{*r->value}; } // NOLINT(readability-identifier-naming)
        //void return_void() {}
        void return_value(T value_) { r->value = value_; } // NOLINT(readability-identifier-naming)
        void unhandled_exception() // NOLINT(readability-identifier-naming)
        {
            DB::tryLogCurrentException("Logger");
            r->exception = std::current_exception(); // NOLINT(bugprone-throw-keyword-missing)
        }

        explicit promise_type(std::string tag_) : tag(tag_) {}
        ~promise_type() { std::cout << "~promise_type " << tag << std::endl; }
        std::string tag;
        coro_handle next;
        Task * r = nullptr;
    };

    using coro_handle = std::coroutine_handle<promise_type>;

    bool await_ready() const noexcept { return false; } // NOLINT(readability-identifier-naming)
    void await_suspend(coro_handle g) noexcept // NOLINT(readability-identifier-naming)
    {
        std::cout << "  await_suspend " << my.promise().tag << std::endl;
        std::cout << "  g tag " << g.promise().tag << std::endl;
        g.promise().next = my;
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

private:
    coro_handle my;
    std::string tag;
    std::optional<T> value;
    std::exception_ptr exception;
};

Task<int> boo([[maybe_unused]] std::string tag)
{
    std::cout << "x" << std::endl;
    co_await std::suspend_always();
    std::cout << StackTrace().toString();
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
