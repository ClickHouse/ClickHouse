#include <iostream>

#include <cassert>
#include <iostream>
#include <experimental/coroutine>
#include <string>
#include <optional>

#include <Common/Exception.h>

template <typename T>
using coroutine_handle = std::experimental::coroutine_handle<T>;

using default_coroutine_handle = std::experimental::coroutine_handle<>;

using suspend_never = std::experimental::suspend_never;
using suspend_always = std::experimental::suspend_always;

template <typename T>
struct suspend_never_val
{
    constexpr bool await_ready() const noexcept { return true; }
    constexpr void await_suspend(default_coroutine_handle) const noexcept {}
    constexpr T await_resume() const noexcept
    {
        std::cout << "  ret " << val << std::endl;
        return val;
    }

    T val;
};

template <typename T>
struct resumable {
  struct promise_type {
    using coro_handle = coroutine_handle<promise_type>;
    auto get_return_object() { return coro_handle::from_promise(*this); }
    auto initial_suspend() { return suspend_never(); }
    auto final_suspend() noexcept { return suspend_never_val<T>{*r->value}; }
    //void return_void() {}
    void return_value(T value_) { r->value = value_; }
    void unhandled_exception() { r->exception = std::current_exception(); }

    explicit promise_type(std::string tag_) : tag(tag_) {}
    ~promise_type() { std::cout << "~promise_type " << tag << std::endl; }
    std::string tag;
    coro_handle next;
    resumable * r = nullptr;
  };

  using coro_handle = coroutine_handle<promise_type>;

  bool await_ready() const noexcept { return false; }
  void await_suspend(coro_handle g) noexcept
  {
      std::cout << "  await_suspend " << my.promise().tag << std::endl;
      std::cout << "  g tag " << g.promise().tag << std::endl;
      g.promise().next = my;
  }
  T await_resume() noexcept
  {
      std::cout << "  await_res " << my.promise().tag << std::endl;
    return *value;
  }

  resumable(coro_handle handle) : my(handle), tag(handle.promise().tag)
  {
      assert(handle);
      my.promise().r = this;
      std::cout << "    resumable " << tag << std::endl;
  }
  resumable(resumable &) = delete;
  resumable(resumable &&rhs) : my(rhs.my), tag(rhs.tag)
  {
      rhs.my = {};
      std::cout << "    resumable&& " << tag << std::endl;
  }
  static bool resume_impl(resumable *r)
  {
      if (r->value)
        return false;

      auto & next = r->my.promise().next;

      if (next)
      {
          if (resume_impl(next.promise().r))
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

  bool resume() {
      return resume_impl(this);
  }

  T res()
  {
      return *value;
  }

  ~resumable() {
      std::cout << "    ~resumable " << tag << std::endl;
  }

private:
  coro_handle my;
  std::string tag;
  std::optional<T> value;
  std::exception_ptr exception;
};

resumable<int> boo(std::string tag)
{
    std::cout << "x" << std::endl;
    co_await suspend_always();
    std::cout << StackTrace().toString();
    std::cout << "y" << std::endl;
    co_return 1;
}

resumable<int> bar(std::string tag)
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

resumable<int> foo(std::string tag) {
  std::cout << "Hello" << std::endl;
  auto res1 = co_await bar("bar1");
  std::cout << "Coro " << res1 << std::endl;
  auto res2 = co_await bar("bar2");
  std::cout << "World " << res2 << std::endl;
  co_return res1 * res2; // 2 * 2 = 4
}

int main()
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
