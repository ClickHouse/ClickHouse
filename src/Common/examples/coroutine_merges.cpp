#include <iostream>

#include <cassert>
#include <iostream>
#include <experimental/coroutine>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <memory>
#include <condition_variable>

struct resumable /// NOLINT
{
  struct promise_type /// NOLINT
  {
    using coro_handle = std::experimental::coroutine_handle<promise_type>;

    auto get_return_object() /// NOLINT
    { 
        return coro_handle::from_promise(*this); 
    } 

    auto initial_suspend() /// NOLINT
    { 
        return std::experimental::suspend_always();
    }

    auto final_suspend() noexcept /// NOLINT
    {
        return std::experimental::suspend_always();
    }

    void return_void() {} /// NOLINT

    [[ noreturn ]] void unhandled_exception() /// NOLINT
    {
        std::rethrow_exception(std::current_exception());
    }
  };

  using coro_handle = std::experimental::coroutine_handle<promise_type>;
  resumable(coro_handle handle_) : handle(handle_) { assert(handle); } /// NOLINT
  resumable(resumable &) = delete;
  resumable(resumable &&rhs) : handle(rhs.handle) { rhs.handle = nullptr; }
  bool resume() {
    if (!handle.done())
      handle.resume();
    return !handle.done();
  }
  ~resumable() {
    if (handle)
      handle.destroy();
  }

private:
  coro_handle handle;
};


class MergeTask
{
public:

    MergeTask() : res(executeImpl("anime")) {
    }


    static resumable foo()
    {
      std::cout << "Foo" << std::endl;
      co_await std::experimental::suspend_always();
      std::cout << "Foo" << std::endl;
    }

    static resumable executeImpl(const std::string & anime) {
        std::cout << "Start merge" << std::endl;
        co_await std::experimental::suspend_always();
        std::cout << "Resumed merge" << std::endl;
        co_await std::experimental::suspend_always();
        std::cout << anime << std::endl;

        std::string hello = "Hello, World!";

        auto resumable = foo();

        while (resumable.resume())
        {
          co_await std::experimental::suspend_always();
        }

        std::cout << "Finalize " << std::endl;
    }

    bool execute()
    {
        return res.resume();
    }

private:
    resumable res;
};


int main() {
  
    auto task = std::make_shared<MergeTask>();
    
    task->execute();
    task->execute();
    task->execute();

    std::thread t1{[&]() {
        task->execute();
        task->execute();
    }};

    t1.join();


    std::thread t2{[&]() {
        task->execute();
        task->execute();

        std::thread t3{[&]() {
            task->execute();
            task->execute();
        }};
        t3.join();
    }};

    t2.join();


    task->execute();
    task->execute();
    task->execute();
    task->execute();
}

