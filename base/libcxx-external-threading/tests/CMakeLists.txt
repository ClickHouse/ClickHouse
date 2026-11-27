set(LIBCXX_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/libcxx")
file(GLOB_RECURSE LIBCXX_THREAD_TESTS RELATIVE "${LIBCXX_DIR}/test/std/thread" CONFIGURE_DEPENDS "${LIBCXX_DIR}/test/std/thread/*.pass.cpp")
list(FILTER LIBCXX_THREAD_TESTS EXCLUDE REGEX "\\.compile\\.pass\\.cpp$")

list(REMOVE_ITEM LIBCXX_THREAD_TESTS
    # The allocator constructor of std::packaged_task is removed since C++17.
    futures/futures.task/futures.task.members/ctor_func_alloc.pass.cpp
    # musl pthread_join does not detect a self-join and hangs.
    thread.jthread/join.deadlock.pass.cpp)

if (SANITIZE)
    # Replaces operator new and operator delete, which the sanitizer runtimes define too.
    list(REMOVE_ITEM LIBCXX_THREAD_TESTS thread.threads/thread.thread.class/thread.thread.constr/F.pass.cpp)
endif ()
if (SANITIZE STREQUAL "address")
    # libc++ marks it unsupported under ASan.
    list(REMOVE_ITEM LIBCXX_THREAD_TESTS futures/futures.async/thread_create_failure.pass.cpp)
endif ()

set(LIBCXX_THREAD_TESTS_FAILING_IN_FIBERS
    # RLIMIT_NPROC does not limit fibers.
    futures/futures.async/thread_create_failure.pass.cpp
    # Several threads busy-wait for each other without yielding.
    thread.mutex/thread.mutex.requirements/thread.shared_mutex.requirements/thread.shared_mutex.class/lock_shared.pass.cpp
    thread.mutex/thread.mutex.requirements/thread.sharedtimedmutex.requirements/thread.sharedtimedmutex.class/lock_shared.pass.cpp
    thread.mutex/thread.mutex.requirements/thread.sharedtimedmutex.requirements/thread.sharedtimedmutex.class/try_lock_shared_for.pass.cpp
    thread.mutex/thread.mutex.requirements/thread.sharedtimedmutex.requirements/thread.sharedtimedmutex.class/try_lock_shared_until.pass.cpp
    thread.mutex/thread.mutex.requirements/thread.sharedtimedmutex.requirements/thread.sharedtimedmutex.class/try_lock_until_deadlock_bug.pass.cpp
    # Wait with std::atomic::wait, which blocks the OS thread.
    thread.semaphore/lost_wakeup.pass.cpp
    thread.semaphore/lost_wakeup.timed.pass.cpp
    thread.stoptoken/stopcallback/cons.const.token.pass.cpp
    thread.stoptoken/stopcallback/cons.rvalue.token.pass.cpp
    thread.stoptoken/stopcallback/dtor.pass.cpp)

add_library(libcxx_thread_tests_silk_thread_impl OBJECT silk_thread_impl.cpp "${ClickHouse_SOURCE_DIR}/contrib/silk/src/util/logger.cpp")
target_compile_options(libcxx_thread_tests_silk_thread_impl PRIVATE -Wno-reserved-identifier)
target_link_libraries(libcxx_thread_tests_silk_thread_impl PRIVATE ch_contrib::silk_fibers)

add_custom_target(libcxx_external_threading_tests)

foreach (test ${LIBCXX_THREAD_TESTS})
    string(MAKE_C_IDENTIFIER "libcxx_thread_${test}" name)
    add_library(${name} OBJECT "${LIBCXX_DIR}/test/std/thread/${test}")
    target_include_directories(${name} PRIVATE "${LIBCXX_DIR}/test/support")
    target_compile_options(${name} PRIVATE -w -UNDEBUG)

    add_executable(${name}_thread $<TARGET_OBJECTS:${name}>)
    add_executable(${name}_silk $<TARGET_OBJECTS:${name}> $<TARGET_OBJECTS:libcxx_thread_tests_silk_thread_impl>)
    target_link_options(${name}_silk PRIVATE -Wl,--wrap=main)
    add_dependencies(libcxx_external_threading_tests ${name}_thread ${name}_silk)

    add_test(NAME libcxx_external_threading/${test}/thread COMMAND ${name}_thread)
    if (NOT test IN_LIST LIBCXX_THREAD_TESTS_FAILING_IN_FIBERS)
        add_test(NAME libcxx_external_threading/${test}/fiber COMMAND ${name}_silk)
        add_test(NAME libcxx_external_threading/${test}/mixed COMMAND ${name}_silk)
        set_tests_properties(libcxx_external_threading/${test}/fiber PROPERTIES ENVIRONMENT LIBCXX_TEST_THREADS=fiber)
        set_tests_properties(libcxx_external_threading/${test}/mixed PROPERTIES ENVIRONMENT LIBCXX_TEST_THREADS=mixed)
    endif ()
endforeach ()

set_property(DIRECTORY APPEND PROPERTY LABELS libcxx_external_threading_tests)
