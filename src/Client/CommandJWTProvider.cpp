#include <config.h>

#if USE_JWT_CPP && USE_SSL
#include <Client/CommandJWTProvider.h>

#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/copyData.h>
#include <base/scope_guard.h>

#include <csignal>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
}

CommandJWTProvider::CommandJWTProvider(std::string command_, int timeout_seconds_)
    : JWTProvider(/*auth_url=*/"", /*client_id=*/"", /*audience=*/"", std::cout, std::cerr)
    , command(std::move(command_))
    , timeout_seconds(timeout_seconds_)
{
}

std::string CommandJWTProvider::getJWT()
{
    ShellCommand::Config config(command);
    config.new_process_group = true;  // so the watchdog can kill the whole tree, not just /bin/sh
    auto child = ShellCommand::execute(config);
    child->in.close();  // we don't write to the script's stdin; close so reads see EOF
    const pid_t pid = child->getPid();

    std::mutex mutex;
    std::condition_variable cv;
    bool finished = false;
    std::atomic<bool> timed_out{false};

    /// Default-construct the threads first and install the cleanup guard before assigning,
    /// so a thread-constructor failure mid-assignment cannot leave a joinable thread that
    /// would call std::terminate on destruction.
    std::thread watchdog;
    std::thread stderr_forwarder;
    SCOPE_EXIT({
        {
            std::lock_guard lock(mutex);
            finished = true;
        }
        cv.notify_all();
        if (stderr_forwarder.joinable()) stderr_forwarder.join();
        if (watchdog.joinable()) watchdog.join();
    });

    watchdog = std::thread([&, pid]()
    {
        std::unique_lock lock(mutex);
        if (!cv.wait_for(lock, std::chrono::seconds(timeout_seconds), [&]{ return finished; }))
        {
            timed_out = true;
            ::kill(-pid, SIGKILL);
        }
    });

    /// Drain stderr on a separate thread so the child doesn't block on a full pipe.
    /// 1-byte buffer flushes each byte so interactive prompts (e.g. device-flow URL) surface live.
    stderr_forwarder = std::thread([&child]()
    {
        try
        {
            WriteBufferFromOStream wb(std::cerr, /*size=*/1);
            copyData(child->err, wb);
            wb.finalize();
        }
        catch (...) {}
    });

    std::string token;
    readStringUntilEOF(token, child->out);

    /// Drain stderr fully before tryWait, since tryWait closes child->err and reading
    /// a buffer whose fd has just been closed from another thread is UB.
    stderr_forwarder.join();

    /// Cancel the watchdog before tryWait. After tryWait reaps the child, the kernel
    /// may recycle the pid; if the watchdog then fires kill(-pid, ...) it could hit an
    /// unrelated process group.
    {
        std::lock_guard lock(mutex);
        finished = true;
    }
    cv.notify_all();
    watchdog.join();

    /// Reap with a catch: on timeout the child is signaled, and we want our own
    /// error message rather than the noisy CHILD_WAS_NOT_EXITED_NORMALLY one.
    int retcode = 0;
    try { retcode = child->tryWait(); }
    catch (...) { if (!timed_out.load()) throw; }

    if (timed_out.load())
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "--jwt-command timed out after {} seconds", timeout_seconds);

    if (retcode != 0)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "--jwt-command exited with non-zero status {}", retcode);

    if (!token.empty() && token.back() == '\n')
        token.pop_back();

    if (token.empty())
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "--jwt-command produced empty output");

    return token;
}

}

#endif
