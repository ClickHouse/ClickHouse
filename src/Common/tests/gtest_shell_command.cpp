#include <iostream>
#include <base/types.h>
#include <Common/ShellCommand.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <chrono>
#include <fstream>
#include <string>
#include <thread>

#include <gtest/gtest.h>


using namespace DB;


TEST(ShellCommand, Execute)
{
    auto command = ShellCommand::execute("echo 'Hello, world!'");

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}

TEST(ShellCommand, ExecuteDirect)
{
    ShellCommand::Config config("/bin/echo");
    config.arguments = {"Hello, world!"};
    auto command = ShellCommand::executeDirect(config);

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}

TEST(ShellCommand, ExecuteWithInput)
{
    auto command = ShellCommand::execute("cat");

    String in_str = "Hello, world!\n";
    ReadBufferFromString in(in_str);
    copyData(in, command->in);
    command->in.close();

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}


namespace
{

/// Blocks until the child has actually exited, by watching for it to become a zombie in `procfs`.
///
/// Not a fixed pause: what these tests need is the state where the very first `waitpid` succeeds,
/// and sleeping "long enough" for that is exactly the kind of synchronization-by-sleep that turns
/// into a flaky test on a loaded machine. `waitpid` cannot be used to check, because reaping is the
/// thing under test - so the state is read instead. Returns false if it never happens.
bool waitUntilZombie(pid_t pid)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);

    while (std::chrono::steady_clock::now() < deadline)
    {
        std::ifstream stat("/proc/" + std::to_string(pid) + "/stat");
        std::string contents;
        if (stat && std::getline(stat, contents))
        {
            /// "pid (comm) state ..." - the command can contain spaces and parentheses, so the
            /// state is the first field after the last ')'.
            const auto comm_end = contents.rfind(')');
            if (comm_end != std::string::npos && comm_end + 2 < contents.size() && contents[comm_end + 2] == 'Z')
                return true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    return false;
}

}

/// Reaping a child closes its pipes, and everything it had written and nobody had read goes with
/// them. `waitDrainingOutput` exists for a caller that owes those bytes to something - an executable
/// UDF with `stderr_reaction` `throw` fails the query on them - so it has to read them out before it
/// lets the descriptors go, not after.
///
/// The command here writes its diagnostic and exits at once, and the test waits until it is provably
/// gone before asking for the wait. So the very first `waitpid` succeeds, and there is no polling
/// loop to pick the bytes up along the way: either the wait reads them after reaping, or they are
/// lost. That ordering is impossible to force from an integration test, where the server reads the
/// command's stderr while it is still reading its response.
TEST(ShellCommand, WaitDrainingOutputKeepsStderrWrittenJustBeforeExit)
{
    auto command = ShellCommand::execute("printf 'boom-boom-boom' >&2; exit 0");

    ASSERT_TRUE(waitUntilZombie(command->getPid()));

    std::string collected;
    EXPECT_TRUE(command->waitDrainingOutput([&](std::string_view chunk) { collected += chunk; }));
    EXPECT_EQ(collected, "boom-boom-boom");
}


/// The same, for a command whose exit status the caller is not checking (`check_exit_code = 0`):
/// the two settings are independent, so a non-zero exit must not be raised while the diagnostic is
/// still delivered.
TEST(ShellCommand, WaitDrainingOutputKeepsStderrWithoutCheckingTheExitStatus)
{
    auto command = ShellCommand::execute("printf 'boom' >&2; exit 3");

    ASSERT_TRUE(waitUntilZombie(command->getPid()));

    std::string collected;
    EXPECT_TRUE(command->waitDrainingOutput(
        [&](std::string_view chunk) { collected += chunk; }, /*check_exit_status=*/ false));
    EXPECT_EQ(collected, "boom");
}


/// A child killed by a signal reports that as an exception, and decoding the status is what raises
/// it - so decoding before reading the pipes loses whatever the command said on its way out. The
/// query then gets "terminated by signal 15" and no idea why.
TEST(ShellCommand, WaitDrainingOutputKeepsStderrOfASignalledChild)
{
    auto command = ShellCommand::execute("printf 'boom' >&2; kill -TERM $$");

    ASSERT_TRUE(waitUntilZombie(command->getPid()));

    std::string collected;
    EXPECT_THROW(command->waitDrainingOutput([&](std::string_view chunk) { collected += chunk; }), DB::Exception);
    EXPECT_EQ(collected, "boom");
}
