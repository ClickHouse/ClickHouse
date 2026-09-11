#include <iostream>
#include <base/types.h>
#include <Common/ShellCommand.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <cerrno>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

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

/// A close-on-exec descriptor with `content` behind it, the way a shared-memory region's `memfd` is
/// held in the server: the child gets it only through `inherited_fds`, never by accident.
int makeInheritableSource(const std::string & content)
{
    int fds[2];
    if (0 != ::pipe(fds))
        return -1;
    if (::write(fds[1], content.data(), content.size()) != static_cast<ssize_t>(content.size()))
        return -1;
    ::close(fds[1]);
    if (-1 == ::fcntl(fds[0], F_SETFD, FD_CLOEXEC))
        return -1;
    return fds[0];
}

/// The child opens the inherited descriptors as `/dev/fd/N` rather than with `<&N`: a POSIX shell
/// only redirects single-digit descriptors, and the numbers here are whatever the test process
/// has free.
std::string readInheritedInChild(const std::vector<std::pair<int, int>> & inherited_fds, const std::string & script)
{
    ShellCommand::Config config("/bin/sh");
    config.arguments = {"-c", script};
    config.inherited_fds = inherited_fds;
    auto command = ShellCommand::executeDirect(config);

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();
    return res;
}

}

/// The descriptor the child is told to expect may be the very number it has in the parent: a
/// region's `memfd` created as 3 is handed over as 3. A plain `dup2(3, 3)` is a no-op that keeps
/// the close-on-exec flag, and the child would find the descriptor closed.
TEST(ShellCommand, InheritsADescriptorUnderItsOwnNumber)
{
    const int source = makeInheritableSource("same number");
    ASSERT_NE(source, -1);

    EXPECT_EQ(readInheritedInChild({{source, source}}, "cat /dev/fd/" + std::to_string(source)), "same number");
    ::close(source);
}

/// Two descriptors handed over as each other's number: whichever is copied first must not destroy
/// the other's source before it is copied.
TEST(ShellCommand, InheritsCrossingDescriptors)
{
    const int first = makeInheritableSource("first");
    const int second = makeInheritableSource("second");
    ASSERT_NE(first, -1);
    ASSERT_NE(second, -1);

    /// The child reads `first`'s number and gets `second`'s content, and the other way round.
    const std::string script = "cat /dev/fd/" + std::to_string(first) + "; echo; cat /dev/fd/" + std::to_string(second);
    EXPECT_EQ(readInheritedInChild({{first, second}, {second, first}}, script), "second\nfirst");

    ::close(first);
    ::close(second);
}

/// The ordinary case, and the one where a target number happens to be free in the parent.
TEST(ShellCommand, InheritsADescriptorUnderAnotherNumber)
{
    const int source = makeInheritableSource("relocated");
    ASSERT_NE(source, -1);

    /// A number that is certainly not open here.
    int probe = ::dup(source);
    ASSERT_NE(probe, -1);
    const int free_number = probe + 1;
    ::close(probe);

    EXPECT_EQ(readInheritedInChild({{free_number, source}}, "cat /dev/fd/" + std::to_string(free_number)), "relocated");
    ::close(source);
}

namespace
{

/// Blocks until the child has actually exited, without reaping it.
///
/// Not a fixed pause: what these tests need is the state where the very first `waitpid` succeeds,
/// and sleeping "long enough" for that is exactly the kind of synchronization-by-sleep that turns
/// into a flaky test on a loaded machine. `waitpid` cannot be used to check, because reaping is the
/// thing under test - so `waitid` with `WNOWAIT` is used instead: it returns once the child has
/// exited and leaves it a zombie for the wait under test to collect. Returns false on error.
bool waitUntilZombie(pid_t pid)
{
    /// Without `WNOHANG` a zero return means exactly one thing: the child named by `P_PID` has
    /// exited. Nothing in `info` needs reading.
    siginfo_t info{};
    while (0 != ::waitid(P_PID, static_cast<id_t>(pid), &info, WEXITED | WNOWAIT))
        if (errno != EINTR)
            return false;
    return true;
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
