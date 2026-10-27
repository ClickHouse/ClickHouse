#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Server/ClientEmbedded/ClientEmbeddedRunner.h>
#include <Server/ClientEmbedded/PipeClientDescriptorSet.h>
#include <Common/tests/gtest_global_context.h>
#include <base/scope_guard.h>

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace DB;

/// The SSH server reads the exit code as soon as its channel closes, while the client may still be running.
TEST(ClientEmbeddedRunner, GetExitCodeWhileClientRuns)
{
    auto context = getMutableContext().context;
    auto & access_control = context->getAccessControl();
    const String storage_name = "gtest_client_embedded_runner_memory";
    access_control.addMemoryStorage(storage_name, /*allow_backup_=*/ false);
    auto user = std::make_shared<User>();
    user->setName("gtest_client_embedded_runner_user");
    user->authentication_methods.emplace_back(AuthenticationType::NO_PASSWORD);
    const UUID user_id = access_control.insert(user);
    SCOPE_EXIT({
        access_control.remove(user_id, /*throw_if_not_exists=*/ false);
        if (auto storage = access_control.findStorageByName(storage_name))
            access_control.removeStorage(storage);
    });

    auto session = std::make_unique<Session>(context, ClientInfo::Interface::LOCAL);
    session->authenticate(user->getName(), "", Poco::Net::SocketAddress{});

    ClientEmbeddedRunner runner(std::make_unique<PipeClientDescriptorSet>(), std::move(session));
    /// `--version` makes the client print its version and return 0 without running a query.
    runner.run({{"version", "1"}});

    const int exit_code_while_running = runner.getExitCode();
    EXPECT_TRUE(exit_code_while_running == 0 || exit_code_while_running == 1);

    while (!runner.hasFinished())
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    EXPECT_EQ(runner.getExitCode(), 0);
}
