#include <Server/ClientEmbedded/ClientEmbeddedRunner.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool ClientEmbeddedRunner::hasStarted() const
{
    return started.test();
}

bool ClientEmbeddedRunner::hasFinished() const
{
    return finished.test();
}

int ClientEmbeddedRunner::getExitCode() const
{
    return exit_code;
}

IClientDescriptorSet::DescriptorSet ClientEmbeddedRunner::getDescriptorsForServer()
{
    return client_descriptors->getDescriptorsForServer();
}

void ClientEmbeddedRunner::closeStdIn() const
{
    client_descriptors->closeStdIn();
}

void ClientEmbeddedRunner::run(const NameToNameMap & envs, const String & starting_query)
{
    if (started.test_and_set())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Client has been already started");

    LOG_DEBUG(log, "Starting embedded client");
    try
    {
        client_thread = ThreadFromGlobalPool(
            ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, &ClientEmbeddedRunner::clientRoutine, this, envs, starting_query);
    }
    catch (...)
    {
        /// The client routine never ran, so `finished` will never be set. Roll back `started`,
        /// otherwise the session would keep waiting for a client that does not exist
        /// and reject every later `shell` / `exec` request.
        started.clear();
        throw;
    }
}


void ClientEmbeddedRunner::changeWindowSize(int width, int height, int width_pixels, int height_pixels)
{
    auto * pty_descriptors = dynamic_cast<PtyClientDescriptorSet *>(client_descriptors.get());
    if (pty_descriptors == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Accessing window change on non pty descriptors");

    pty_descriptors->changeWindowSize(width, height, width_pixels, height_pixels);
}

ClientEmbeddedRunner::~ClientEmbeddedRunner()
{
    LOG_DEBUG(log, "Closing server descriptors and waiting for client to finish");
    client_descriptors->closeServerDescriptors(); // May throw if something bad happens to descriptors, which will call std::terminate
    if (client_thread.joinable())
        client_thread.join();

    LOG_DEBUG(log, "Embedded client has finished");
}

void ClientEmbeddedRunner::clientRoutine(NameToNameMap envs, String starting_query)
{
    try
    {
        auto descr = client_descriptors->getDescriptorsForClient();
        auto stre = client_descriptors->getStreamsForClient();
        ClientEmbedded client(std::move(db_session), descr.in, descr.out, descr.err, stre.in, stre.out, stre.err);
        client.setWaitingForQueryInputCallback([this](bool waiting)
        {
            if (waiting)
                prompts_count.fetch_add(1);
            waiting_for_query_input.store(waiting);
        });
        exit_code = client.run(envs, starting_query);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    finished.test_and_set();
    if (client_descriptors->isPty())
    {
        char c = 0;
        // Server may poll on a descriptor waiting for client output, wake him up with invisible character
        (void)write(client_descriptors->getDescriptorsForClient().out, &c, 1);
    }
}

}
