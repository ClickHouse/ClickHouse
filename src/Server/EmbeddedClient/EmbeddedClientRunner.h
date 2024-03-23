#pragma once

#include <stdexcept>
#include <Server/EmbeddedClient/EmbeddedClient.h>
#include <Server/EmbeddedClient/IClientDescriptorSet.h>
#include <Server/EmbeddedClient/PipeClientDescriptorSet.h>
#include <Server/EmbeddedClient/PtyClientDescriptorSet.h>
#include <Common/ThreadPool.h>


namespace DB
{

// Runs embedded client in dedicated thread, passes descriptors, checks its state
class EmbeddedClientRunner
{
public:
    bool hasStarted() { return started.test(); }

    bool hasFinished() { return finished.test(); }

    // void stopQuery() { client.stopQuery(); } // this is save for client until he uses thread-safe structures to handle query stopping

    void run(const NameToNameMap & envs, const String & starting_query = "");

    IClientDescriptorSet::DescriptorSet getDescriptorsForServer() { return client_descriptors->getDescriptorsForServer(); }

    bool hasPty() const { return client_descriptors->isPty(); }

    // Sets new window size for tty. Works only if IClientDescriptorSet is pty
    void changeWindowSize(int width, int height, int width_pixels, int height_pixels);

    ~EmbeddedClientRunner();

    explicit EmbeddedClientRunner(std::unique_ptr<IClientDescriptorSet> && client_descriptor_, std::unique_ptr<Session> && dbSession_)
        : client_descriptors(std::move(client_descriptor_)), db_session(std::move(dbSession_)), log(&Poco::Logger::get("EmbeddedClientRunner"))
    {
    }

private:
    void clientRoutine(NameToNameMap envs, String starting_query);

    std::unique_ptr<IClientDescriptorSet>
        client_descriptors; // This is used by server thread and client thread, be sure that server only gets them via getDescriptorsForServer
    std::atomic_flag started = ATOMIC_FLAG_INIT;
    std::atomic_flag finished = ATOMIC_FLAG_INIT;

    ThreadFromGlobalPool client_thread;
    std::unique_ptr<Session> db_session;
    Poco::Logger * log;
    // LocalServerPty client;
};
}
