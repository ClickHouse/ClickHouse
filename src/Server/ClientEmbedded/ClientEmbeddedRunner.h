#pragma once

#include <atomic>
#include <memory>

#include <Server/ClientEmbedded/ClientEmbedded.h>
#include <Server/ClientEmbedded/IClientDescriptorSet.h>
#include <Server/ClientEmbedded/PipeClientDescriptorSet.h>
#include <Server/ClientEmbedded/PtyClientDescriptorSet.h>
#include <Common/ThreadPool.h>
namespace DB
{

// Runs embedded client in a dedicated thread, passes descriptors, checks its state
class ClientEmbeddedRunner
{
public:
    explicit ClientEmbeddedRunner(
        std::unique_ptr<IClientDescriptorSet> && client_descriptor_,
        std::unique_ptr<Session> && database_session_)
        : client_descriptors(std::move(client_descriptor_))
        , db_session(std::move(database_session_))
        , log(&Poco::Logger::get("ClientEmbeddedRunner"))
    {
    }
    ~ClientEmbeddedRunner();

    bool hasStarted() const;
    bool hasFinished() const;
    /// Whether the client waits for the user to type a query, i.e. no query is being processed.
    bool isWaitingForQueryInput() const { return waiting_for_query_input.load(); }
    /// How many times the client has started waiting for a query, i.e. returned to the prompt.
    /// Unlike `isWaitingForQueryInput`, it allows noticing queries that finish between two checks.
    UInt64 getPromptsCount() const { return prompts_count.load(); }
    int getExitCode() const;
    IClientDescriptorSet::DescriptorSet getDescriptorsForServer();
    void closeStdIn() const;
    bool hasPty() const { return client_descriptors->isPty(); }
    // Sets new window size for tty. Works only if IClientDescriptorSet is pty
    void changeWindowSize(int width, int height, int width_pixels, int height_pixels);

    void run(const NameToNameMap & envs, const String & starting_query = "");
private:
    void clientRoutine(NameToNameMap envs, String starting_query);

    // This is used by server thread and client thread, be sure that server only gets them via getDescriptorsForServer.
    std::unique_ptr<IClientDescriptorSet> client_descriptors;
    std::atomic_flag started = ATOMIC_FLAG_INIT;
    std::atomic_flag finished = ATOMIC_FLAG_INIT;
    std::atomic_bool waiting_for_query_input = false;
    std::atomic<UInt64> prompts_count = 0;

    ThreadFromGlobalPool client_thread;
    std::unique_ptr<Session> db_session;
    Poco::Logger * log;

    int exit_code = 1;
};

}
