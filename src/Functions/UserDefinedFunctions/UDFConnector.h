#ifndef CLICKHOUSE_UDFCONNECTOR_H
#define CLICKHOUSE_UDFCONNECTOR_H

#include <thread>
#include <map>
#include <condition_variable>
#include <mutex>
#include <optional>

#include <boost/noncopyable.hpp>

#include <Functions/FunctionFactory.h>
#include <Common/ShellCommand.h>

#include "UDFAPI.h"
#include "UDFControlCommand.h"

namespace DB
{

class UDFConnector : private boost::noncopyable
{
public:
    explicit UDFConnector(std::optional<unsigned> uid_) : uid(uid_)
    {
        std::unique_lock lock(mutex);

        std::vector<std::string> args;
        if (uid) {
            args.push_back("uid=" + std::to_string(uid.value()));
        }
        manager = ShellCommand::executeDirect("./clickhouse-udf-manager", args, true);
        thread = std::thread([this]() { this->run(); });
    }

    ~UDFConnector()
    {
        std::unique_lock lock(mutex);
        active = false;
        for (auto && waiter : result_waiters) {
            waiter.second.done = waiter.second.canceled = true;
            waiter.second.cv.notify_all();
        }
        thread.join();
    }

    void load(std::string_view filename);
    void getReturnTypeCall(std::string_view) {}
    void execCall(std::string_view) {}

private:
    void run();

    UDFControlCommandResult run_command(UDFControlCommand & cmd)
    {
        std::unique_lock lock(mutex);
        if (!active) {
            return UDFControlCommandResult{1, "Aborted", cmd.request_id};
        }
        cmd.request_id = last_id++;
        auto &waiter = result_waiters[cmd.request_id];
        cmd.write(manager->in);
        while (!waiter.done) {
            waiter.cv.wait(lock);
        }
        auto result = std::move(waiter.result);
        if (waiter.canceled) {
            result_waiters.erase(cmd.request_id);
            return UDFControlCommandResult{1, "Canceled", cmd.request_id};
        }
        result_waiters.erase(cmd.request_id);
        lock.unlock();
        return result;
    }

    std::thread thread;
    std::optional<unsigned> uid;
    bool active = true;

    std::mutex mutex;
    unsigned last_id = 0;

    struct Unit
    {
        std::condition_variable cv;
        UDFControlCommandResult result;
        bool done = false;
        bool canceled = false;
    };
    std::map<unsigned, Unit> result_waiters;

    std::unique_ptr<ShellCommand> manager;
};

}

#endif //CLICKHOUSE_UDFCONNECTOR_H
