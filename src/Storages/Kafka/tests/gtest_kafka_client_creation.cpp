#include <gtest/gtest.h>
#include <config.h>

#if USE_RDKAFKA

#include <cppkafka/producer.h>

#include <condition_variable>
#include <mutex>
#include <string_view>

namespace
{

/// Global: before the fix, the threads of the client that `rd_kafka_new` gave up on outlive the test.
std::mutex gate_mutex;
std::condition_variable gate_cv;
bool gate_open = false;

rd_kafka_resp_err_t holdThread(rd_kafka_t *, rd_kafka_thread_type_t, const char *, void *)
{
    std::unique_lock lock(gate_mutex);
    gate_cv.wait(lock, [] { return gate_open; });
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

/// `rd_kafka_new` logs `INIT` once its main thread has not initialized for 60 seconds.
void openGateOnInitTimeout(const rd_kafka_t *, int, const char * facility, const char *)
{
    if (std::string_view(facility) != "INIT")
        return;
    std::lock_guard lock(gate_mutex);
    gate_open = true;
    gate_cv.notify_all();
}

rd_kafka_resp_err_t onNew(rd_kafka_t * rk, const rd_kafka_conf_t *, void *, char *, size_t)
{
    return rd_kafka_interceptor_add_on_thread_start(rk, "gate", holdThread, nullptr);
}

/// cppkafka duplicates the configuration, and `rd_kafka_conf_dup` does not copy interceptors.
rd_kafka_resp_err_t onConfDup(rd_kafka_conf_t * new_conf, const rd_kafka_conf_t *, size_t, const char **, void *)
{
    rd_kafka_conf_interceptor_add_on_conf_dup(new_conf, "gate", onConfDup, nullptr);
    return rd_kafka_conf_interceptor_add_on_new(new_conf, "gate", onNew, nullptr);
}

}

/// `rd_kafka_new` used to return no client after 60 seconds while the main thread kept running with the freed
/// cppkafka object as its opaque, so the next log line of that thread was a use-after-free.
TEST(KafkaClientCreation, WaitsForSlowMainThread)
{
    cppkafka::Configuration config;
    rd_kafka_conf_set_log_cb(config.get_handle(), openGateOnInitTimeout);
    rd_kafka_conf_interceptor_add_on_conf_dup(config.get_handle(), "gate", onConfDup, nullptr);
    EXPECT_NO_THROW(cppkafka::Producer client(config));
}

#endif
