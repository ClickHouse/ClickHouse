#include <Common/Exception.h>
#include <Common/FailPoint.h>

#include <boost/core/noncopyable.hpp>
#include <chrono>
#include <condition_variable>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
};

#if USE_LIBFIU
static struct InitFiu
{
    InitFiu()
    {
        fiu_init(0);
    }
} init_fiu;
#endif

/// We should define different types of failpoints here. There are four types of them:
/// - ONCE: the failpoint will only be triggered once.
/// - REGULAR: the failpoint will always be triggered until disableFailPoint is called.
/// - PAUSEABLE_ONCE: the failpoint will be blocked one time when pauseFailPoint is called, util disableFailPoint is called.
/// - PAUSEABLE: the failpoint will be blocked every time when pauseFailPoint is called, util disableFailPoint is called.

#define APPLY_FOR_FAILPOINTS(ONCE, REGULAR, PAUSEABLE_ONCE, PAUSEABLE) \
    ONCE(replicated_merge_tree_commit_zk_fail_after_op) \
    ONCE(replicated_queue_fail_next_entry) \
    REGULAR(replicated_queue_unfail_entries) \
    ONCE(replicated_merge_tree_insert_quorum_fail_0) \
    REGULAR(replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault) \
    REGULAR(use_delayed_remote_source) \
    REGULAR(cluster_discovery_faults) \
    REGULAR(replicated_sends_failpoint) \
    REGULAR(stripe_log_sink_write_fallpoint) \
    ONCE(smt_commit_merge_mutate_zk_fail_after_op) \
    ONCE(smt_commit_merge_mutate_zk_fail_before_op) \
    ONCE(smt_commit_write_zk_fail_after_op) \
    ONCE(smt_commit_write_zk_fail_before_op) \
    PAUSEABLE_ONCE(smt_commit_tweaks_gate_open) \
    PAUSEABLE_ONCE(smt_commit_tweaks_gate_close) \
    ONCE(smt_commit_merge_change_version_before_op) \
    ONCE(smt_merge_mutate_intention_freeze_in_destructor) \
    ONCE(smt_add_part_sleep_after_add_before_commit) \
    ONCE(smt_sleep_in_constructor) \
    ONCE(meta_in_keeper_create_metadata_failure) \
    ONCE(smt_insert_retry_timeout) \
    ONCE(smt_insert_fake_hardware_error) \
    ONCE(smt_sleep_after_hardware_in_insert) \
    ONCE(smt_throw_keeper_exception_after_successful_insert) \
    ONCE(smt_lightweight_snapshot_fail) \
    ONCE(smt_lightweight_update_sleep_after_block_allocation) \
    ONCE(smt_merge_task_sleep_in_prepare) \
    ONCE(rmt_lightweight_update_sleep_after_block_allocation) \
    ONCE(rmt_merge_task_sleep_in_prepare) \
    ONCE(s3_read_buffer_throw_expired_token) \
    ONCE(distributed_cache_fail_request_in_the_middle_of_request) \
    ONCE(object_storage_queue_fail_commit_once) \
    REGULAR(distributed_cache_fail_connect_non_retriable) \
    REGULAR(distributed_cache_fail_connect_retriable) \
    REGULAR(object_storage_queue_fail_commit) \
    REGULAR(object_storage_queue_fail_startup) \
    REGULAR(smt_dont_merge_first_part) \
    REGULAR(smt_mutate_only_second_part) \
    REGULAR(smt_sleep_in_schedule_data_processing_job) \
    REGULAR(cache_warmer_stall) \
    REGULAR(file_cache_dynamic_resize_fail_to_evict) \
    REGULAR(check_table_query_delay_for_part) \
    REGULAR(dummy_failpoint) \
    REGULAR(prefetched_reader_pool_failpoint) \
    REGULAR(shared_set_sleep_during_update) \
    REGULAR(smt_outdated_parts_exception_response) \
    REGULAR(object_storage_queue_fail_in_the_middle_of_file) \
    PAUSEABLE_ONCE(replicated_merge_tree_insert_retry_pause) \
    PAUSEABLE_ONCE(finish_set_quorum_failed_parts) \
    PAUSEABLE_ONCE(finish_clean_quorum_failed_parts) \
    PAUSEABLE_ONCE(smt_wait_next_mutation) \
    PAUSEABLE(dummy_pausable_failpoint) \
    ONCE(execute_query_calling_empty_set_result_func_on_exception) \
    ONCE(receive_timeout_on_table_status_response) \
    ONCE(delta_kernel_fail_literal_visitor) \
    REGULAR(keepermap_fail_drop_data) \
    REGULAR(lazy_pipe_fds_fail_close) \
    PAUSEABLE(infinite_sleep) \
    PAUSEABLE(stop_moving_part_before_swap_with_active) \
    REGULAR(replicated_merge_tree_all_replicas_stale) \
    REGULAR(zero_copy_lock_zk_fail_before_op) \
    REGULAR(zero_copy_lock_zk_fail_after_op) \
    REGULAR(plain_object_storage_write_fail_on_directory_create) \
    REGULAR(plain_object_storage_write_fail_on_directory_move) \
    REGULAR(zero_copy_unlock_zk_fail_before_op) \
    REGULAR(zero_copy_unlock_zk_fail_after_op) \
    REGULAR(plain_rewritable_object_storage_azure_not_found_on_init) \
    PAUSEABLE(storage_merge_tree_background_clear_old_parts_pause) \
    PAUSEABLE(database_replicated_startup_pause) \
    ONCE(keeper_leader_sets_invalid_digest) \
    ONCE(parallel_replicas_wait_for_unused_replicas) \
    REGULAR(plain_object_storage_copy_fail_on_file_move) \
    REGULAR(database_replicated_delay_recovery) \
    REGULAR(database_replicated_delay_entry_execution) \
    REGULAR(remove_merge_tree_part_delay) \
    REGULAR(plain_object_storage_copy_temp_source_file_fail_on_file_move) \
    REGULAR(plain_object_storage_copy_temp_target_file_fail_on_file_move) \
    REGULAR(output_format_sleep_on_progress) \
    REGULAR(slowdown_parallel_replicas_local_plan_read) \
    ONCE(iceberg_writes_cleanup) \
    ONCE(backup_add_empty_memory_table) \
    REGULAR(refresh_task_stop_racing_for_running_refresh) \
    REGULAR(sleep_in_logs_flush) \
    ONCE(smt_commit_exception_before_op) \
    ONCE(disk_object_storage_fail_commit_metadata_transaction) \
    ONCE(database_replicated_drop_before_removing_keeper_failed) \
    ONCE(database_replicated_drop_after_removing_keeper_failed)


namespace FailPoints
{
#define M(NAME) extern const char(NAME)[] = #NAME "";
APPLY_FOR_FAILPOINTS(M, M, M, M)
#undef M
}

#if USE_LIBFIU

std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointInjection::fail_point_wait_channels;
std::mutex FailPointInjection::mu;

class FailPointChannel : private boost::noncopyable
{
public:
    explicit FailPointChannel(UInt64 timeout_)
        : timeout_ms(timeout_)
    {}
    FailPointChannel()
        : timeout_ms(0)
    {}

    void wait()
    {
        std::unique_lock lock(m);
        if (timeout_ms == 0)
            cv.wait(lock);
        else
            cv.wait_for(lock, std::chrono::milliseconds(timeout_ms));
    }

    void notifyAll()
    {
        std::unique_lock lock(m);
        cv.notify_all();
    }

private:
    UInt64 timeout_ms;
    std::mutex m;
    std::condition_variable cv;
};

void FailPointInjection::enablePauseFailPoint(const String & fail_point_name, UInt64 time_ms)
{
#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        std::lock_guard lock(mu);                                                                           \
        fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>(time_ms));   \
        return;                                                                                             \
    }
#define ONCE(NAME)
#define REGULAR(NAME)
#define PAUSEABLE_ONCE(NAME) SUB_M(NAME, FIU_ONETIME)
#define PAUSEABLE(NAME) SUB_M(NAME, 0)
    APPLY_FOR_FAILPOINTS(ONCE, REGULAR, PAUSEABLE_ONCE, PAUSEABLE)
#undef SUB_M
#undef ONCE
#undef REGULAR
#undef PAUSEABLE_ONCE
#undef PAUSEABLE

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find fail point {}", fail_point_name);
}

void FailPointInjection::pauseFailPoint(const String & fail_point_name)
{
    fiu_do_on(fail_point_name.c_str(), FailPointInjection::wait(fail_point_name););
}

void FailPointInjection::enableFailPoint(const String & fail_point_name)
{
#define SUB_M(NAME, flags, pause)                                                                               \
    if (fail_point_name == FailPoints::NAME)                                                                    \
    {                                                                                                           \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/     \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                        \
        if (pause)                                                                                               \
        {                                                                                                       \
            std::lock_guard lock(mu);                                                                           \
            fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>());       \
        }                                                                                                       \
        return;                                                                                                 \
    }
#define ONCE(NAME) SUB_M(NAME, FIU_ONETIME, 0)
#define REGULAR(NAME) SUB_M(NAME, 0, 0)
#define PAUSEABLE_ONCE(NAME) SUB_M(NAME, FIU_ONETIME, 1)
#define PAUSEABLE(NAME) SUB_M(NAME, 0, 1)
    APPLY_FOR_FAILPOINTS(ONCE, REGULAR, PAUSEABLE_ONCE, PAUSEABLE)
#undef SUB_M
#undef ONCE
#undef REGULAR
#undef PAUSEABLE_ONCE
#undef PAUSEABLE

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find fail point {}", fail_point_name);
}

void FailPointInjection::disableFailPoint(const String & fail_point_name)
{
    std::lock_guard lock(mu);
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
    {
        /// can not rely on deconstruction to do the notify_all things, because
        /// if someone wait on this, the deconstruct will never be called.
        iter->second->notifyAll();
        fail_point_wait_channels.erase(iter);
    }
    fiu_disable(fail_point_name.c_str());
}

void FailPointInjection::wait(const String & fail_point_name)
{
    std::unique_lock lock(mu);
    auto iter = fail_point_wait_channels.find(fail_point_name);
    if (iter == fail_point_wait_channels.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find channel for fail point {}", fail_point_name);

    lock.unlock();
    auto ptr = iter->second;
    ptr->wait();
}

#else // USE_LIBFIU

void FailPointInjection::pauseFailPoint(const String &)
{
}

void FailPointInjection::enableFailPoint(const String &)
{
}

void FailPointInjection::enablePauseFailPoint(const String &, UInt64)
{
}

void FailPointInjection::disableFailPoint(const String &)
{
}

void FailPointInjection::wait(const String &)
{
}

void FailPointInjection::enableFromGlobalConfig(const Poco::Util::AbstractConfiguration & config)
{
    String root_key = "fail_points_active";

    Poco::Util::AbstractConfiguration::Keys fail_point_names;
    config.keys(root_key, fail_point_names);

    if (!fail_point_names.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "FIU is not enabled");
}

#endif // USE_LIBFIU

}
