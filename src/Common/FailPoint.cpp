#include <Common/Exception.h>
#include <Common/FailPoint.h>

#include <boost/core/noncopyable.hpp>
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
    ONCE(distributed_cache_fail_continue_request) \
    ONCE(distributed_cache_fail_choose_server) \
    REGULAR(file_cache_stall_free_space_ratio_keeping_thread) \
    REGULAR(distributed_cache_fail_connect_non_retriable) \
    REGULAR(distributed_cache_fail_connect_retriable) \
    REGULAR(object_storage_queue_fail_commit) \
    REGULAR(object_storage_queue_fail_after_insert) \
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
    PAUSEABLE_ONCE(delta_lake_metadata_iterate_pause) \
    PAUSEABLE(dummy_pausable_failpoint) \
    ONCE(execute_query_calling_empty_set_result_func_on_exception) \
    ONCE(terminate_with_exception) \
    ONCE(terminate_with_std_exception) \
    ONCE(libcxx_hardening_out_of_bounds_assertion) \
    ONCE(receive_timeout_on_table_status_response) \
    ONCE(delta_kernel_fail_literal_visitor) \
    ONCE(column_aggregate_function_ensureOwnership_exception) \
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
    PAUSEABLE_ONCE(storage_shared_merge_tree_mutate_pause_before_wait) \
    PAUSEABLE(database_replicated_startup_pause) \
    ONCE(keeper_leader_sets_invalid_digest) \
    ONCE(parallel_replicas_wait_for_unused_replicas) \
    REGULAR(plain_object_storage_copy_fail_on_file_move) \
    REGULAR(database_replicated_delay_recovery) \
    REGULAR(database_replicated_delay_entry_execution) \
    PAUSEABLE(database_replicated_stop_entry_execution) \
    REGULAR(remove_merge_tree_part_delay) \
    REGULAR(plain_object_storage_copy_temp_source_file_fail_on_file_move) \
    REGULAR(plain_object_storage_copy_temp_target_file_fail_on_file_move) \
    REGULAR(output_format_sleep_on_progress) \
    ONCE(smt_commit_exception_before_op) \
    ONCE(disk_object_storage_fail_commit_metadata_transaction) \
    ONCE(disk_object_storage_fail_precommit_metadata_transaction) \
    REGULAR(slowdown_parallel_replicas_local_plan_read) \
    ONCE(iceberg_writes_cleanup) \
    ONCE(backup_add_empty_memory_table) \
    PAUSEABLE(sc_state_application_pause) \
    PAUSEABLE(sc_state_application_pause_after_fetch) \
    REGULAR(sc_intentions_commit_fail) \
    REGULAR(sleep_in_logs_flush) \
    ONCE(database_replicated_drop_before_removing_keeper_failed) \
    ONCE(database_replicated_drop_after_removing_keeper_failed) \
    PAUSEABLE_ONCE(mt_mutate_task_pause_in_prepare) \
    PAUSEABLE(rmt_mutate_task_pause_in_prepare) \
    PAUSEABLE(rmt_merge_selecting_task_pause_when_scheduled) \
    PAUSEABLE(mt_merge_selecting_task_pause_when_scheduled) \
    REGULAR(mt_select_parts_to_mutate_no_free_threads) \
    REGULAR(mt_select_parts_to_mutate_max_part_size) \
    REGULAR(rmt_merge_selecting_task_no_free_threads) \
    REGULAR(rmt_merge_selecting_task_max_part_size) \
    PAUSEABLE(smt_mutate_task_pause_in_prepare) \
    PAUSEABLE(smt_merge_selecting_task_pause_when_scheduled) \
    REGULAR(smt_merge_selecting_task_reach_memory_limit) \
    REGULAR(smt_merge_selecting_task_max_part_size) \
    ONCE(shared_set_full_update_fails_when_initializing) \
    PAUSEABLE(after_snapshot_clean_pause) \
    ONCE(parallel_replicas_reading_response_timeout) \
    ONCE(database_iceberg_gcs) \
    REGULAR(rmt_delay_execute_drop_range) \
    REGULAR(rmt_delay_commit_part) \
    ONCE(local_object_storage_network_error_during_remove) \
    ONCE(parallel_replicas_check_read_mode_always)\
    REGULAR(lightweight_show_tables)

namespace FailPoints
{
#define M(NAME) extern const char(NAME)[] = #NAME "";
APPLY_FOR_FAILPOINTS(M, M, M, M)
#undef M
}

#if USE_LIBFIU

std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointInjection::fail_point_wait_channels;
std::mutex FailPointInjection::mu;

struct FailPointChannel
{
    /// Condition variable for target threads to wait when paused at failpoint
    std::condition_variable pause_cv;

    /// Condition variable for target threads to wait for resume notification
    std::condition_variable resume_cv;

    /// Number of threads currently paused at this failpoint
    size_t pause_count = 0;

    /// Resume epoch: incremented on each notify or disable to wake up waiting threads.
    /// Threads record the epoch when they start waiting, and only wake up
    /// if the current epoch is greater than their recorded epoch.
    size_t resume_epoch = 0;

    /// Pause epoch: incremented each time a thread pauses at this failpoint.
    /// Used by waitForPause to distinguish new pauses from stale ones:
    /// after a notify, waitForPause waits for pause_epoch > resume_epoch,
    /// ensuring the pause happened after the most recent resume.
    size_t pause_epoch = 0;
};

void FailPointInjection::pauseFailPoint(const String & fail_point_name)
{
    fiu_do_on(fail_point_name.c_str(), FailPointInjection::notifyPauseAndWaitForResume(fail_point_name););
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
        /// Increment resume_epoch to wake up all waiting threads.
        ++iter->second->resume_epoch;
        iter->second->resume_cv.notify_all();
        iter->second->pause_cv.notify_all();
        fail_point_wait_channels.erase(iter);
    }
    fiu_disable(fail_point_name.c_str());
}

void FailPointInjection::notifyFailPoint(const String & fail_point_name)
{
    std::lock_guard lock(mu);
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
    {
        /// Increment resume_epoch to mark a new notification cycle
        ++iter->second->resume_epoch;
        iter->second->resume_cv.notify_all();
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find channel for fail point {}", fail_point_name);
    }
}

void FailPointInjection::notifyPauseAndWaitForResume(const String & fail_point_name)
{
    std::unique_lock lock(mu);
    auto iter = fail_point_wait_channels.find(fail_point_name);
    if (iter == fail_point_wait_channels.end())
        return;

    auto channel = iter->second;
    size_t my_resume_epoch = channel->resume_epoch;

    /// Signal that a thread has reached and paused at this failpoint
    ++channel->pause_count;
    ++channel->pause_epoch;
    channel->pause_cv.notify_all();

    /// Wait for resume_epoch to be incremented by notify or disable
    channel->resume_cv.wait(lock, [&] {
        return channel->resume_epoch > my_resume_epoch;
    });

    --channel->pause_count;

}

void FailPointInjection::waitForPause(const String & fail_point_name)
{
    std::unique_lock lock(mu);
    auto iter = fail_point_wait_channels.find(fail_point_name);
    if (iter == fail_point_wait_channels.end())
        return;

    auto channel = iter->second;

    /// Wait until a thread has paused at this failpoint after the most recent resume.
    /// Using pause_epoch > resume_epoch instead of pause_count > 0 avoids a race:
    /// after NOTIFY, the task thread may not have decremented pause_count yet,
    /// so a stale pause_count > 0 could cause waitForPause to return prematurely.
    channel->pause_cv.wait(lock, [&] {
        return channel->pause_epoch > channel->resume_epoch;
    });
}

void FailPointInjection::waitForResume(const String & fail_point_name)
{
    std::unique_lock lock(mu);
    auto iter = fail_point_wait_channels.find(fail_point_name);
    if (iter == fail_point_wait_channels.end())
        return;

    auto channel = iter->second;
    size_t my_resume_epoch = channel->resume_epoch;

    /// Wait for resume_epoch to be incremented by notify or disable
    channel->resume_cv.wait(lock, [&] {
        return channel->resume_epoch > my_resume_epoch;
    });
}

std::vector<FailPointInjection::FailPointInfo> FailPointInjection::getFailPoints()
{
    std::vector<FailPointInfo> result;

#define SUB_M(NAME, TP)                                 \
    result.push_back(                                   \
        FailPointInfo{                                  \
            .name = FailPoints::NAME,                   \
            .type = FailPointType::TP,                  \
            .enabled = fiu_fail(FailPoints::NAME) != 0, \
        });
#define ADD_ONCE(NAME) SUB_M(NAME, Once)
#define ADD_REGULAR(NAME) SUB_M(NAME, Regular)
#define ADD_PAUSEABLE_ONCE(NAME) SUB_M(NAME, PauseableOnce)
#define ADD_PAUSEABLE(NAME) SUB_M(NAME, Pauseable)
    APPLY_FOR_FAILPOINTS(ADD_ONCE, ADD_REGULAR, ADD_PAUSEABLE_ONCE, ADD_PAUSEABLE)
#undef SUB_M
#undef ADD_ONCE
#undef ADD_REGULAR
#undef ADD_PAUSEABLE_ONCE
#undef ADD_PAUSEABLE

    return result;
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

void FailPointInjection::notifyFailPoint(const String &)
{
}

void FailPointInjection::wait(const String &)
{
}

void FailPointInjection::waitForPause(const String &)
{
}

void FailPointInjection::waitForResume(const String &)
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

std::vector<FailPointInjection::FailPointInfo> FailPointInjection::getFailPoints()
{
    std::vector<FailPointInfo> result;

    return result;
}

#endif // USE_LIBFIU

}
