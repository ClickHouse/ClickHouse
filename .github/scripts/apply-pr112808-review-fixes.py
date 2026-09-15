from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    s = p.read_text()
    count = s.count(old)
    if count == 0 and new in s:
        return
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, found {count}: {old[:120]!r}")
    p.write_text(s.replace(old, new, 1))


# ProcessList: let an internal RMV explicitly participate in workload resources.
# Keep one absolute admission deadline across its asynchronously acquired QUERY slot and
# the subsequently acquired MEMORY RESERVATION.
replace_once(
    "src/Interpreters/ProcessList.h",
    "#include <condition_variable>\n",
    "#include <chrono>\n#include <condition_variable>\n",
)
replace_once(
    "src/Interpreters/ProcessList.h",
    "    EntryPtr insert(const String & query_, UInt64 normalized_query_hash, const IAST * ast, ContextMutablePtr query_context, UInt64 watch_start_nanoseconds, bool is_internal, QuerySlotPtr query_slot = {});\n",
    "    EntryPtr insert(const String & query_, UInt64 normalized_query_hash, const IAST * ast, ContextMutablePtr query_context, UInt64 watch_start_nanoseconds, bool is_internal, QuerySlotPtr query_slot = {}, bool use_workload_resources = false,\n        std::chrono::steady_clock::time_point workload_admission_deadline = std::chrono::steady_clock::time_point::max());\n",
)
replace_once(
    "src/Interpreters/ProcessList.cpp",
    "    bool is_internal,\n    QuerySlotPtr query_slot)\n",
    "    bool is_internal,\n    QuerySlotPtr query_slot,\n    bool use_workload_resources,\n    std::chrono::steady_clock::time_point workload_admission_deadline)\n",
)
replace_once(
    "src/Interpreters/ProcessList.cpp",
    "    if (!is_unlimited_query)\n    {\n",
    "    if (!is_unlimited_query || use_workload_resources)\n    {\n",
)
replace_once(
    "src/Interpreters/ProcessList.cpp",
    "        const auto admission_deadline = admission_timeout_ms\n            ? std::chrono::steady_clock::now() + saturatedMilliseconds(admission_timeout_ms)\n            : std::chrono::steady_clock::time_point::max();\n",
    "        const auto admission_deadline = workload_admission_deadline != std::chrono::steady_clock::time_point::max()\n            ? workload_admission_deadline\n            : admission_timeout_ms\n                ? std::chrono::steady_clock::now() + saturatedMilliseconds(admission_timeout_ms)\n                : std::chrono::steady_clock::time_point::max();\n",
)

# refresh_workload must dominate both ASTSetQuery carriers: explicit `workload = x`
# and `workload = DEFAULT`, including nested SELECT settings.
replace_once(
    "src/Storages/StorageMaterializedView.cpp",
    "#include <thread>\n",
    "#include <algorithm>\n#include <thread>\n",
)
replace_once(
    "src/Storages/StorageMaterializedView.cpp",
    "            if (auto * settings = node->as<ASTSetQuery>())\n                for (auto & change : settings->changes)\n                    if (change.name == \"workload\")\n                        change.value = refresh_workload;\n",
    "            if (auto * settings = node->as<ASTSetQuery>())\n            {\n                for (auto & change : settings->changes)\n                    if (change.name == \"workload\")\n                        change.value = refresh_workload;\n                settings->default_settings.erase(\n                    std::remove(settings->default_settings.begin(), settings->default_settings.end(), \"workload\"),\n                    settings->default_settings.end());\n            }\n",
)

# Async admission: use workload_admission_timeout_ms without occupying a background
# worker, preserve its deadline for memory admission, and do not dispatch execution after
# stop/pause has already cancelled the attempt.
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "#include <thread>\n",
    "#include <algorithm>\n#include <thread>\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "#include <Common/thread_local_rng.h>\n",
    "#include <Common/thread_local_rng.h>\n#include <Common/saturatedDuration.h>\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "    extern const SettingsString workload;\n",
    "    extern const SettingsString workload;\n    extern const SettingsMilliseconds workload_admission_timeout_ms;\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "    extern const int RESOURCE_ACCESS_DENIED;\n",
    "    extern const int RESOURCE_ACCESS_DENIED;\n    extern const int QUERY_SLOT_ACQUISITION_TIMEOUT;\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "    AsyncQuerySlot(RefreshTask & task_, ClassifierPtr classifier_, ResourceLink link_, String workload_)\n        : task(task_), classifier(std::move(classifier_)), link(link_), workload(std::move(workload_))\n",
    "    AsyncQuerySlot(RefreshTask & task_, ClassifierPtr classifier_, ResourceLink link_, String workload_,\n        std::chrono::steady_clock::time_point admission_deadline_)\n        : task(task_), classifier(std::move(classifier_)), link(link_), workload(std::move(workload_)), admission_deadline(admission_deadline_)\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "    bool isReady() const\n    {\n        std::lock_guard lock(slot_mutex);\n        return ready;\n    }\n\n    void checkGranted(const String & current_workload) const\n",
    "    bool isReady() const\n    {\n        std::lock_guard lock(slot_mutex);\n        return ready;\n    }\n\n    void expireIfTimedOut()\n    {\n        if (admission_deadline == std::chrono::steady_clock::time_point::max()\n            || std::chrono::steady_clock::now() < admission_deadline)\n            return;\n        if (!link.queue->cancelRequest(this))\n            return;\n\n        std::lock_guard lock(slot_mutex);\n        timed_out = true;\n        complete();\n    }\n\n    UInt64 nextCheckDelayMs() const\n    {\n        if (admission_deadline == std::chrono::steady_clock::time_point::max())\n            return 1000;\n        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(\n            admission_deadline - std::chrono::steady_clock::now()).count();\n        if (remaining <= 0)\n            return 1;\n        return static_cast<UInt64>(std::min<Int64>(remaining, 1000));\n    }\n\n    std::chrono::steady_clock::time_point getAdmissionDeadline() const\n    {\n        return admission_deadline;\n    }\n\n    void checkGranted(const String & current_workload) const\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "        chassert(ready);\n        if (exception)\n",
    "        chassert(ready);\n        if (timed_out)\n            throw Exception(ErrorCodes::QUERY_SLOT_ACQUISITION_TIMEOUT,\n                \"Timed out waiting to acquire a query slot for workload scheduling (exceeded workload_admission_timeout_ms)\");\n        if (exception)\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "    bool ready = true;\n    bool granted = false;\n    std::exception_ptr exception;\n",
    "    bool ready = true;\n    bool granted = false;\n    bool timed_out = false;\n    std::exception_ptr exception;\n    std::chrono::steady_clock::time_point admission_deadline;\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "        if (!is_shutdown && execution.state == ExecutionState::State::WaitingForResource)\n        {\n            if (execution.query_slot->isReady())\n            {\n                /// Resolve admission before the coordination early returns as well: losing Keeper\n                /// capabilities or ownership must not strand an already-cancelled attempt.\n                execution_task->schedule();\n                execution.state = ExecutionState::State::Requested;\n            }\n            else\n            {\n                /// Normally completion wakes us immediately. Keep a scheduled check so a failed\n                /// wake-up can be reported as an admission error without another scheduler callback.\n                scheduling_task->scheduleAfter(1000);\n            }\n        }\n",
    "        if (!is_shutdown && execution.state == ExecutionState::State::WaitingForResource)\n        {\n            if (!execution.query_slot->isReady())\n                execution.query_slot->expireIfTimedOut();\n\n            if (execution.query_slot->isReady())\n            {\n                /// Cancellation can race with scheduler dequeue. If stop/pause won before execution\n                /// started, release even a concurrently granted slot here instead of dispatching a\n                /// backlogged RefreshExec merely to observe the cancellation.\n                if (execution.interrupt_execution.load())\n                {\n                    execution.query_slot.reset();\n                    execution.admission_exception = nullptr;\n                    execution.znode.last_attempt_time = std::chrono::floor<std::chrono::seconds>(currentTime());\n                    execution.znode.last_attempt_error = \"cancelled\";\n                    execution.znode.refresh_running = false;\n                    execution.state = ExecutionState::State::Finished;\n                    scheduling_task->schedule();\n                }\n                else\n                {\n                    /// Resolve admission before coordination early returns as well: losing Keeper\n                    /// capabilities or ownership must not strand an already-cancelled attempt.\n                    execution_task->schedule();\n                    execution.state = ExecutionState::State::Requested;\n                }\n            }\n            else\n            {\n                /// Completion normally wakes us immediately. Keep a scheduled check both as a\n                /// fallback wake-up and to enforce the admission deadline without blocking a worker.\n                scheduling_task->scheduleAfter(execution.query_slot->nextCheckDelayMs());\n            }\n        }\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "                    if (view->getContext()->getRefreshSet().refreshesStopped())\n                        interruptExecution();\n",
    "                    if (view->getContext()->getRefreshSet().refreshesStopped() || coordination.paused_znode_exists)\n                        interruptExecution();\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "                        /// Arm before enqueue: completion (including failure) may happen immediately.\n                        scheduling_task->scheduleAfter(1000);\n                        execution.query_slot = std::make_unique<AsyncQuerySlot>(*this, std::move(classifier), link,\n                            admission_context->getSettingsRef()[Setting::workload]);\n",
    "                        const UInt64 admission_timeout_ms = static_cast<UInt64>(\n                            admission_context->getSettingsRef()[Setting::workload_admission_timeout_ms].totalMilliseconds());\n                        const auto admission_deadline = admission_timeout_ms\n                            ? std::chrono::steady_clock::now() + saturatedMilliseconds(admission_timeout_ms)\n                            : std::chrono::steady_clock::time_point::max();\n\n                        /// Arm before enqueue: completion (including failure) may happen immediately.\n                        scheduling_task->scheduleAfter(\n                            admission_timeout_ms ? std::max<UInt64>(1, std::min<UInt64>(1000, admission_timeout_ms)) : 1000);\n                        execution.query_slot = std::make_unique<AsyncQuerySlot>(*this, std::move(classifier), link,\n                            admission_context->getSettingsRef()[Setting::workload], admission_deadline);\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "        const bool incremental = isIncremental();\n",
    "        const auto admission_deadline = query_slot\n            ? query_slot->getAdmissionDeadline()\n            : std::chrono::steady_clock::time_point::max();\n        const bool use_workload_resources =\n            view->getContext()->getServerSettings()[ServerSetting::use_query_slot_to_refresh_materialized_view];\n\n        const bool incremental = isIncremental();\n",
)
replace_once(
    "src/Storages/MaterializedView/RefreshTask.cpp",
    "                query_for_logging, normalized_query_hash, refresh_query.get(), refresh_context, Stopwatch{CLOCK_MONOTONIC}.getStart(), internal,\n                std::move(query_slot));\n",
    "                query_for_logging, normalized_query_hash, refresh_query.get(), refresh_context, Stopwatch{CLOCK_MONOTONIC}.getStart(), internal,\n                std::move(query_slot), use_workload_resources, admission_deadline);\n",
)

# Focused regression coverage for all currently live review findings.
test_path = Path("tests/integration/test_refreshable_mv_query_slots/test.py")
s = test_path.read_text()
cleanup_anchor = '        instance.query("DROP RESOURCE IF EXISTS query")\n'
cleanup_new = cleanup_anchor + '        instance.query("DROP RESOURCE IF EXISTS memory")\n'
if cleanup_new not in s:
    if s.count(cleanup_anchor) != 1:
        raise RuntimeError("unexpected cleanup resource anchor")
    s = s.replace(cleanup_anchor, cleanup_new, 1)

insert_anchor = "\n\ndef test_query_slot_released_before_exchange():\n"
new_tests = '''

@pytest.mark.parametrize("nested", [False, True])
def test_refresh_workload_overrides_workload_default(nested):
    create_workload(node)
    if nested:
        select = (
            "SELECT workload, x FROM "
            "(SELECT getSetting('workload') AS workload, toUInt64(1) AS x SETTINGS workload=DEFAULT) "
            "SETTINGS refresh_workload='all'"
        )
    else:
        select = (
            "SELECT getSetting('workload') AS workload, toUInt64(1) AS x "
            "SETTINGS workload=DEFAULT, refresh_workload='all'"
        )
    node.query(
        "CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR "
        "SETTINGS refresh_retries=0 APPEND (workload String, x UInt64) ENGINE Memory EMPTY AS "
        + select
    )
    node.query("SYSTEM REFRESH VIEW mv")
    node.query("SYSTEM WAIT VIEW mv", timeout=30)
    assert node.query("SELECT workload, x FROM mv") == "all\\t1\\n"


def test_async_admission_timeout():
    create_workload(node)
    node.query(
        "CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR "
        "SETTINGS refresh_retries=0 APPEND (x UInt64) ENGINE Memory EMPTY "
        "AS SELECT toUInt64(1) AS x "
        "SETTINGS refresh_workload='all', workload_admission_timeout_ms=200"
    )
    with occupied_slot(node):
        node.query("SYSTEM REFRESH VIEW mv")
        error = node.query_and_get_error("SYSTEM WAIT VIEW mv", timeout=30)
        assert "Timed out waiting to acquire a query slot" in error
        assert node.query("SELECT count() FROM mv") == "0\\n"
        wait_metric(node, "ConcurrentQueryScheduled", 0)


def test_refresh_uses_memory_reservation_resource():
    node.query(
        "CREATE RESOURCE memory (MEMORY RESERVATION);"
        "CREATE WORKLOAD all SETTINGS max_memory='1K'"
    )
    node.query(
        "CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR "
        "SETTINGS refresh_retries=0 APPEND (x UInt64) ENGINE Memory EMPTY "
        "AS SELECT toUInt64(1) AS x "
        "SETTINGS refresh_workload='all', reserve_memory='1M', workload_admission_timeout_ms=200"
    )
    node.query("SYSTEM REFRESH VIEW mv")
    error = node.query_and_get_error("SYSTEM WAIT VIEW mv", timeout=30)
    assert "memory reservation" in error.lower()
    assert node.query("SELECT count() FROM mv") == "0\\n"


def test_stop_replicated_cancels_queued_admission():
    create_workload(node)
    path = f"/test/rmv_query_slots/stop_replicated/{uuid.uuid4()}"
    node.query(f"CREATE DATABASE rmv_slots ENGINE=Replicated('{path}', 's', 'r')")
    create_view(node, "rmv_slots.mv")
    with occupied_slot(node):
        node.query("SYSTEM REFRESH VIEW rmv_slots.mv")
        wait_status(node, "WaitingForResource", database="rmv_slots")
        node.query("SYSTEM STOP REPLICATED VIEW rmv_slots.mv", timeout=30)
        wait_status(node, "Disabled", database="rmv_slots")
        wait_metric(node, "ConcurrentQueryScheduled", 0)
        assert metric(node, "ConcurrentQueryAcquired") == "1"
        assert node.query("SELECT count() FROM rmv_slots.mv") == "0\\n"
    wait_metric(node, "ConcurrentQueryAcquired", 0)
'''
if "def test_async_admission_timeout():" not in s:
    if s.count(insert_anchor) != 1:
        raise RuntimeError("unexpected test insertion anchor")
    s = s.replace(insert_anchor, new_tests + insert_anchor, 1)
test_path.write_text(s)
