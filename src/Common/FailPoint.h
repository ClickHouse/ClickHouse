#pragma once

#include <Core/Types.h>

#include <mutex>

#include "config.h"

#if USE_LIBFIU

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#  include <fiu.h>
#  include <fiu-control.h>
#pragma clang diagnostic pop

#else // USE_LIBFIU

// stubs from fiu-local.h
#define fiu_init(flags) 0
#define fiu_fail(name) 0
#define fiu_failinfo() NULL
#define fiu_do_on(name, action)
#define fiu_exit_on(name)
#define fiu_return_on(name, retval)

#endif // USE_LIBFIU

#include <unordered_map>


namespace DB
{

/// This is a simple named failpoint library inspired by https://github.com/pingcap/tiflash
/// The usage is simple:
/// 1. define failpoint with a 'failpoint_name' in FailPoint.cpp
/// 2. inject failpoint in normal code
///   2.1 use fiu_do_on which can inject any code blocks, when it is a regular-triggered / once-triggered failpoint
///   2.2 use pauseFailPoint when it is a pausable failpoint
/// 3. in test file, we can use system failpoint enable/disable 'failpoint_name'

struct FailPointChannel;

class FailPointInjection
{
public:

    static void pauseFailPoint(const String & fail_point_name);

    static void enableFailPoint(const String & fail_point_name);

    static void disableFailPoint(const String & fail_point_name);

    static void notifyFailPoint(const String & fail_point_name);

    /// Notify test code that this thread has paused, then wait for resume notification
    static void notifyPauseAndWaitForResume(const String & fail_point_name);

    /**
      * IMPORTANT DIFFERENCE between waitForPause() and waitForResume():
      *
      * waitForPause():
      *   - Checks STATE (pause_count > 0)
      *   - Can be called AFTER target pauses
      *   - Example: target pauses at T=1, you call waitForPause() at T=5, returns immediately
      *
      * waitForResume():
      *   - Waits for EVENT (resume_epoch increment)
      *   - Must be called BEFORE notify
      *   - Example: notify at T=1, you call waitForResume() at T=5, will timeout
      *
      * This asymmetry exists because:
      * - Pause is a PERSISTENT STATE: threads remain paused until notified
      * - Resume is a TRANSIENT EVENT: happens once when notify is called
     */

    /** Wait for target code to reach and pause at the failpoint.
      *
      * This function waits until at least one thread has reached the failpoint and paused.
      * It checks the current state (pause_count > 0), so it can be called AFTER the target
      * thread has already paused - it will return immediately if threads are already paused.
      *
      * Typical usage pattern:
      *
      * Test code:
      *   SYSTEM ENABLE FAILPOINT fp;
      *   // Trigger background operation (e.g., ALTER TABLE, MERGE, etc.)
      *   SYSTEM WAIT FAILPOINT fp PAUSE;  // Wait for operation to reach failpoint
      *   // Now safe to inspect intermediate state
      *   SELECT ... FROM system.mutations;
      *   SYSTEM NOTIFY FAILPOINT fp;      // Let operation continue
      *
      * Target code:
      *   FailPointInjection::pauseFailPoint(FailPoints::fp);  // Pauses here until notified
      *
      * Key characteristics:
      * - Checks CURRENT STATE: returns immediately if pause_count > 0
      * - Can be called after target thread has already paused
      * - Thread-safe: multiple test threads can wait simultaneously
      */
    static void waitForPause(const String & fail_point_name);

    /** Wait for the failpoint to be notified and threads to resume.
      *
      * This function waits until the failpoint's resume_epoch is incremented, which happens
      * when notifyFailPoint() or disableFailPoint() is called. Unlike waitForPause(), this
      * function waits for an EVENT (epoch change), not a state. This means it must be called
      * BEFORE the notify happens, otherwise it will miss the event and timeout.
      *
      * Typical usage pattern:
      *
      * Test code:
      *   SYSTEM ENABLE FAILPOINT fp;
      *   // Trigger background operation
      *   SYSTEM WAIT FAILPOINT fp PAUSE;       // Wait for pause
      *
      *   // Start waiting for resume BEFORE notifying
      *   SYSTEM WAIT FAILPOINT fp RESUME       // Must wait for resume event in another session
      *
      *   SYSTEM NOTIFY FAILPOINT fp;           // Trigger resume event
      *
      * Key characteristics:
      * - Waits for EVENT: must be called BEFORE notifyFailPoint()
      * - Records current resume_epoch, waits for it to increment
      * - Will timeout if notify was already called before wait starts
      */
    static void waitForResume(const String & fail_point_name);

private:
    static std::mutex mu;
    static std::unordered_map<String, std::shared_ptr<FailPointChannel>> fail_point_wait_channels;
};
}
