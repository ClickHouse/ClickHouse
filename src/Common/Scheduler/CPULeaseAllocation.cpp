#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event ConcurrencyControlWaitMicroseconds;
    extern const Event ConcurrencyControlPreemptedMicroseconds;
    extern const Event ConcurrencyControlSlotsAcquired;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrencyControlScheduled;
    extern const Metric ConcurrencyControlAcquired;
    extern const Metric ConcurrencyControlPreempted;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
}

CPULeaseAllocation::Lease::Lease(CPULeaseAllocationPtr && lease_, size_t thread_num_)
    : parent(std::move(lease_))
    , thread_num(thread_num_)
{}

CPULeaseAllocation::Lease::~Lease()
{
    if (parent)
        parent->release(*this);
}

void CPULeaseAllocation::Lease::startConsumption()
{
    last_report_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
}

bool CPULeaseAllocation::Lease::renew()
{
    if (parent)
        return parent->renew(*this);
    else
        return false;
}

CPULeaseAllocation::CPULeaseAllocation(SlotCount max_threads_, ResourceLink master_link_, ResourceLink worker_link_, Settings settings_ = {})
    : max_threads(max_threads_)
    , master_link(master_link_)
    , worker_link(worker_link_)
    , settings(std::move(settings_))
    , threads(max_threads)
    , requests(max_threads) // NOTE: it should not be reallocated after initialization because we use raw pointers and iterators
    , head(requests.begin())
    , tail(head)
    , acquired_increment(CurrentMetrics::ConcurrencyControlAcquired, 0)
    , scheduled_increment(CurrentMetrics::ConcurrencyControlScheduled, 0)
{
    chassert(max_threads > 0);
    for (Request & request : requests)
        request.lease = this;
    std::unique_lock lock{mutex};
    schedule(lock);
}

CPULeaseAllocation::~CPULeaseAllocation()
{
    std::unique_lock lock{mutex};
    shutdown = true;

    // Properly cancel pending resource request (if any)
    if (enqueued)
    {
        auto * queue = head->is_master_slot ? master_link.queue : worker_link.queue;
        chassert(queue);
        bool canceled = queue->cancelRequest(&*head);
        if (!canceled) // Request is currently processed by the scheduler thread, we have to wait
            shutdown_cv.wait(lock, [this] { return !enqueued; });
        else
            enqueued = false;
    }

    // Finish all resource requests in consumption state
    while (allocated > 0)
    {
        tail->finish();
        ++tail;
        if (tail == requests.end())
            tail = requests.begin();
    }
}

[[nodiscard]] AcquiredSlotPtr CPULeaseAllocation::tryAcquire()
{
    std::unique_lock lock{mutex};
    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));
    if (granted > 0)
        return acquireImpl(lock);
    return {};
}

[[nodiscard]] AcquiredSlotPtr CPULeaseAllocation::acquire()
{
    std::unique_lock lock{mutex};
    if (threads.leased.count() == max_threads)
        return {}; // Max number of threads already acquired
    return acquireImpl(lock);
}

AcquiredSlotPtr CPULeaseAllocation::acquireImpl(std::unique_lock<std::mutex> &)
{
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired);
    acquired_increment.add();
    --granted; // Might became negative, but it is ok because we are going allocate a slot later
    return AcquiredSlotPtr(new Lease(std::static_pointer_cast<CPULeaseAllocation>(shared_from_this()), upscale()));
}

size_t CPULeaseAllocation::upscale()
{
    for (size_t thread_num = 0; thread_num < max_threads; ++thread_num)
    {
        if (!threads.leased[thread_num])
        {
            threads.leased.set(thread_num);
            chassert(!threads.preempted[thread_num]);
            // Update fields about running threads
            ++threads.running_count;
            threads.last_running = std::max(threads.last_running, thread_num);
            return thread_num;
        }
    }
    chassert(false);
    return max_threads;
}

void CPULeaseAllocation::downscale(size_t thread_num)
{
    chassert(threads.leased[thread_num]);
    threads.leased.reset(thread_num);

    if (threads.preempted[thread_num])
        threads.preempted.reset(thread_num);
    else
    {
        // Update fields about running threads
        --threads.running_count;
        if (threads.last_running == thread_num)
        {
            while (threads.last_running-- > 0)
            {
                if (threads.leased[threads.last_running] && !threads.preempted[threads.last_running])
                    break;
            }
        }
    }
}

void CPULeaseAllocation::setPreempted(size_t thread_num)
{
    // Mark the thread as preempted
    chassert(threads.leased[thread_num]);
    chassert(!threads.preempted[thread_num]);
    threads.preempted.set(thread_num);

    // Update fields about running threads
    --threads.running_count;
    if (threads.last_running == thread_num)
    {
        while (threads.last_running-- > 0)
        {
            if (threads.leased[threads.last_running] && !threads.preempted[threads.last_running])
                break;
        }
    }
}

void CPULeaseAllocation::resetPreempted(size_t thread_num)
{
    // Mark the thread as not preempted
    chassert(threads.leased[thread_num]);
    threads.preempted.reset(thread_num);

    // Update fields about running threads
    ++threads.running_count;
    threads.last_running = std::max(threads.last_running, thread_num);
}

void CPULeaseAllocation::failed(const std::exception_ptr & ptr)
{
    // This code runs in the scheduler thread, so we have to keep it fast and simple
    std::unique_lock lock{mutex};
    enqueued = false;
    scheduled_increment.sub();
    wait_timer.reset();
    exception = ptr;

    // Notify all preempted threads to wake and throw an exception
    for (auto & cv : threads.wake)
        cv.notify_one();

    // Notify destructor that we are detached from the scheduler
    if (shutdown)
        shutdown_cv.notify_one();
}

void CPULeaseAllocation::grant()
{
    // This code runs in the scheduler thread, so we have to keep it fast and simple
    std::unique_lock lock{mutex};
    enqueued = false;
    scheduled_increment.sub();
    wait_timer.reset();
    grantImpl(lock);
    // Notify destructor that we are detached from the scheduler
    if (shutdown)
        shutdown_cv.notify_one();
}

void CPULeaseAllocation::grantImpl(std::unique_lock<std::mutex> & lock)
{
    ++allocated;
    ++head;
    if (head == requests.end())
        head = requests.begin();
    schedule(lock); // schedule the next request
    if (granted < 0 || !resumePreemptedThread(lock))
        ++granted;
    // TODO(serxa): we should release granted but not acquired slots after some timeout, to avoid unnecessary overprovisioning
}

bool CPULeaseAllocation::renew(Lease & lease)
{
    UInt64 thread_time_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
    chassert(thread_time_ns >= lease.last_report_ns); // This is guaranteed on Linux for thread clock
    ResourceCost delta_ns = thread_time_ns - lease.last_report_ns;
    if (delta_ns < settings.report_ns)
        return true; // Not enough time passed to report
    lease.last_report_ns = thread_time_ns;

    std::unique_lock lock{mutex};
    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));

    consume(lock, delta_ns);

    // Consume-in-credit model:
    // We allow to consume allocated + requested resource to avoid frequent preemptions of the last running thread
    // But when pending resource request would not cover already consumed resource, we do preemption to lower consumption
    if (consumed_ns > requested_ns)
    {
        // Check if preemption is needed
        size_t thread_num = lease.thread_num;
        if (allocated < threads.running_count && thread_num == threads.last_running)
        {
            // Preemption. If we run more thread than we have slots, the last thread should wait for the next slot to be granted.
            // We only preempt the last running thread to avoid running many threads with low utilization (e.g spread 2 CPU among 10 threads).
            // It is better to run less threads, but utilize CPU better to avoid frequent context switches. This is how down-scaling works.
            setPreempted(thread_num);

            auto preemption_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrencyControlWaitMicroseconds);
            CurrentMetrics::Increment preempted_increment(CurrentMetrics::ConcurrencyControlPreempted);
            acquired_increment.sub(1);

            using Timeout = std::chrono::steady_clock::duration;
            auto timeout = thread_num == 0
                ? Timeout::max() // Never involuntary stop the master thread - only downscale worker threads
                : std::chrono::duration_cast<Timeout>(settings.preemption_timeout);
            auto predicate = [this, thread_num]
            {
                return !threads.preempted[thread_num] || exception;
            };
            if (!threads.wake[thread_num].wait_for(lock, timeout, predicate))
            {
                // Timeout - worker thread should stop, but query continues
                downscale(thread_num);
                lease.parent.reset();
                return false;
            }

            if (exception) // Stop the query
                throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));

            acquired_increment.add(1);
            // There is no need in updating lease.last_report_ns because it counts only CPU time, not waiting time
        }
    }
    return true;
}

void CPULeaseAllocation::consume(std::unique_lock<std::mutex> & lock, ResourceCost delta_ns)
{
    consumed_ns += delta_ns;
    if (allocated > 0 && consumed_ns >= tail->max_consumed)
    {
        tail->finish();
        if (tail->is_master_slot)
            request_master_slot = true;
        ++tail;
        if (tail == requests.end())
            tail = requests.begin();
        if (!enqueued)
            schedule(lock); // In case if we renew the last slot, othewise the next request is already scheduled
        // NOTE: we do not finish more than one request per one report to avoid stalling the pipeline for reports larger than quantum
    }
}

void CPULeaseAllocation::schedule(std::unique_lock<std::mutex> & lock)
{
    chassert(!enqueued);

    if (allocated == max_threads || shutdown)
        return;

    ResourceCost cost = settings.quantum_ns + std::max<ResourceCost>(0, consumed_ns - requested_ns);
    head->reset(cost);
    head->is_master_slot = request_master_slot;
    request_master_slot = false;
    requested_ns += cost;
    head->max_consumed = requested_ns; // Lease expires if we consume what we requested

    if (auto * queue = head->is_master_slot ? master_link.queue : worker_link.queue)
    {
        head->is_noncompeting = false;
        // We do not use enqueueRequestUsingBudget() because it redistributes resource between requests in the queue (which might be from different queries).
        // Instead we do budgeting for every query independently for better fairness
        queue->enqueueRequest(&*head);
        enqueued = true;
        scheduled_increment.add();
        wait_timer.emplace(CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrencyControlWaitMicroseconds));
    }
    else // noncompeting slot - provide immediately for free
    {
        head->is_noncompeting = true;
        grantImpl(lock);
    }
}

bool CPULeaseAllocation::resumePreemptedThread(std::unique_lock<std::mutex> &)
{
    // We are trying to wake inactive thread with lowest thread number to increase utilization of lower threads
    size_t thread_num = threads.preempted.find_first();
    if (thread_num == boost::dynamic_bitset<>::npos)
        return false; // No preempted threads to wake
    resetPreempted(thread_num);
    threads.wake[thread_num].notify_one(); // Wake the first inactive thread
    return true;
}

void CPULeaseAllocation::release(Lease & lease)
{
    UInt64 thread_time_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
    chassert(thread_time_ns >= lease.last_report_ns); // This is guaranteed on Linux for thread clock
    ResourceCost delta_ns = thread_time_ns - lease.last_report_ns;
    lease.last_report_ns = thread_time_ns;

    // Report the last chunk of consumed resource
    std::unique_lock lock{mutex};
    consume(lock, delta_ns);

    // Release the slot
    downscale(lease.thread_num);
    lease.parent.reset();
}

}
