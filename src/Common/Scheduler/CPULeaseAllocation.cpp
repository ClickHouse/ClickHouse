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
    extern const Event ConcurrencyControlSlotsAcquiredNonCompeting;
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
    , acquired_increment(CurrentMetrics::ConcurrencyControlAcquired, 0)
{}

CPULeaseAllocation::Lease::~Lease()
{
    parent->release(*this);
}

void CPULeaseAllocation::Lease::startConsumption()
{
    acquired_increment.changeTo(1);
    last_report_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
}

bool CPULeaseAllocation::Lease::renew()
{
    return parent->renew(*this);
}

CPULeaseAllocation::CPULeaseAllocation(SlotCount max_threads_, ResourceLink cpu_link_)
    : max_threads(max_threads_)
    , cpu_link(cpu_link_)
    , wake_threads(max_threads)
    , requests(max_threads) // NOTE: it should not be reallocated after initialization because we use raw pointers and iterators
    , head(requests.begin())
    , tail(head)
{
    chassert(cpu_link);
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
        bool canceled = cpu_link.queue->cancelRequest(&*head);
        if (!canceled) // Request is currently processed by the scheduler thread, we have to wait
            shutdown_cv.wait(lock, [this] { return !enqueued; });
        else
            enqueued = false;
    }

    // Finish all resource requests in consumption state
    while (cur_slots > 0)
    {
        tail->finish();
        tail++;
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
    {
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
        granted--;
        size_t thread_num = acquired_threads.size();
        acquired_threads.push_back(true);
        preempted_threads.push_back(false);
        return AcquiredSlotPtr(new Lease(std::static_pointer_cast<CPULeaseAllocation>(shared_from_this()), thread_num));
    }
    return {};
}

[[nodiscard]] AcquiredSlotPtr CPULeaseAllocation::acquire()
{
    std::unique_lock lock{mutex};
    if (acquired_threads.size() == max_threads)
        return {}; // Max number of threads already acquired
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
    granted--; // Might became negative, but it is ok because we are going allocate a slot later
    size_t thread_num = acquired_threads.size();
    acquired_threads.push_back(true);
    preempted_threads.push_back(false);
    return AcquiredSlotPtr(new Lease(std::static_pointer_cast<CPULeaseAllocation>(shared_from_this()), thread_num));
}

void CPULeaseAllocation::failed(const std::exception_ptr & ptr)
{
    // This code runs in the scheduler thread, so we have to keep it fast and simple
    std::unique_lock lock{mutex};
    enqueued = false;
    scheduled_slot_increment.reset();
    wait_timer.reset();
    exception = ptr;

    // Notify all preempted threads to wake and throw an exception
    for (auto & cv : wake_threads)
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
    scheduled_slot_increment.reset();
    wait_timer.reset();
    cur_slots++;
    head++;
    if (head == requests.end())
        head = requests.begin();
    schedule(lock);
    if (granted < 0 || !resumePreemptedThread(lock))
        granted++;

    // Notify destructor that we are detached from the scheduler
    if (shutdown)
        shutdown_cv.notify_one();
}

bool CPULeaseAllocation::renew(Lease & lease)
{
    UInt64 thread_time_ns = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
    chassert(thread_time_ns >= lease.last_report_ns); // This is guaranteed on Linux for thread clock
    ResourceCost delta_ns = thread_time_ns - lease.last_report_ns;
    if (delta_ns < report_quantum)
        return true; // Not enough time passed to report
    lease.last_report_ns = thread_time_ns;

    std::unique_lock lock{mutex};
    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));

    consume(lock, delta_ns);

    // Consume-in-credit model:
    // We allow to consume allocated + requested resource to avoid frequent preemptions of the last thread
    // But when pending resource request would not cover already consumed resource, we do preemption for downscaling
    if (consumed > requested)
    {
        // Check if preemption is needed
        // NOTE: Bit manipulations on boost::dynamic_bitset lead to allocations and there is no find_last() so we do iteration
        size_t running_threads = 0; // acquired_threads & ~preempted_threads
        size_t last_running = boost::dynamic_bitset<>::npos;
        for (size_t i = acquired_threads.size(); i-- > 0;)
        {
            if (acquired_threads[i] && !preempted_threads[i])
            {
                ++running_threads;
                if (last_running == boost::dynamic_bitset<>::npos)
                    last_running = i;
            }
        }

        if (cur_slots < running_threads && lease.thread_num == last_running)
        {
            // Preemption. If we run more thread than we have slots, the last thread should wait for the next slot to be granted.
            // We only preempt the last running thread to avoid running many threads with low utilization (e.g spread 2 CPU among 10 threads).
            // It is better to run less threads, but utilize CPU better to avoid frequent context switches. This is how down-scaling works.
            preempted_threads.set(lease.thread_num);

            auto preemption_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrencyControlWaitMicroseconds);
            CurrentMetrics::Increment preempted_increment(CurrentMetrics::ConcurrencyControlPreempted);
            lease.acquired_increment.changeTo(0);

            // TODO(serxa): add timeout and return false to stop the thread (we do not want to block threads forever)
            wake_threads[lease.thread_num].wait(lock, [this, thread_num = lease.thread_num] { return !preempted_threads[thread_num] || exception; });
            if (exception)
                throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));

            lease.acquired_increment.changeTo(1);
            // There is no need in updating lease.last_report_ns because it counts only CPU time, not waiting time
        }
    }
    return true;
}

void CPULeaseAllocation::consume(std::unique_lock<std::mutex> & lock, ResourceCost delta_ns)
{
    consumed += delta_ns;
    if (cur_slots > 0 && consumed >= tail->max_consumed)
    {
        tail->finish();
        tail++;
        if (tail == requests.end())
            tail = requests.begin();
        if (cur_slots-- == max_threads)
            schedule(lock); // In case if we renew the last slot, othewise the next request is already scheduled
        // NOTE: we do not finish more than one request per one report to avoid stalling the pipeline for reports larger than quantum
    }
}

void CPULeaseAllocation::schedule(std::unique_lock<std::mutex> &)
{
    if (cur_slots == max_threads || shutdown)
        return;

    ResourceCost cost = quantum + std::max<ResourceCost>(0, consumed - requested);
    head->reset(cost);
    requested += cost;
    head->max_consumed = requested; // Lease expires if we consume what we requested

    // We do not use enqueueRequestUsingBudget() because it redistributes resource between requests in the queue (which might be from different queries).
    // Instead we do budgeting for every query independently for better fairness
    cpu_link.queue->enqueueRequest(&*head);
    enqueued = true;

    scheduled_slot_increment.emplace(CurrentMetrics::ConcurrencyControlScheduled);
    wait_timer.emplace(CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrencyControlWaitMicroseconds));
}

bool CPULeaseAllocation::resumePreemptedThread(std::unique_lock<std::mutex> &)
{
    // We are trying to wake inactive thread with lowest thread number to increase utilization of lower threads
    size_t thread_num = preempted_threads.find_first();
    if (thread_num == boost::dynamic_bitset<>::npos)
        return false; // No preempted threads to wake
    preempted_threads.reset(thread_num);
    wake_threads[thread_num].notify_one(); // Wake the first inactive thread
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
    chassert(acquired_threads[lease.thread_num]);
    chassert(!preempted_threads[lease.thread_num]);
    acquired_threads.reset(lease.thread_num);
}

}
