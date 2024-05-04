#pragma once

#include <Common/CurrentMetrics.h>

namespace tp
{

template <typename Task>
struct ActiveWorkers
{
    using Metric = CurrentMetrics::Metric;

    void activate()
    {
        ++m_scheduled_jobs;
        add(metric_scheduled_jobs);
    }

    void deactivate()
    {
        --m_scheduled_jobs;
        sub(metric_scheduled_jobs);
    }

    ActiveWorkers(Metric metric_active_threads_, Metric metric_scheduled_jobs_)
        : metric_active_threads(metric_active_threads_)
        , metric_scheduled_jobs(metric_scheduled_jobs_)
    {
    }

    Metric metric_active_threads;

protected:
    std::atomic<size_t> m_scheduled_jobs = 0;

    // virtual bool steal(Task & task, size_t acceptor_num) =0;

    virtual ~ActiveWorkers() { }

private:
    Metric metric_scheduled_jobs;

};

}
