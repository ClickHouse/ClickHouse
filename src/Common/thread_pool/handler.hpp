#pragma once

namespace tp
{

template <typename Task>
struct ActiveWorkers
{
    std::atomic<size_t> m_active_tasks = 0;

    // virtual bool steal(Task & task, size_t acceptor_num) =0;

    virtual ~ActiveWorkers()
    {
    }
};

}
