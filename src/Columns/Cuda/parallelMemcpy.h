#pragma once

#include <memory>
#include <thread>

struct parallelMemcpyThreadParams_
{
    parallelMemcpyThreadParams_(char * __restrict x_, const char * __restrict y_, size_t my_start_, size_t my_size_) : 
        x(x_), y(y_), my_start(my_start_), my_size(my_size_) {}

    char * __restrict       x; 
    const char * __restrict y; 
    size_t                  my_start; 
    size_t                  my_size;
};

void parallelMemcpyThread_(parallelMemcpyThreadParams_ params) 
{
    memcpy(params.x+params.my_start, params.y+params.my_start, params.my_size);
}

void parallelMemcpy(char * __restrict x, const char * __restrict y, const size_t n, const size_t threads_num) 
{
    std::thread     t[threads_num-1];
    for (size_t id = 0;id < threads_num-1;++id) {
        size_t my_start, my_size;
        my_start = (id*n)/threads_num;
        my_size = ((id+1)*n)/threads_num - my_start;
        /// TODO we don't use lambda because of stupid nvcc + gcc-6/7 problem
        t[id] = std::thread(parallelMemcpyThread_, 
            parallelMemcpyThreadParams_(x, y, my_start, my_size) );
    }
    {
        size_t my_start, my_size;
        my_start = ((threads_num-1)*n)/threads_num;
        my_size = ((threads_num)*n)/threads_num - my_start;
        memcpy(x+my_start, y+my_start, my_size);
    }
    for (size_t id = 0;id < threads_num-1;++id) t[id].join();
}