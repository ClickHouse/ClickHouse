#include <vector>
#include <thread>
#include <iostream>
#include <Common/Stopwatch.h>


int main(int, char **)
{
    static constexpr size_t num_threads = 10;
    static constexpr size_t num_iterations = 3;

    std::vector<std::thread> threads(num_threads);

    AtomicStopwatch watch;
    Stopwatch total_watch;

    for (size_t i = 0; i < num_threads; ++i)
    {
        threads[i] = std::thread([i, &watch, &total_watch]
        {
            size_t iteration = 0;
            while (iteration < num_iterations)
            {
                if (auto lock = watch.compareAndRestartDeferred(1))
                {
                    std::cerr << "Thread " << i << ": begin iteration " << iteration << ", elapsed: " << total_watch.elapsedMilliseconds() << " ms.\n";
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    std::cerr << "Thread " << i << ": end iteration " << iteration << ", elapsed: " << total_watch.elapsedMilliseconds() << " ms.\n";
                    ++iteration;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    return 0;
}
