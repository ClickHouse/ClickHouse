#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>


void alloc_loop()
{
    for (size_t i = 0; i < 100; ++i)
    {
        size_t size = 4096;

        void * buf = malloc(size);
        if (!buf)
            abort();
        memset(buf, 0, size);

        while (size < 1048576)
        {
            size_t next_size = size * 4;

            free(buf);
            void * new_buf = malloc(next_size);
            if (!new_buf)
                abort();
            buf = new_buf;

            memset(reinterpret_cast<char*>(buf) + size, 0, next_size - size);
            size = next_size;
        }

        free(buf);
    }
}

void thread_func()
{
    for (size_t i = 0; i < 1000; ++i)
    {
        alloc_loop();
    }
}


int main(int, char **)
{
    std::vector<std::thread> threads(16);
    for (auto & thread : threads)
        thread = std::thread(thread_func);
    for (auto & thread : threads)
        thread.join();
    return 0;
}
