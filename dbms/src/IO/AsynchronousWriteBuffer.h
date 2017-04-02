#pragma once

#include <math.h>

#include <vector>

#include <Common/ThreadPool.h>
#include <IO/WriteBuffer.h>


namespace DB
{


/** Записывает данные асинхронно с помощью двойной буферизации.
  */
class AsynchronousWriteBuffer : public WriteBuffer
{
private:
    WriteBuffer & out;                /// Основной буфер, отвечает за запись данных.
    std::vector<char> memory;        /// Кусок памяти для дублирования буфера.
    ThreadPool pool;                /// Для асинхронной записи данных.
    bool started;                    /// Была ли запущена асинхронная запись данных.

    /// Менять местами основной и дублирующий буферы.
    void swapBuffers()
    {
        buffer().swap(out.buffer());
        std::swap(position(), out.position());
    }

    void nextImpl() override
    {
        if (!offset())
            return;

        if (started)
            pool.wait();
        else
            started = true;

        swapBuffers();

        /// Данные будут записываться в отельном потоке.
        pool.schedule([this] { thread(); });
    }

public:
    AsynchronousWriteBuffer(WriteBuffer & out_) : WriteBuffer(nullptr, 0), out(out_), memory(out.buffer().size()), pool(1), started(false)
    {
        /// Данные пишутся в дублирующий буфер.
        set(memory.data(), memory.size());
    }

    ~AsynchronousWriteBuffer() override
    {
        try
        {
            if (started)
                pool.wait();

            swapBuffers();
            out.next();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /// То, что выполняется в отдельном потоке
    void thread()
    {
        out.next();
    }
};

}
