#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class ProcessListEntry;

struct BlockIO
{
    /** process_list_entry should be destroyed after in and after out,
      *  since in and out contain pointer to an object inside process_list_entry
      *  (MemoryTracker * current_memory_tracker),
      *  which could be used before destroying of in and out.
      */
    std::shared_ptr<ProcessListEntry> process_list_entry;

    BlockInputStreamPtr in;
    BlockOutputStreamPtr out;

    Block in_sample;    /// Пример блока, который будет прочитан из in.
    Block out_sample;    /// Пример блока, которого нужно писать в out.

    /// Callbacks for query logging could be set here.
     std::function<void(IBlockInputStream *, IBlockOutputStream *)>    finish_callback;
    std::function<void()>                                             exception_callback;

    /// Вызывайте эти функции, если нужно логгировать запрос.
    void onFinish()
    {
        if (finish_callback)
            finish_callback(in.get(), out.get());
    }

    void onException()
    {
        if (exception_callback)
            exception_callback();
    }

    BlockIO & operator= (const BlockIO & rhs)
    {
        /// Обеспечиваем правильный порядок уничтожения.
        out                 = nullptr;
        in                     = nullptr;
        process_list_entry     = nullptr;

        process_list_entry     = rhs.process_list_entry;
        in                     = rhs.in;
        out                 = rhs.out;
        in_sample             = rhs.in_sample;
        out_sample             = rhs.out_sample;

        finish_callback        = rhs.finish_callback;
        exception_callback    = rhs.exception_callback;

        return *this;
    }

    ~BlockIO();
};

}
