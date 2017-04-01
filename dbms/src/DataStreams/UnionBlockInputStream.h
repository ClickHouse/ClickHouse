#pragma once

#include <common/logger_useful.h>

#include <Common/ConcurrentBoundedQueue.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ParallelInputsProcessor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{

template <StreamUnionMode mode>
struct OutputData;

/// Блок или эксепшен.
template <>
struct OutputData<StreamUnionMode::Basic>
{
    Block block;
    std::exception_ptr exception;

    OutputData() {}
    OutputData(Block & block_) : block(block_) {}
    OutputData(std::exception_ptr & exception_) : exception(exception_) {}
};

/// Блок + дополнительнцю информацию или эксепшен.
template <>
struct OutputData<StreamUnionMode::ExtraInfo>
{
    Block block;
    BlockExtraInfo extra_info;
    std::exception_ptr exception;

    OutputData() {}
    OutputData(Block & block_, BlockExtraInfo & extra_info_) : block(block_), extra_info(extra_info_) {}
    OutputData(std::exception_ptr & exception_) : exception(exception_) {}
};

}

/** Объединяет несколько источников в один.
  * Блоки из разных источников перемежаются друг с другом произвольным образом.
  * Можно указать количество потоков (max_threads),
  *  в которых будет выполняться получение данных из разных источников.
  *
  * Устроено так:
  * - с помощью ParallelInputsProcessor в нескольких потоках вынимает из источников блоки;
  * - полученные блоки складываются в ограниченную очередь готовых блоков;
  * - основной поток вынимает готовые блоки из очереди готовых блоков;
  * - если указан режим StreamUnionMode::ExtraInfo, в дополнение к блокам UnionBlockInputStream
  *   вынимает информацию о блоках; в таком случае все источники должны поддержать такой режим.
  */

template <StreamUnionMode mode = StreamUnionMode::Basic>
class UnionBlockInputStream : public IProfilingBlockInputStream
{
public:
    using ExceptionCallback = std::function<void()>;

private:
    using Self = UnionBlockInputStream<mode>;

public:
    UnionBlockInputStream(BlockInputStreams inputs, BlockInputStreamPtr additional_input_at_end, size_t max_threads,
        ExceptionCallback exception_callback_ = ExceptionCallback()) :
        output_queue(std::min(inputs.size(), max_threads)),
        handler(*this),
        processor(inputs, additional_input_at_end, max_threads, handler),
        exception_callback(exception_callback_)
    {
        children = inputs;
        if (additional_input_at_end)
            children.push_back(additional_input_at_end);
    }

    String getName() const override { return "Union"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Union(";

        Strings children_ids(children.size());
        for (size_t i = 0; i < children.size(); ++i)
            children_ids[i] = children[i]->getID();

        /// Порядок не имеет значения.
        std::sort(children_ids.begin(), children_ids.end());

        for (size_t i = 0; i < children_ids.size(); ++i)
            res << (i == 0 ? "" : ", ") << children_ids[i];

        res << ")";
        return res.str();
    }


    ~UnionBlockInputStream() override
    {
        try
        {
            if (!all_read)
                cancel();

            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /** Отличается от реализации по-умолчанию тем, что пытается остановить все источники,
      *  пропуская отвалившиеся по эксепшену.
      */
    void cancel() override
    {
        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
            return;

        //std::cerr << "cancelling\n";
        processor.cancel();
    }

    BlockExtraInfo getBlockExtraInfo() const override
    {
        return doGetBlockExtraInfo();
    }

protected:
    void finalize()
    {
        if (!started)
            return;

        LOG_TRACE(log, "Waiting for threads to finish");

        std::exception_ptr exception;
        if (!all_read)
        {
            /** Прочитаем всё до конца, чтобы ParallelInputsProcessor не заблокировался при попытке вставить в очередь.
              * Может быть, в очереди есть ещё эксепшен.
              */
            OutputData<mode> res;
            while (true)
            {
                //std::cerr << "popping\n";
                output_queue.pop(res);

                if (res.exception)
                {
                    if (!exception)
                        exception = res.exception;
                    else if (Exception * e = exception_cast<Exception *>(exception))
                        e->addMessage("\n" + getExceptionMessage(res.exception, false));
                }
                else if (!res.block)
                    break;
            }

            all_read = true;
        }

        processor.wait();

        LOG_TRACE(log, "Waited for threads to finish");

        if (exception)
            std::rethrow_exception(exception);
    }

    /// Ничего не делаем, чтобы подготовка к выполнению запроса делалась параллельно, в ParallelInputsProcessor.
    void readPrefix() override
    {
    }

    /** Возможны следующие варианты:
      * 1. Функция readImpl вызывается до тех пор, пока она не вернёт пустой блок.
      *  Затем вызывается функция readSuffix и затем деструктор.
      * 2. Вызывается функция readImpl. В какой-то момент, возможно из другого потока вызывается функция cancel.
      *  Затем вызывается функция readSuffix и затем деструктор.
      * 3. В любой момент, объект может быть и так уничтожен (вызываться деструктор).
      */

    Block readImpl() override
    {
        if (all_read)
            return received_payload.block;

        /// Запускаем потоки, если это ещё не было сделано.
        if (!started)
        {
            started = true;
            processor.process();
        }

        /// Будем ждать, пока будет готов следующий блок или будет выкинуто исключение.
        //std::cerr << "popping\n";
        output_queue.pop(received_payload);

        if (received_payload.exception)
        {
            if (exception_callback)
                exception_callback();
            std::rethrow_exception(received_payload.exception);
        }

        if (!received_payload.block)
            all_read = true;

        return received_payload.block;
    }

    /// Вызывается либо после того, как всё прочитано, либо после cancel-а.
    void readSuffix() override
    {
        //std::cerr << "readSuffix\n";
        if (!all_read && !is_cancelled.load(std::memory_order_seq_cst))
            throw Exception("readSuffix called before all data is read", ErrorCodes::LOGICAL_ERROR);

        finalize();

        for (size_t i = 0; i < children.size(); ++i)
            children[i]->readSuffix();
    }

private:
    template<StreamUnionMode mode2 = mode>
    BlockExtraInfo doGetBlockExtraInfo(typename std::enable_if<mode2 == StreamUnionMode::ExtraInfo>::type * = nullptr) const
    {
        return received_payload.extra_info;
    }

    template<StreamUnionMode mode2 = mode>
    BlockExtraInfo doGetBlockExtraInfo(typename std::enable_if<mode2 == StreamUnionMode::Basic>::type * = nullptr) const
    {
        throw Exception("Method getBlockExtraInfo is not supported for mode StreamUnionMode::Basic",
            ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    using Payload = OutputData<mode>;
    using OutputQueue = ConcurrentBoundedQueue<Payload>;

private:
    /** Очередь готовых блоков. Также туда можно положить эксепшен вместо блока.
      * Когда данные закончатся - в очередь вставляется пустой блок.
      * В очередь всегда (даже после исключения или отмены запроса) рано или поздно вставляется пустой блок.
      * Очередь всегда (даже после исключения или отмены запроса, даже в деструкторе) нужно дочитывать до пустого блока,
      *  иначе ParallelInputsProcessor может заблокироваться при вставке в очередь.
      */
    OutputQueue output_queue;

    struct Handler
    {
        Handler(Self & parent_) : parent(parent_) {}

        template <StreamUnionMode mode2 = mode>
        void onBlock(Block & block, size_t thread_num,
            typename std::enable_if<mode2 == StreamUnionMode::Basic>::type * = nullptr)
        {
            //std::cerr << "pushing block\n";
            parent.output_queue.push(Payload(block));
        }

        template <StreamUnionMode mode2 = mode>
        void onBlock(Block & block, BlockExtraInfo & extra_info, size_t thread_num,
            typename std::enable_if<mode2 == StreamUnionMode::ExtraInfo>::type * = nullptr)
        {
            //std::cerr << "pushing block with extra info\n";
            parent.output_queue.push(Payload(block, extra_info));
        }

        void onFinish()
        {
            //std::cerr << "pushing end\n";
            parent.output_queue.push(Payload());
        }

        void onFinishThread(size_t thread_num)
        {
        }

        void onException(std::exception_ptr & exception, size_t thread_num)
        {
            //std::cerr << "pushing exception\n";

            /// Порядок строк имеет значение. Если его поменять, то возможна ситуация,
            ///  когда перед эксепшеном, в очередь окажется вставлен пустой блок (конец данных),
            ///  и эксепшен потеряется.

            parent.output_queue.push(exception);
            parent.cancel();    /// Не кидает исключений.
        }

        Self & parent;
    };

    Handler handler;
    ParallelInputsProcessor<Handler, mode> processor;

    ExceptionCallback exception_callback;

    Payload received_payload;

    bool started = false;
    bool all_read = false;

    Logger * log = &Logger::get("UnionBlockInputStream");
};

}
