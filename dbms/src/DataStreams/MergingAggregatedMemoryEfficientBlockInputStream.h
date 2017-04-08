#pragma once

#include <Interpreters/Aggregator.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <condition_variable>


class MemoryTracker;

namespace DB
{


/** Доагрегирует потоки блоков, держа в оперативной памяти только по одному или несколько (до merging_threads) блоков из каждого источника.
  * Это экономит оперативку в случае использования двухуровневой агрегации, где в каждом источнике будет до 256 блоков с частями результата.
  *
  * Агрегатные функции в блоках не должны быть финализированы, чтобы их состояния можно было объединить.
  *
  * Используется для решения двух задач:
  *
  * 1. Внешняя агрегация со сбросом данных на диск.
  * Частично агрегированные данные (предварительно разбитые на 256 корзин) сброшены в какое-то количество файлов на диске.
  * Нужно читать их и мерджить по корзинам - держа в оперативке одновременно только несколько корзин из каждого файла.
  *
  * 2. Слияние результатов агрегации при распределённой обработке запроса.
  * С разных серверов приезжают частично агрегированные данные, которые могут быть разбиты, а могут быть не разбиты на 256 корзин,
  *  и эти корзины отдаются нам по сети с каждого сервера последовательно, друг за другом.
  * Надо так же читать и мерджить по корзинам.
  *
  * Суть работы:
  *
  * Есть какое-то количество источников. Они отдают блоки с частично агрегированными данными.
  * Каждый источник может отдать одну из следующих последовательностей блоков:
  * 1. "неразрезанный" блок с bucket_num = -1;
  * 2. "разрезанные" (two_level) блоки с bucket_num от 0 до 255;
  * В обоих случаях, может ещё присутствовать блок "переполнений" (overflows) с bucket_num = -1 и is_overflows = true;
  *
  * Исходим из соглашения, что разрезанные блоки всегда передаются в порядке bucket_num.
  * То есть, если a < b, то блок с bucket_num = a идёт раньше bucket_num = b.
  * Это нужно для экономного по памяти слияния
  * - чтобы не надо было читать блоки наперёд, а идти по всем последовательностям по возрастанию bucket_num.
  *
  * При этом, не все bucket_num из диапазона 0..255 могут присутствовать.
  * Блок переполнений может присутствовать в любом порядке относительно других блоков (но он может быть только один).
  *
  * Необходимо объединить эти последовательности блоков и отдать результат в виде последовательности с такими же свойствами.
  * То есть, на выходе, если в последовательности есть "разрезанные" блоки, то они должны идти в порядке bucket_num.
  *
  * Мердж можно осуществлять с использованием нескольких (merging_threads) потоков.
  * Для этого, получение набора блоков для следующего bucket_num надо делать последовательно,
  *  а затем, когда мы имеем несколько полученных наборов, их объединение можно делать параллельно.
  *
  * При получении следующих блоков из разных источников,
  *  данные из источников можно также читать в несколько потоков (reading_threads)
  *  для оптимальной работы при наличии быстрой сети или дисков (откуда эти блоки читаются).
  */
class MergingAggregatedMemoryEfficientBlockInputStream : public IProfilingBlockInputStream
{
public:
    MergingAggregatedMemoryEfficientBlockInputStream(
        BlockInputStreams inputs_, const Aggregator::Params & params, bool final_,
        size_t reading_threads_, size_t merging_threads_);

    ~MergingAggregatedMemoryEfficientBlockInputStream() override;

    String getName() const override { return "MergingAggregatedMemoryEfficient"; }

    String getID() const override;

    /// Отправляет запрос (инициирует вычисления) раньше, чем read.
    void readPrefix() override;

    /// Вызывается либо после того, как всё прочитано, либо после cancel-а.
    void readSuffix() override;

    /** Отличается от реализации по-умолчанию тем, что пытается остановить все источники,
      *  пропуская отвалившиеся по эксепшену.
      */
    void cancel() override;

protected:
    Block readImpl() override;

private:
    static constexpr size_t NUM_BUCKETS = 256;

    Aggregator aggregator;
    bool final;
    size_t reading_threads;
    size_t merging_threads;

    bool started = false;
    bool all_read = false;
    std::atomic<bool> has_two_level {false};
    std::atomic<bool> has_overflows {false};
    int current_bucket_num = -1;

    Logger * log = &Logger::get("MergingAggregatedMemoryEfficientBlockInputStream");


    struct Input
    {
        BlockInputStreamPtr stream;
        Block block;
        Block overflow_block;
        std::vector<Block> splitted_blocks;
        bool is_exhausted = false;

        Input(BlockInputStreamPtr & stream_) : stream(stream_) {}
    };

    std::vector<Input> inputs;

    using BlocksToMerge = std::unique_ptr<BlocksList>;

    void start();

    /// Получить блоки, которые можно мерджить. Это позволяет мерджить их параллельно в отдельных потоках.
    BlocksToMerge getNextBlocksToMerge();

    std::unique_ptr<ThreadPool> reading_pool;

    /// Для параллельного мерджа.

    struct ParallelMergeData
    {
        ThreadPool pool;

        /// Сейчас один из мерджащих потоков получает следующие блоки для мерджа. Эта операция должна делаться последовательно.
        std::mutex get_next_blocks_mutex;

        std::atomic<bool> exhausted {false};    /// No more source data.
        std::atomic<bool> finish {false};        /// Need to terminate early.

        std::exception_ptr exception;
        /// Следует отдавать блоки стого в порядке ключа (bucket_num).
        /// Если значение - пустой блок - то нужно дождаться его мерджа.
        /// (Такое значение означает обещание, что здесь будут данные. Это важно, потому что данные нужно отдавать в порядке ключа - bucket_num)
        std::map<int, Block> merged_blocks;
        std::mutex merged_blocks_mutex;
        /// Событие, с помощью которого мерджащие потоки говорят главному потоку, что новый блок готов.
        std::condition_variable merged_blocks_changed;
        /// Событие, с помощью которого главный поток говорят мерджащим потокам, что можно обработать следующую группу блоков.
        std::condition_variable have_space;

        ParallelMergeData(size_t max_threads) : pool(max_threads) {}
    };

    std::unique_ptr<ParallelMergeData> parallel_merge_data;

    void mergeThread(MemoryTracker * memory_tracker);

    void finalize();
};

}
