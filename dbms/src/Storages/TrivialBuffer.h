#pragma once

#include <mutex>
#include <thread>
#include <ext/shared_ptr_helper.hpp>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Poco/Event.h>

namespace Poco { class Logger; }

namespace DB
{

class Context;

/** При вставке буферизует входящие блоки, пока не превышены некоторые пороги.
  * Когда пороги превышены - отправляет блоки в другую таблицу в том же порядке,
  * в котором они пришли в данную таблицу.
  *
  * Пороги проверяются при вставке, а также, периодически, в фоновом потоке
  * (чтобы реализовать пороги по времени).
  * Если в таблицу вставляется блок, который сам по себе превышает max-пороги, то он
  * записывается сразу в подчинённую таблицу без буферизации.
  * Пороги могут быть превышены. Например, если max_rows = 1 000 000, в буфере уже было
  * 500 000 строк, и добавляется кусок из 800 000 строк, то в буфере окажется 1 300 000 строк,
  * и затем такой блок будет записан в подчинённую таблицу
  *
  * При уничтожении таблицы типа TrivialBuffer и при завершении работы, все данные сбрасываются.
  * Данные в буфере не реплицируются, не логгируются на диск, не индексируются. При грубом
  * перезапуске сервера, данные пропадают.
  */
class TrivialBuffer : private ext::shared_ptr_helper<TrivialBuffer>, public IStorage
{
friend class ext::shared_ptr_helper<TrivialBuffer>;
friend class TrivialBufferBlockInputStream;
friend class TrivialBufferBlockOutputStream;

public:
    /// Пороги.
    struct Thresholds
    {
        time_t time;    /// Количество секунд от момента вставки первой строчки в блок.
        size_t rows;    /// Количество строк в блоке.
        size_t bytes;    /// Количество (несжатых) байт в блоке.
    };

    static StoragePtr create(const std::string & name_, NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        Context & context_, size_t num_blocks_to_deduplicate_,
        const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
        const String & destination_database_, const String & destination_table_);

    std::string getName() const override { return "TrivialBuffer"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

    bool checkThresholds(const time_t current_time, const size_t additional_rows = 0,
                const size_t additional_bytes = 0) const;
    bool checkThresholdsImpl(const size_t rows, const size_t bytes,
                const time_t time_passed) const;

    /// Сбрасывает все буферы в подчинённую таблицу.
    void shutdown() override;
    bool optimize(const String & partition, bool final, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name,
            const String & new_table_name) override { name = new_table_name; }

    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsParallelReplicas() const override { return true; }

    /// Структура подчинённой таблицы не проверяется и не изменяется.
    void alter(const AlterCommands & params, const String & database_name,
            const String & table_name, const Context & context) override;

private:
    String name;
    NamesAndTypesListPtr columns;

    Context & context;

    std::mutex mutex;

    BlocksList data;

    size_t current_rows = 0;
    size_t current_bytes = 0;
    time_t first_write_time = 0;
    const size_t num_blocks_to_deduplicate;
    using HashType = UInt64;
    using DeduplicationBuffer = std::unordered_set<HashType>;
    /// Вставка хэшей новый блоков идет в current_hashes, lookup - в
    /// обоих set'ах. Когда current_hashes переполняется, current сбрасывается
    /// в previous, а в current создается новый set.
    std::unique_ptr<DeduplicationBuffer> current_hashes, previous_hashes;
    const Thresholds min_thresholds;
    const Thresholds max_thresholds;

    const String destination_database;
    const String destination_table;
    /// Если задано - не записывать данные из буфера, а просто опустошать буфер.
    bool no_destination;

    Poco::Logger * log;

    Poco::Event shutdown_event;
    /// Выполняет сброс данных по таймауту.
    std::thread flush_thread;

    TrivialBuffer(const std::string & name_, NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        Context & context_, size_t num_blocks_to_deduplicate_,
        const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
        const String & destination_database_, const String & destination_table_);

    void addBlock(const Block & block);
    /// Аргумент table передаётся, так как иногда вычисляется заранее. Он должен
    /// соответствовать destination-у.
    void writeBlockToDestination(const Block & block, StoragePtr table);


    void flush(bool check_thresholds = true);
    void flushThread();
};

}
