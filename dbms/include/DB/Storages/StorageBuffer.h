#pragma once

#include <mutex>
#include <thread>

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/IStorage.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** При вставке, буферизует данные в оперативке, пока не превышены некоторые пороги.
  * Когда пороги превышены - сбрасывает данные в другую таблицу.
  * При чтении, читает как из своих буферов, так и из подчинённой таблицы.
  *
  * Буфер представляет собой набор из num_shards блоков.
  * При записи, выбирается номер блока по остатку от деления ThreadNumber на num_shards (или один из других),
  *  и в соответствующий блок добавляются строчки.
  * При использовании блока, он блокируется некоторым mutex-ом. Если при записи, соответствующий блок уже занят
  *  - пробуем заблокировать следующий по кругу блок, и так не более num_shards раз (далее блокируемся).
  * Пороги проверяются при вставке, а также, периодически, в фоновом потоке (чтобы реализовать пороги по времени).
  * Пороги действуют независимо для каждого shard-а. Каждый shard может быть сброшен независимо от других.
  * Если в таблицу вставляется блок, который сам по себе превышает max-пороги, то он записывается сразу в подчинённую таблицу без буферизации.
  * Пороги могут быть превышены. Например, если max_rows = 1 000 000, в буфере уже было 500 000 строк,
  *  и добавляется кусок из 800 000 строк, то в буфере окажется 1 300 000 строк, и затем такой блок будет записан в подчинённую таблицу
  *
  * При уничтожении таблицы типа Buffer и при завершении работы, все данные сбрасываются.
  * Данные в буфере не реплицируются, не логгируются на диск, не индексируются. При грубом перезапуске сервера, данные пропадают.
  */
class StorageBuffer : public IStorage
{
friend class BufferBlockInputStream;
friend class BufferBlockOutputStream;

public:
	/// Пороги.
	struct Thresholds
	{
		time_t time;	/// Количество секунд от момента вставки первой строчки в блок.
		size_t rows;	/// Количество строк в блоке.
		size_t bytes;	/// Количество (несжатых) байт в блоке.
	};

	/** num_shards - уровень внутреннего параллелизма (количество независимых буферов)
	  * Буфер сбрасывается, если превышены все минимальные пороги или хотя бы один из максимальных.
	  */
	static StoragePtr create(const std::string & name_, NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		Context & context_,
		size_t num_shards_, const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
		const String & destination_database_, const String & destination_table_);

	std::string getName() const override { return "Buffer"; }
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

	BlockOutputStreamPtr write(ASTPtr query) override;

	/// Сбрасывает все буферы в подчинённую таблицу.
	void shutdown() override;
	bool optimize(const Settings & settings) override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override { name = new_table_name; }

	bool supportsSampling() const override { return true; }
	bool supportsPrewhere() const override { return true; }
	bool supportsFinal() const override { return true; }
	bool supportsIndexForIn() const override { return true; }
	bool supportsParallelReplicas() const override { return true; }

	/// Структура подчинённой таблицы не проверяется и не изменяется.
	void alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context) override;

private:
	String name;
	NamesAndTypesListPtr columns;

	Context & context;

	struct Buffer
	{
		time_t first_write_time = 0;
		Block data;
		std::mutex mutex;
	};

	/// Имеется num_shards независимых буферов.
	const size_t num_shards;
	std::vector<Buffer> buffers;

	const Thresholds min_thresholds;
	const Thresholds max_thresholds;

	const String destination_database;
	const String destination_table;
	bool no_destination;	/// Если задано - не записывать данные из буфера, а просто опустошать буфер.

	Logger * log;

	Poco::Event shutdown_event;
	/// Выполняет сброс данных по таймауту.
	std::thread flush_thread;

	StorageBuffer(const std::string & name_, NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		Context & context_,
		size_t num_shards_, const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
		const String & destination_database_, const String & destination_table_);

	void flushAllBuffers(bool check_thresholds = true);
	/// Сбросить буфер. Если выставлено check_thresholds - сбрасывает только если превышены пороги.
	void flushBuffer(Buffer & buffer, bool check_thresholds);
	bool checkThresholds(Buffer & buffer, time_t current_time, size_t additional_rows = 0, size_t additional_bytes = 0);

	/// Аргумент table передаётся, так как иногда вычисляется заранее. Он должен соответствовать destination-у.
	void writeBlockToDestination(const Block & block, StoragePtr table);

	void flushThread();
};

}
