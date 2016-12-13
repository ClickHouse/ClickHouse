#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/PKCondition.h>


namespace DB
{

class MergeTreeReader;
class MergeTreeReadPool;
struct MergeTreeReadTask;
class UncompressedCache;
class MarkCache;


/** Used in conjunction with MergeTreeReadPool, asking it for more work to do and performing whatever reads it is asked
  * to perform.
  */
class MergeTreeThreadBlockInputStream : public IProfilingBlockInputStream
{
	/// "thread" index (there are N threads and each thread is assigned index in interval [0..N-1])
	std::size_t thread;
public:
	MergeTreeThreadBlockInputStream(
		const std::size_t thread,
		const std::shared_ptr<MergeTreeReadPool> & pool, const std::size_t min_marks_to_read, const std::size_t block_size,
		MergeTreeData & storage, const bool use_uncompressed_cache, const ExpressionActionsPtr & prewhere_actions,
		const String & prewhere_column, const Settings & settings, const Names & virt_column_names);

    ~MergeTreeThreadBlockInputStream() override;

	String getName() const override { return "MergeTreeThread"; }

	String getID() const override;

protected:
	/// Будем вызывать progressImpl самостоятельно.
	void progress(const Progress & value) override {}

	Block readImpl() override;

private:
	/// Requests read task from MergeTreeReadPool and signals whether it got one
	bool getNewTask();
	Block readFromPart();

	void injectVirtualColumns(Block & block);

	std::shared_ptr<MergeTreeReadPool> pool;
	const std::size_t block_size_marks;
	const std::size_t min_marks_to_read;
	MergeTreeData & storage;
	const bool use_uncompressed_cache;
	ExpressionActionsPtr prewhere_actions;
	const String prewhere_column;
	const std::size_t min_bytes_to_use_direct_io;
	const std::size_t max_read_buffer_size;
	const Names virt_column_names;

	Logger * log;

	using MergeTreeReaderPtr = std::unique_ptr<MergeTreeReader>;

	std::shared_ptr<UncompressedCache> owned_uncompressed_cache;
	std::shared_ptr<MarkCache> owned_mark_cache;

	std::shared_ptr<MergeTreeReadTask> task;
	MergeTreeReaderPtr reader;
	MergeTreeReaderPtr pre_reader;
};


}
