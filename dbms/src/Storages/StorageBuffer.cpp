#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Databases/IDatabase.h>
#include <DB/Storages/StorageBuffer.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/CurrentMetrics.h>
#include <Poco/Ext/ThreadNumber.h>

#include <ext/range.hpp>


namespace ProfileEvents
{
	extern const Event StorageBufferErrorOnFlush;
}

namespace CurrentMetrics
{
	extern const Metric StorageBufferRows;
	extern const Metric StorageBufferBytes;
}


namespace DB
{

namespace ErrorCodes
{
	extern const int INFINITE_LOOP;
	extern const int BLOCKS_HAS_DIFFERENT_STRUCTURE;
}


StoragePtr StorageBuffer::create(const std::string & name_, NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	Context & context_,
	size_t num_shards_, const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
	const String & destination_database_, const String & destination_table_)
{
	return make_shared(
		name_, columns_, materialized_columns_, alias_columns_, column_defaults_,
		context_, num_shards_, min_thresholds_, max_thresholds_, destination_database_, destination_table_);
}


StorageBuffer::StorageBuffer(const std::string & name_, NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	Context & context_,
	size_t num_shards_, const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
	const String & destination_database_, const String & destination_table_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	name(name_), columns(columns_), context(context_),
	num_shards(num_shards_), buffers(num_shards_),
	min_thresholds(min_thresholds_), max_thresholds(max_thresholds_),
	destination_database(destination_database_), destination_table(destination_table_),
	no_destination(destination_database.empty() && destination_table.empty()),
	log(&Logger::get("StorageBuffer (" + name + ")")),
	flush_thread(&StorageBuffer::flushThread, this)
{
}


/// Читает из одного буфера (из одного блока) под его mutex-ом.
class BufferBlockInputStream : public IProfilingBlockInputStream
{
public:
	BufferBlockInputStream(const Names & column_names_, StorageBuffer::Buffer & buffer_)
		: column_names(column_names_.begin(), column_names_.end()), buffer(buffer_) {}

	String getName() const { return "Buffer"; }

	String getID() const
	{
		std::stringstream res;
		res << "Buffer(" << &buffer;

		for (const auto & name : column_names)
			res << ", " << name;

		res << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		Block res;

		if (has_been_read)
			return res;
		has_been_read = true;

		std::lock_guard<std::mutex> lock(buffer.mutex);

		if (!buffer.data.rowsInFirstColumn())
			return res;

		for (const auto & name : column_names)
		{
			auto & col = buffer.data.getByName(name);
			res.insert(ColumnWithTypeAndName(col.column->clone(), col.type, name));
		}

		return res;
	}

private:
	NameSet column_names;
	StorageBuffer::Buffer & buffer;
	bool has_been_read = false;
};


BlockInputStreams StorageBuffer::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	processed_stage = QueryProcessingStage::FetchColumns;

	BlockInputStreams streams_from_dst;

	if (!no_destination)
	{
		auto destination = context.getTable(destination_database, destination_table);

		if (destination.get() == this)
			throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

		/** Отключаем оптимизацию "перенос в PREWHERE",
		  *  так как Buffer не поддерживает PREWHERE.
		  */
		Settings modified_settings = settings;
		modified_settings.optimize_move_to_prewhere = false;

		streams_from_dst = destination->read(column_names, query, context, modified_settings, processed_stage, max_block_size, threads);
	}

	BlockInputStreams streams_from_buffers;
	streams_from_buffers.reserve(num_shards);
	for (auto & buf : buffers)
		streams_from_buffers.push_back(std::make_shared<BufferBlockInputStream>(column_names, buf));

	/** Если источники из таблицы были обработаны до какой-то не начальной стадии выполнения запроса,
	  * то тогда источники из буферов надо тоже обернуть в конвейер обработки до той же стадии.
	  */
	if (processed_stage > QueryProcessingStage::FetchColumns)
		for (auto & stream : streams_from_buffers)
			stream = InterpreterSelectQuery(query, context, processed_stage, 0, stream).execute().in;

	streams_from_dst.insert(streams_from_dst.end(), streams_from_buffers.begin(), streams_from_buffers.end());
	return streams_from_dst;
}


static void appendBlock(const Block & from, Block & to)
{
	if (!to)
		throw Exception("Cannot append to empty block", ErrorCodes::LOGICAL_ERROR);

	size_t rows = from.rows();
	size_t bytes = from.bytes();

	CurrentMetrics::add(CurrentMetrics::StorageBufferRows, rows);
	CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, bytes);

	size_t old_rows = to.rows();

	try
	{
		for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
		{
			const IColumn & col_from = *from.getByPosition(column_no).column.get();
			IColumn & col_to = *to.getByPosition(column_no).column.get();

			if (col_from.getName() != col_to.getName())
				throw Exception("Cannot append block to another: different type of columns at index " + toString(column_no)
					+ ". Block 1: " + from.dumpStructure() + ". Block 2: " + to.dumpStructure(), ErrorCodes::BLOCKS_HAS_DIFFERENT_STRUCTURE);

			col_to.insertRangeFrom(col_from, 0, rows);
		}
	}
	catch (...)
	{
		/// Rollback changes.
		try
		{
			for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
			{
				ColumnPtr & col_to = to.getByPosition(column_no).column;
				if (col_to->size() != old_rows)
					col_to = col_to->cut(0, old_rows);
			}
		}
		catch (...)
		{
			/// In case when we cannot rollback, do not leave incorrect state in memory.
			std::terminate();
		}

		throw;
	}
}


class BufferBlockOutputStream : public IBlockOutputStream
{
public:
	BufferBlockOutputStream(StorageBuffer & storage_) : storage(storage_) {}

	void write(const Block & block) override
	{
		if (!block)
			return;

		size_t rows = block.rowsInFirstColumn();
		if (!rows)
			return;

		StoragePtr destination;
		if (!storage.no_destination)
		{
			destination = storage.context.tryGetTable(storage.destination_database, storage.destination_table);

			if (destination)
			{
				if (destination.get() == &storage)
					throw Exception("Destination table is myself. Write will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

				/// Проверяем структуру таблицы.
				try
				{
					destination->check(block, true);
				}
				catch (Exception & e)
				{
					e.addMessage("(when looking at destination table " + storage.destination_database + "." + storage.destination_table + ")");
					throw;
				}
			}
		}

		size_t bytes = block.bytes();

		/// Если блок уже превышает максимальные ограничения, то пишем минуя буфер.
		if (rows > storage.max_thresholds.rows || bytes > storage.max_thresholds.bytes)
		{
			if (!storage.no_destination)
			{
				LOG_TRACE(storage.log, "Writing block with " << rows << " rows, " << bytes << " bytes directly.");
				storage.writeBlockToDestination(block, destination);
 			}
			return;
		}

		/// Распределяем нагрузку по шардам по номеру потока.
		const auto start_shard_num = Poco::ThreadNumber::get() % storage.num_shards;

		/// Перебираем буферы по кругу, пытаясь заблокировать mutex. Не более одного круга.
		auto shard_num = start_shard_num;
		size_t try_no = 0;
		for (; try_no != storage.num_shards; ++try_no)
		{
			std::unique_lock<std::mutex> lock(storage.buffers[shard_num].mutex, std::try_to_lock_t());
			if (lock.owns_lock())
			{
				insertIntoBuffer(block, storage.buffers[shard_num], std::move(lock));
				break;
			}

			++shard_num;
			if (shard_num == storage.num_shards)
				shard_num = 0;
		}

		/// Если так и не удалось ничего сразу заблокировать, то будем ждать на mutex-е.
		if (try_no == storage.num_shards)
			insertIntoBuffer(block, storage.buffers[start_shard_num], std::unique_lock<std::mutex>(storage.buffers[start_shard_num].mutex));
	}
private:
	StorageBuffer & storage;

	void insertIntoBuffer(const Block & block, StorageBuffer::Buffer & buffer, std::unique_lock<std::mutex> && lock)
	{
		time_t current_time = time(0);

		/// Сортируем столбцы в блоке. Это нужно, чтобы было проще потом конкатенировать блоки.
		Block sorted_block = block.sortColumns();

		if (!buffer.data)
		{
			buffer.data = sorted_block.cloneEmpty();
		}
		else if (storage.checkThresholds(buffer, current_time, sorted_block.rowsInFirstColumn(), sorted_block.bytes()))
		{
			/** Если после вставки в буфер, ограничения будут превышены, то будем сбрасывать буфер.
			  * Это также защищает от неограниченного потребления оперативки, так как в случае невозможности записать в таблицу,
			  *  будет выкинуто исключение, а новые данные не будут добавлены в буфер.
			  */

			lock.unlock();
			storage.flushBuffer(buffer, true);
			lock.lock();
		}

		if (!buffer.first_write_time)
			buffer.first_write_time = current_time;

		appendBlock(sorted_block, buffer.data);
	}
};


BlockOutputStreamPtr StorageBuffer::write(ASTPtr query, const Settings & settings)
{
	return std::make_shared<BufferBlockOutputStream>(*this);
}


void StorageBuffer::shutdown()
{
	shutdown_event.set();

	if (flush_thread.joinable())
		flush_thread.join();

	try
	{
		optimize({}, {}, context.getSettings());
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}


/** NOTE If you do OPTIMIZE after insertion,
  * it does not guarantee, that all data will be in destination table at the time of next SELECT just after OPTIMIZE.
  *
  * Because in case if there was already running flushBuffer method,
  *  then call to flushBuffer inside OPTIMIZE will see empty buffer and return quickly,
  *  but at the same time, the already running flushBuffer method possibly is not finished,
  *  so next SELECT will observe missing data.
  *
  * This kind of race condition make very hard to implement proper tests.
  */
bool StorageBuffer::optimize(const String & partition, bool final, const Settings & settings)
{
	if (!partition.empty())
		throw Exception("Partition cannot be specified when optimizing table of type Buffer", ErrorCodes::NOT_IMPLEMENTED);

	if (final)
		throw Exception("FINAL cannot be specified when optimizing table of type Buffer", ErrorCodes::NOT_IMPLEMENTED);

	flushAllBuffers(false);
	return true;
}


bool StorageBuffer::checkThresholds(const Buffer & buffer, time_t current_time, size_t additional_rows, size_t additional_bytes) const
{
	time_t time_passed = 0;
	if (buffer.first_write_time)
		time_passed = current_time - buffer.first_write_time;

	size_t rows = buffer.data.rowsInFirstColumn() + additional_rows;
	size_t bytes = buffer.data.bytes() + additional_bytes;

	return checkThresholdsImpl(rows, bytes, time_passed);
}


bool StorageBuffer::checkThresholdsImpl(size_t rows, size_t bytes, time_t time_passed) const
{
	return
	       (time_passed > min_thresholds.time && rows > min_thresholds.rows && bytes > min_thresholds.bytes)
		|| (time_passed > max_thresholds.time || rows > max_thresholds.rows || bytes > max_thresholds.bytes);
}


void StorageBuffer::flushAllBuffers(const bool check_thresholds)
{
	for (auto & buf : buffers)
		flushBuffer(buf, check_thresholds);
}


void StorageBuffer::flushBuffer(Buffer & buffer, bool check_thresholds)
{
	Block block_to_write;
	time_t current_time = time(0);

	size_t rows = 0;
	size_t bytes = 0;
	time_t time_passed = 0;

	{
		std::lock_guard<std::mutex> lock(buffer.mutex);

		block_to_write = buffer.data.cloneEmpty();

		rows = buffer.data.rowsInFirstColumn();
		bytes = buffer.data.bytes();
		if (buffer.first_write_time)
			time_passed = current_time - buffer.first_write_time;

		if (check_thresholds)
		{
			if (!checkThresholdsImpl(rows, bytes, time_passed))
				return;
		}
		else
		{
			if (rows == 0)
				return;
		}

		buffer.data.swap(block_to_write);
		buffer.first_write_time = 0;

		CurrentMetrics::sub(CurrentMetrics::StorageBufferRows, block_to_write.rows());
		CurrentMetrics::sub(CurrentMetrics::StorageBufferBytes, block_to_write.bytes());

		LOG_TRACE(log, "Flushing buffer with " << rows << " rows, " << bytes << " bytes, age " << time_passed << " seconds.");

		if (no_destination)
			return;

		/** For simplicity, buffer is locked during write.
		  * We could unlock buffer temporary, but it would lead to too much difficulties:
		  * - data, that is written, will not be visible for SELECTs;
		  * - new data could be appended to buffer, and in case of exception, we must merge it with old data, that has not been written;
		  * - this could lead to infinite memory growth.
		  */
		try
		{
			writeBlockToDestination(block_to_write, context.tryGetTable(destination_database, destination_table));
		}
		catch (...)
		{
			ProfileEvents::increment(ProfileEvents::StorageBufferErrorOnFlush);

			/// Возвращаем блок на место в буфер.

			CurrentMetrics::add(CurrentMetrics::StorageBufferRows, block_to_write.rows());
			CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, block_to_write.bytes());

			buffer.data.swap(block_to_write);

			if (!buffer.first_write_time)
				buffer.first_write_time = current_time;

			/// Через некоторое время будет следующая попытка записать.
			throw;
		}
	}
}


void StorageBuffer::writeBlockToDestination(const Block & block, StoragePtr table)
{
	if (no_destination || !block)
		return;

	if (!table)
	{
		LOG_ERROR(log, "Destination table " << destination_database << "." << destination_table << " doesn't exist. Block of data is discarded.");
		return;
	}

	auto insert = std::make_shared<ASTInsertQuery>();

	insert->database = destination_database;
	insert->table = destination_table;

	/** Будем вставлять столбцы, являющиеся пересечением множества столбцов таблицы-буфера и подчинённой таблицы.
	  * Это позволит поддержать часть случаев (но не все), когда структура таблицы не совпадает.
	  */
	Block structure_of_destination_table = table->getSampleBlock();
	Names columns_intersection;
	columns_intersection.reserve(block.columns());
	for (size_t i : ext::range(0, structure_of_destination_table.columns()))
	{
		auto dst_col = structure_of_destination_table.unsafeGetByPosition(i);
		if (block.has(dst_col.name))
		{
			if (block.getByName(dst_col.name).type->getName() != dst_col.type->getName())
			{
				LOG_ERROR(log, "Destination table " << destination_database << "." << destination_table
					<< " have different type of column " << dst_col.name << ". Block of data is discarded.");
				return;
			}

			columns_intersection.push_back(dst_col.name);
		}
	}

	if (columns_intersection.empty())
	{
		LOG_ERROR(log, "Destination table " << destination_database << "." << destination_table << " have no common columns with block in buffer. Block of data is discarded.");
		return;
	}

	if (columns_intersection.size() != block.columns())
		LOG_WARNING(log, "Not all columns from block in buffer exist in destination table "
			<< destination_database << "." << destination_table << ". Some columns are discarded.");

	auto list_of_columns = std::make_shared<ASTExpressionList>();
	insert->columns = list_of_columns;
	list_of_columns->children.reserve(columns_intersection.size());
	for (const String & column : columns_intersection)
		list_of_columns->children.push_back(std::make_shared<ASTIdentifier>(StringRange(), column, ASTIdentifier::Column));

	InterpreterInsertQuery interpreter{insert, context};

	auto block_io = interpreter.execute();
	block_io.out->writePrefix();
	block_io.out->write(block);
	block_io.out->writeSuffix();
}


void StorageBuffer::flushThread()
{
	setThreadName("BufferFlush");

	do
	{
		try
		{
			flushAllBuffers(true);
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	} while (!shutdown_event.tryWait(1000));
}


void StorageBuffer::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
	for (const auto & param : params)
		if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
			throw Exception("Storage engine " + getName() + " doesn't support primary key.", ErrorCodes::NOT_IMPLEMENTED);

	auto lock = lockStructureForAlter();

	/// Чтобы не осталось блоков старой структуры.
	optimize({}, {}, context.getSettings());

	params.apply(*columns, materialized_columns, alias_columns, column_defaults);

	context.getDatabase(database_name)->alterTable(
		context, table_name,
		*columns, materialized_columns, alias_columns, column_defaults, {});
}

}
