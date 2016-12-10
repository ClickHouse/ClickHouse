#pragma once

#include <common/logger_useful.h>

#include <DB/Core/Defines.h>
#include <DB/Core/Names.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Common/Exception.h>
#include <DB/Core/QueryProcessingStage.h>
#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTAlterQuery.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Storages/ITableDeclaration.h>
#include <DB/Storages/AlterCommands.h>
#include <Poco/File.h>
#include <Poco/RWLock.h>
#include <memory>
#include <experimental/optional>


namespace DB
{

namespace ErrorCodes
{
	extern const int TABLE_IS_DROPPED;
}

class Context;
class IBlockInputStream;
class IBlockOutputStream;

using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;


class IStorage;

using StoragePtr = std::shared_ptr<IStorage>;


/** Хранилище. Отвечает за:
  * - хранение данных таблицы;
  * - определение, в каком файле (или не файле) хранятся данные;
  * - поиск данных и обновление данных;
  * - структура хранения данных (сжатие, etc.)
  * - конкуррентный доступ к данным (блокировки, etc.)
  */
class IStorage : public std::enable_shared_from_this<IStorage>, private boost::noncopyable, public ITableDeclaration
{
public:
	/// Основное имя типа таблицы (например, StorageMergeTree).
	virtual std::string getName() const = 0;

	/** Возвращает true, если хранилище получает данные с удалённого сервера или серверов. */
	virtual bool isRemote() const { return false; }

	/** Возвращает true, если хранилище поддерживает запросы с секцией SAMPLE. */
	virtual bool supportsSampling() const { return false; }

	/** Возвращает true, если хранилище поддерживает запросы с секцией FINAL. */
	virtual bool supportsFinal() const { return false; }

	/** Возвращает true, если хранилище поддерживает запросы с секцией PREWHERE. */
	virtual bool supportsPrewhere() const { return false; }

	/** Возвращает true, если хранилище поддерживает несколько реплик. */
	virtual bool supportsParallelReplicas() const { return false; }

	/** Не дает изменять описание таблицы (в том числе переименовывать и удалять таблицу).
	  * Если в течение какой-то операции структура таблицы должна оставаться неизменной, нужно держать такой лок на все ее время.
	  * Например, нужно держать такой лок на время всего запроса SELECT или INSERT и на все время слияния набора кусков
	  *  (но между выбором кусков для слияния и их слиянием структура таблицы может измениться).
	  * NOTE: Это лок на "чтение" описания таблицы. Чтобы изменить описание таблицы, нужно взять TableStructureWriteLock.
	  */
	class TableStructureReadLock
	{
	private:
		friend class IStorage;

		StoragePtr storage;
		/// Порядок важен.
		std::experimental::optional<Poco::ScopedReadRWLock> data_lock;
		std::experimental::optional<Poco::ScopedReadRWLock> structure_lock;

	public:
		TableStructureReadLock(StoragePtr storage_, bool lock_structure, bool lock_data) : storage(storage_)
		{
			if (lock_data)
				data_lock.emplace(storage->data_lock);
			if (lock_structure)
				structure_lock.emplace(storage->structure_lock);
		}
	};

	using TableStructureReadLockPtr = std::shared_ptr<TableStructureReadLock>;
	using TableStructureReadLocks = std::vector<TableStructureReadLockPtr>;

	/** Не дает изменять структуру или имя таблицы.
	  * Если в рамках этого лока будут изменены данные в таблице, нужно указать will_modify_data=true.
	  * Это возьмет дополнительный лок, не позволяющий начать ALTER MODIFY.
	  *
	  * WARNING: Вызывать методы из ITableDeclaration нужно под такой блокировкой. Без нее они не thread safe.
	  * WARNING: To avoid deadlocks, this method must not be called under lock of Context.
	  */
	TableStructureReadLockPtr lockStructure(bool will_modify_data)
	{
		TableStructureReadLockPtr res = std::make_shared<TableStructureReadLock>(shared_from_this(), true, will_modify_data);
		if (is_dropped)
			throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
		return res;
	}

	using TableStructureWriteLockPtr = std::unique_ptr<Poco::ScopedWriteRWLock>;
	using TableDataWriteLockPtr = std::unique_ptr<Poco::ScopedWriteRWLock>;
	using TableFullWriteLockPtr = std::pair<TableDataWriteLockPtr, TableStructureWriteLockPtr>;

	/** Не дает читать структуру таблицы. Берется для ALTER, RENAME и DROP.
	  */
	TableFullWriteLockPtr lockForAlter()
	{
		/// Порядок вычисления важен.
		auto data_lock = lockDataForAlter();
		auto structure_lock = lockStructureForAlter();

		return {std::move(data_lock), std::move(structure_lock)};
	}

	/** Не дает изменять данные в таблице. (Более того, не дает посмотреть на структуру таблицы с намерением изменить данные).
	  * Берется на время записи временных данных в ALTER MODIFY.
	  * Под этим локом можно брать lockStructureForAlter(), чтобы изменить структуру таблицы.
	  */
	TableDataWriteLockPtr lockDataForAlter()
	{
		auto res = std::make_unique<Poco::ScopedWriteRWLock>(data_lock);
		if (is_dropped)
			throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
		return res;
	}

	TableStructureWriteLockPtr lockStructureForAlter()
	{
		auto res = std::make_unique<Poco::ScopedWriteRWLock>(structure_lock);
		if (is_dropped)
			throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
		return res;
	}


	/** Читать набор столбцов из таблицы.
	  * Принимает список столбцов, которых нужно прочитать, а также описание запроса,
	  *  из которого может быть извлечена информация о том, каким способом извлекать данные
	  *  (индексы, блокировки и т. п.)
	  * Возвращает поток с помощью которого можно последовательно читать данные
	  *  или несколько потоков для параллельного чтения данных.
	  * Также в processed_stage записывается, до какой стадии запрос был обработан.
	  * (Обычно функция только читает столбцы из списка, но в других случаях,
	  *  например, запрос может быть частично обработан на удалённом сервере.)
	  *
	  * settings - настройки на один запрос.
	  * Обычно Storage не заботится об этих настройках, так как они применяются в интерпретаторе.
	  * Но, например, при распределённой обработке запроса, настройки передаются на удалённый сервер.
	  *
	  * threads - рекомендация, сколько потоков возвращать,
	  *  если хранилище может возвращать разное количество потоков.
	  *
	  * Гарантируется, что структура таблицы не изменится за время жизни возвращенных потоков (то есть не будет ALTER, RENAME и DROP).
	  */
	virtual BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1)
	{
		throw Exception("Method read is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Пишет данные в таблицу.
	  * Принимает описание запроса, в котором может содержаться информация о методе записи данных.
	  * Возвращает объект, с помощью которого можно последовательно писать данные.
	  *
	  * Гарантируется, что структура таблицы не изменится за время жизни возвращенных потоков (то есть не будет ALTER, RENAME и DROP).
	  */
	virtual BlockOutputStreamPtr write(
		ASTPtr query,
		const Settings & settings)
	{
		throw Exception("Method write is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Удалить данные таблицы. Вызывается перед удалением директории с данными.
	  * Если не требуется никаких действий, кроме удаления директории с данными, этот метод можно оставить пустым.
	  */
	virtual void drop() {}

	/** Переименовать таблицу.
	  * Переименование имени в файле с метаданными, имени в списке таблиц в оперативке, осуществляется отдельно.
	  * В этой функции нужно переименовать директорию с данными, если она есть.
	  * Вызывается при заблокированной на запись структуре таблицы.
	  */
	virtual void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
	{
		throw Exception("Method rename is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** ALTER таблицы в виде изменения столбцов, не затрагивающий изменение Storage или его параметров.
	  * Этот метод должен полностью выполнить запрос ALTER, самостоятельно заботясь о блокировках.
	  * Для обновления метаданных таблицы на диске этот метод должен вызвать InterpreterAlterQuery::updateMetadata.
	  */
	virtual void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
	{
		throw Exception("Method alter is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Выполнить запрос (DROP|DETACH) PARTITION.
	  */
	virtual void dropPartition(ASTPtr query, const Field & partition, bool detach, bool unreplicated, const Settings & settings)
	{
		throw Exception("Method dropPartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Выполнить запрос ATTACH [UNREPLICATED] (PART|PARTITION).
	  */
	virtual void attachPartition(ASTPtr query, const Field & partition, bool unreplicated, bool part, const Settings & settings)
	{
		throw Exception("Method attachPartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Выполнить запрос FETCH PARTITION.
	  */
	virtual void fetchPartition(const Field & partition, const String & from, const Settings & settings)
	{
		throw Exception("Method fetchPartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Выполнить запрос FREEZE PARTITION. То есть, создать локальный бэкап (снэпшот) данных с помощью функции localBackup (см. localBackup.h)
	  */
	virtual void freezePartition(const Field & partition, const String & with_name, const Settings & settings)
	{
		throw Exception("Method freezePartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Выполнить запрос RESHARD PARTITION.
	  */
	virtual void reshardPartitions(ASTPtr query, const String & database_name,
		const Field & first_partition, const Field & last_partition,
		const WeightedZooKeeperPaths & weighted_zookeeper_paths,
		const ASTPtr & sharding_key_expr, bool do_copy, const Field & coordinator,
		const Settings & settings)
	{
		throw Exception("Method reshardPartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Выполнить какую-либо фоновую работу. Например, объединение кусков в таблице типа MergeTree.
	  * Возвращает - была ли выполнена какая-либо работа.
	  */
	virtual bool optimize(const String & partition, bool final, const Settings & settings)
	{
		throw Exception("Method optimize is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Если при уничтожении объекта надо сделать какую-то сложную работу - сделать её заранее.
	  * Например, если таблица содержит какие-нибудь потоки для фоновой работы - попросить их завершиться и дождаться завершения.
	  * По-умолчанию - ничего не делать.
	  * Может вызываться одновременно из разных потоков, даже после вызова drop().
	  */
	virtual void shutdown() {}

	bool is_dropped{false};

	/// Поддерживается ли индекс в секции IN
	virtual bool supportsIndexForIn() const { return false; }

	/// проверяет валидность данных
	virtual bool checkData() const { throw DB::Exception("Check query is not supported for " + getName() + " storage"); }

protected:
	using ITableDeclaration::ITableDeclaration;
	using std::enable_shared_from_this<IStorage>::shared_from_this;

private:
	/// Брать следующие два лока всегда нужно в этом порядке.

	/** Берется на чтение на все время запроса INSERT и на все время слияния кусков (для MergeTree).
	  * Берется на запись на все время ALTER MODIFY.
	  *
	  * Формально:
	  * Ввзятие на запись гарантирует, что:
	  *  1) данные в таблице не изменится, пока лок жив,
	  *  2) все изменения данных после отпускания лока будут основаны на структуре таблицы на момент после отпускания лока.
	  * Нужно брать на чтение на все время операции, изменяющей данные.
	  */
	mutable Poco::RWLock data_lock;

	/** Лок для множества столбцов и пути к таблице. Берется на запись в RENAME, ALTER (для ALTER MODIFY ненадолго) и DROP.
	  * Берется на чтение на все время SELECT, INSERT и слияния кусков (для MergeTree).
	  *
	  * Взятие этого лока на запись - строго более "сильная" операция, чем взятие parts_writing_lock на запись.
	  * То есть, если этот лок взят на запись, о parts_writing_lock можно не заботиться.
	  * parts_writing_lock нужен только для случаев, когда не хочется брать table_structure_lock надолго (ALTER MODIFY).
	  */
	mutable Poco::RWLock structure_lock;
};

using StorageVector = std::vector<StoragePtr>;
using TableLocks = IStorage::TableStructureReadLocks;

/// имя таблицы -> таблица
using Tables = std::map<String, StoragePtr>;

}
