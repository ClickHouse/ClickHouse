#pragma once

#include <DB/Core/SortDescription.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Storages/IStorage.h>
#include <DB/Storages/MergeTree/ActiveDataPartSet.h>
#include <DB/Storages/MergeTree/MergeTreeSettings.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/GraphiteRollupSortedBlockInputStream.h>
#include <DB/Storages/MergeTree/MergeTreeDataPart.h>


struct SimpleIncrement;


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int INVALID_PARTITION_NAME;
	extern const int TOO_MUCH_PARTS;
	extern const int NO_SUCH_DATA_PART;
	extern const int DUPLICATE_DATA_PART;
	extern const int DIRECTORY_ALREADY_EXISTS;
	extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
	extern const int NO_SUCH_COLUMN_IN_TABLE;
	extern const int TABLE_DIFFERS_TOO_MUCH;
}

/** Структура данных для *MergeTree движков.
  * Используется merge tree для инкрементальной сортировки данных.
  * Таблица представлена набором сортированных кусков.
  * При вставке, данные сортируются по указанному выражению (первичному ключу) и пишутся в новый кусок.
  * Куски объединяются в фоне, согласно некоторой эвристике.
  * Для каждого куска, создаётся индексный файл, содержащий значение первичного ключа для каждой n-ой строки.
  * Таким образом, реализуется эффективная выборка по диапазону первичного ключа.
  *
  * Дополнительно:
  *
  *  Указывается столбец, содержащий дату.
  *  Для каждого куска пишется минимальная и максимальная дата.
  *  (по сути - ещё один индекс)
  *
  *  Данные разделяются по разным месяцам (пишутся в разные куски для разных месяцев).
  *  Куски для разных месяцев не объединяются - для простоты эксплуатации.
  *  (дают локальность обновлений, что удобно для синхронизации и бэкапа)
  *
  * Структура файлов:
  *  / min-date _ max-date _ min-id _ max-id _ level / - директория с куском.
  * Внутри директории с куском:
  *  checksums.txt - список файлов с их размерами и контрольными суммами.
  *  columns.txt - список столбцов с их типами.
  *  primary.idx - индексный файл.
  *  Column.bin - данные столбца
  *  Column.mrk - засечки, указывающие, откуда начинать чтение, чтобы пропустить n * k строк.
  *
  * Имеется несколько режимов работы, определяющих, что делать при мердже:
  * - Ordinary - ничего дополнительно не делать;
  * - Collapsing - при склейке кусков "схлопывать"
  *   пары записей с разными значениями sign_column для одного значения первичного ключа.
  *   (см. CollapsingSortedBlockInputStream.h)
  * - Replacing - при склейке кусков, при совпадении PK, оставлять только одну строчку
  *             - последнюю, либо, если задан столбец "версия" - последнюю среди строчек с максимальной версией.
  * - Summing - при склейке кусков, при совпадении PK суммировать все числовые столбцы, не входящие в PK.
  * - Aggregating - при склейке кусков, при совпадении PK, делается слияние состояний столбцов-агрегатных функций.
  * - Unsorted - при склейке кусков, данные не упорядочиваются, а всего лишь конкатенируются;
  *            - это позволяет читать данные ровно такими пачками, какими они были записаны.
  * - Graphite - выполняет загрубление исторических данных для таблицы Графита - системы количественного мониторинга.
  */

/** Этот класс хранит список кусков и параметры структуры данных.
  * Для чтения и изменения данных используются отдельные классы:
  *  - MergeTreeDataSelectExecutor
  *  - MergeTreeDataWriter
  *  - MergeTreeDataMerger
  */

class MergeTreeData : public ITableDeclaration
{
	friend class ReshardingWorker;

public:
	/// Функция, которую можно вызвать, если есть подозрение, что данные куска испорчены.
	using BrokenPartCallback = std::function<void (const String &)>;
	using DataPart = MergeTreeDataPart;

	using MutableDataPartPtr = std::shared_ptr<DataPart>;
	/// После добавление в рабочее множество DataPart нельзя изменять.
	using DataPartPtr = std::shared_ptr<const DataPart>;
	struct DataPartPtrLess { bool operator() (const DataPartPtr & lhs, const DataPartPtr & rhs) const { return *lhs < *rhs; } };
	using DataParts = std::set<DataPartPtr, DataPartPtrLess>;
	using DataPartsVector = std::vector<DataPartPtr>;

	/// Для перешардирования.
	using MutableDataParts = std::set<MutableDataPartPtr, DataPartPtrLess>;
	using PerShardDataParts = std::unordered_map<size_t, MutableDataPartPtr>;

	/// Некоторые операции над множеством кусков могут возвращать такой объект.
	/// Если не был вызван commit или rollback, деструктор откатывает операцию.
	class Transaction : private boost::noncopyable
	{
	public:
		Transaction() {}

		void commit()
		{
			clear();
		}

		void rollback()
		{
			if (data && (!parts_to_remove_on_rollback.empty() || !parts_to_add_on_rollback.empty()))
			{
				LOG_DEBUG(data->log, "Undoing transaction");
				data->replaceParts(parts_to_remove_on_rollback, parts_to_add_on_rollback, true);

				clear();
			}
		}

		~Transaction()
		{
			try
			{
				rollback();
			}
			catch(...)
			{
				tryLogCurrentException("~MergeTreeData::Transaction");
			}
		}
	private:
		friend class MergeTreeData;

		MergeTreeData * data = nullptr;

		/// Что делать для отката операции.
		DataPartsVector parts_to_remove_on_rollback;
		DataPartsVector parts_to_add_on_rollback;

		void clear()
		{
			data = nullptr;
			parts_to_remove_on_rollback.clear();
			parts_to_add_on_rollback.clear();
		}
	};

	/// Объект, помнящий какие временные файлы были созданы в директории с куском в ходе изменения (ALTER) его столбцов.
	class AlterDataPartTransaction : private boost::noncopyable
	{
	public:
		/// Переименовывает временные файлы, завершая ALTER куска.
		void commit();

		/// Если не был вызван commit(), удаляет временные файлы, отменяя ALTER куска.
		~AlterDataPartTransaction();

		/// Посмотреть изменения перед коммитом.
		const NamesAndTypesList & getNewColumns() const { return new_columns; }
		const DataPart::Checksums & getNewChecksums() const { return new_checksums; }

	private:
		friend class MergeTreeData;

		AlterDataPartTransaction(DataPartPtr data_part_) : data_part(data_part_), alter_lock(data_part->alter_mutex) {}

		void clear()
		{
			alter_lock.unlock();
			data_part = nullptr;
		}

		DataPartPtr data_part;
		std::unique_lock<std::mutex> alter_lock;

		DataPart::Checksums new_checksums;
		NamesAndTypesList new_columns;
		/// Если значение - пустая строка, файл нужно удалить, и он не временный.
		NameToNameMap rename_map;
	};

	using AlterDataPartTransactionPtr = std::unique_ptr<AlterDataPartTransaction>;


	/// Настройки для разных режимов работы.
	struct MergingParams
	{
		/// Режим работы. См. выше.
		enum Mode
		{
			Ordinary 	= 0,	/// Числа сохраняются - не меняйте.
			Collapsing 	= 1,
			Summing 	= 2,
			Aggregating = 3,
			Unsorted 	= 4,
			Replacing	= 5,
			Graphite	= 6,
		};

		Mode mode;

		/// Для Collapsing режима.
		String sign_column;

		/// Для Summing режима. Если пустое - то выбирается автоматически.
		Names columns_to_sum;

		/// Для Replacing режима. Может быть пустым.
		String version_column;

		/// Для Graphite режима.
		Graphite::Params graphite_params;

		/// Проверить наличие и корректность типов столбцов.
		void check(const NamesAndTypesList & columns) const;

		String getModeName() const;
	};


	/** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
	  *  (корректность имён и путей не проверяется)
	  *  состоящую из указанных столбцов.
	  *
	  * primary_expr_ast	- выражение для сортировки; Пустое для UnsortedMergeTree.
	  * date_column_name 	- имя столбца с датой;
	  * index_granularity 	- на сколько строчек пишется одно значение индекса.
	  * require_part_metadata - обязательно ли в директории с куском должны быть checksums.txt и columns.txt
	  */
	MergeTreeData(	const String & full_path_, NamesAndTypesListPtr columns_,
					const NamesAndTypesList & materialized_columns_,
					const NamesAndTypesList & alias_columns_,
					const ColumnDefaults & column_defaults_,
					Context & context_,
					ASTPtr & primary_expr_ast_,
					const String & date_column_name_,
					const ASTPtr & sampling_expression_, /// nullptr, если семплирование не поддерживается.
					size_t index_granularity_,
					const MergingParams & merging_params_,
					const MergeTreeSettings & settings_,
					const String & log_name_,
					bool require_part_metadata_,
					BrokenPartCallback broken_part_callback_ = [](const String &){});

	/// Загрузить множество кусков с данными с диска. Вызывается один раз - сразу после создания объекта.
	void loadDataParts(bool skip_sanity_checks);

	bool supportsSampling() const { return !!sampling_expression; }
	bool supportsPrewhere() const { return true; }

	bool supportsFinal() const
	{
		return merging_params.mode == MergingParams::Collapsing
			|| merging_params.mode == MergingParams::Summing
			|| merging_params.mode == MergingParams::Aggregating
			|| merging_params.mode == MergingParams::Replacing;
	}

	Int64 getMaxDataPartIndex();

	std::string getTableName() const override
	{
		throw Exception("Logical error: calling method getTableName of not a table.", ErrorCodes::LOGICAL_ERROR);
	}

	const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

	NameAndTypePair getColumn(const String & column_name) const override
	{
		if (column_name == "_part")
			return NameAndTypePair("_part", std::make_shared<DataTypeString>());
		if (column_name == "_part_index")
			return NameAndTypePair("_part_index", std::make_shared<DataTypeUInt64>());
		if (column_name == "_sample_factor")
			return NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>());

		return ITableDeclaration::getColumn(column_name);
	}

	bool hasColumn(const String & column_name) const override
	{
		return ITableDeclaration::hasColumn(column_name)
			|| column_name == "_part"
			|| column_name == "_part_index"
			|| column_name == "_sample_factor";
	}

	String getFullPath() const { return full_path; }

	String getLogName() const { return log_name; }

	/** Возвращает копию списка, чтобы снаружи можно было не заботиться о блокировках.
	  */
	DataParts getDataParts() const;
	DataPartsVector getDataPartsVector() const;
	DataParts getAllDataParts() const;

	/** Размер активной части в количестве байт.
	  */
	size_t getTotalActiveSizeInBytes() const;

	/** Максимальное количество кусков в одном месяце.
	  */
	size_t getMaxPartsCountForMonth() const;

	/** Минимальный номер блока в указанном месяце.
	  * Возвращает также bool - есть ли хоть один кусок.
	  */
	std::pair<Int64, bool> getMinBlockNumberForMonth(DayNum_t month) const;

	/** Есть ли указанный номер блока в каком-нибудь куске указанного месяца.
	  */
	bool hasBlockNumberInMonth(Int64 block_number, DayNum_t month) const;

	/** Если в таблице слишком много активных кусков, спит некоторое время, чтобы дать им возможность смерджиться.
	  * Если передано until - проснуться раньше, если наступило событие.
	  */
	void delayInsertIfNeeded(Poco::Event * until = nullptr);

	/** Возвращает активный кусок с указанным именем или кусок, покрывающий его. Если такого нет, возвращает nullptr.
	  */
	DataPartPtr getActiveContainingPart(const String & part_name);

	/** Возвращает кусок с таким именем (активный или не активный). Если нету, nullptr.
	  */
	DataPartPtr getPartIfExists(const String & part_name);
	DataPartPtr getShardedPartIfExists(const String & part_name, size_t shard_no);

	/** Переименовывает временный кусок в постоянный и добавляет его в рабочий набор.
	  * Если increment != nullptr, индекс куска берется из инкремента. Иначе индекс куска не меняется.
	  * Предполагается, что кусок не пересекается с существующими.
	  * Если out_transaction не nullptr, присваивает туда объект, позволяющий откатить добавление куска (но не переименование).
	  */
	void renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr);

	/** То же, что renameTempPartAndAdd, но кусок может покрывать существующие куски.
	  * Удаляет и возвращает все куски, покрытые добавляемым (в возрастающем порядке).
	  */
	DataPartsVector renameTempPartAndReplace(
		MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr);

	/** Убирает из рабочего набора куски remove и добавляет куски add. add должны уже быть в all_data_parts.
	  * Если clear_without_timeout, данные будут удалены сразу, либо при следующем clearOldParts, игнорируя old_parts_lifetime.
	  */
	void replaceParts(const DataPartsVector & remove, const DataPartsVector & add, bool clear_without_timeout);

	/** Добавляет новый кусок в список известных кусков и в рабочий набор.
	  */
	void attachPart(const DataPartPtr & part);

	/** Переименовывает кусок в detached/prefix_кусок и забывает про него. Данные не будут удалены в clearOldParts.
	  * Если restore_covered, добавляет в рабочий набор неактивные куски, слиянием которых получен удаляемый кусок.
	  */
	void renameAndDetachPart(const DataPartPtr & part, const String & prefix = "", bool restore_covered = false, bool move_to_detached = true);

	/** Убирает кусок из списка кусков (включая all_data_parts), но не перемещает директорию.
	  */
	void detachPartInPlace(const DataPartPtr & part);

	/** Возвращает старые неактуальные куски, которые можно удалить. Одновременно удаляет их из списка кусков, но не с диска.
	  */
	DataPartsVector grabOldParts();

	/** Обращает изменения, сделанные grabOldParts().
	  */
	void addOldParts(const DataPartsVector & parts);

	/** Удалить неактуальные куски.
	  */
	void clearOldParts();

	/** Удалить старые временные директории.
	  */
	void clearOldTemporaryDirectories();

	/** После вызова dropAllData больше ничего вызывать нельзя.
	  * Удаляет директорию с данными и сбрасывает кеши разжатых блоков и засечек.
	  */
	void dropAllData();

	/** Перемещает всю директорию с данными.
	  * Сбрасывает кеши разжатых блоков и засечек.
	  * Нужно вызывать под залоченным lockStructureForAlter().
	  */
	void setPath(const String & full_path, bool move_data);

	/* Проверить, что такой ALTER можно выполнить:
	 *  - Есть все нужные столбцы.
	 *  - Все преобразования типов допустимы.
	 *  - Не затронуты столбцы ключа, знака и семплирования.
	 * Бросает исключение, если что-то не так.
	 */
	void checkAlter(const AlterCommands & params);

	/** Выполняет ALTER куска данных, записывает результат во временные файлы.
	  * Возвращает объект, позволяющий переименовать временные файлы в постоянные.
	  * Если измененных столбцов подозрительно много, и !skip_sanity_checks, бросает исключение.
	  * Если никаких действий над данными не требуется, возвращает nullptr.
	  */
	AlterDataPartTransactionPtr alterDataPart(
		const DataPartPtr & part,
		const NamesAndTypesList & new_columns,
		const ASTPtr & new_primary_key,
		bool skip_sanity_checks);

	/// Нужно вызывать под залоченным lockStructureForAlter().
	void setColumnsList(const NamesAndTypesList & new_columns) { columns = std::make_shared<NamesAndTypesList>(new_columns); }

	/// Нужно вызвать, если есть подозрение, что данные куска испорчены.
	void reportBrokenPart(const String & name)
	{
		broken_part_callback(name);
	}

	ExpressionActionsPtr getPrimaryExpression() const { return primary_expr; }
	SortDescription getSortDescription() const { return sort_descr; }

	/// Проверить, что кусок не сломан и посчитать для него чексуммы, если их нет.
	MutableDataPartPtr loadPartAndFixMetadata(const String & relative_path);

	/** Create local backup (snapshot) for parts with specified prefix.
	  * Backup is created in directory clickhouse_dir/shadow/i/, where i - incremental number,
	  *  or if 'with_name' is specified - backup is created in directory with specified name.
	  */
	void freezePartition(const std::string & prefix, const String & with_name);

	/** Возвращает размер заданной партиции в байтах.
	  */
	size_t getPartitionSize(const std::string & partition_name) const;

	size_t getColumnSize(const std::string & name) const
	{
		std::lock_guard<std::mutex> lock{data_parts_mutex};

		const auto it = column_sizes.find(name);
		return it == std::end(column_sizes) ? 0 : it->second;
	}

	using ColumnSizes = std::unordered_map<std::string, size_t>;
	ColumnSizes getColumnSizes() const
	{
		std::lock_guard<std::mutex> lock{data_parts_mutex};
		return column_sizes;
	}

	/// Для ATTACH/DETACH/DROP/RESHARD PARTITION.
	static String getMonthName(const Field & partition);
	static String getMonthName(DayNum_t month);
	static DayNum_t getMonthDayNum(const Field & partition);
	static DayNum_t getMonthFromName(const String & month_name);
	/// Получить месяц из имени куска или достаточной его части.
	static DayNum_t getMonthFromPartPrefix(const String & part_prefix);

	Context & context;
	const String date_column_name;
	const ASTPtr sampling_expression;
	const size_t index_granularity;

	/// Режим работы - какие дополнительные действия делать при мердже.
	const MergingParams merging_params;

	const MergeTreeSettings settings;

	ASTPtr primary_expr_ast;
	Block primary_key_sample;
	DataTypes primary_key_data_types;

private:
	friend struct MergeTreeDataPart;
	friend class StorageMergeTree;

	bool require_part_metadata;

	ExpressionActionsPtr primary_expr;
	SortDescription sort_descr;

	String full_path;

	NamesAndTypesListPtr columns;
	/// Актуальные размеры столбцов в сжатом виде
	ColumnSizes column_sizes;

	BrokenPartCallback broken_part_callback;

	String log_name;
	Logger * log;

	/** Актуальное множество кусков с данными. */
	DataParts data_parts;
	mutable std::mutex data_parts_mutex;

	/** Множество всех кусков с данными, включая уже слитые в более крупные, но ещё не удалённые. Оно обычно небольшое (десятки элементов).
	  * Ссылки на кусок есть отсюда, из списка актуальных кусков и из каждого потока чтения, который его сейчас использует.
	  * То есть, если количество ссылок равно 1 - то кусок не актуален и не используется прямо сейчас, и его можно удалить.
	  */
	DataParts all_data_parts;
	mutable std::mutex all_data_parts_mutex;

	/// Используется, чтобы не выполнять одновременно функцию grabOldParts.
	std::mutex grab_old_parts_mutex;
	/// То же самое для clearOldTemporaryDirectories.
	std::mutex clear_old_temporary_directories_mutex;

	/** Для каждого шарда множество шардированных кусков.
	  */
	PerShardDataParts per_shard_data_parts;

	void initPrimaryKey();

	/** Выражение, преобразующее типы столбцов.
	  * Если преобразований типов нет, out_expression=nullptr.
	  * out_rename_map отображает файлы-столбцы на выходе выражения в новые файлы таблицы.
	  * out_force_update_metadata показывает, нужно ли обновить метаданные даже если out_rename_map пуста (используется
	  * 	для бесплатного изменения списка значений Enum).
	  * Файлы, которые нужно удалить, в out_rename_map отображаются в пустую строку.
	  * Если !part, просто проверяет, что все нужные преобразования типов допустимы.
	  */
	void createConvertExpression(const DataPartPtr & part, const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns,
		ExpressionActionsPtr & out_expression, NameToNameMap & out_rename_map, bool & out_force_update_metadata);

	/// Рассчитывает размеры столбцов в сжатом виде для текущего состояния data_parts. Вызывается под блокировкой.
	void calculateColumnSizes();
	/// Добавляет или вычитывает вклад part в размеры столбцов в сжатом виде
	void addPartContributionToColumnSizes(const DataPartPtr & part);
	void removePartContributionToColumnSizes(const DataPartPtr & part);
};

}
