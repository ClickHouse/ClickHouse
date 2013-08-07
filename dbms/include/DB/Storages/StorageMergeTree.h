#pragma once

#include <statdaemons/Increment.h>
#include <statdaemons/threadpool.hpp>

#include <DB/Core/SortDescription.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

/** Движок, использующий merge tree для инкрементальной сортировки данных.
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
  *  / increment.txt - файл, содержащий одно число, увеличивающееся на 1 - для генерации идентификаторов кусков.
  *  / min-date _ max-date _ min-id _ max-id _ level / - директория с куском.
  *  / min-date _ max-date _ min-id _ max-id _ level / primary.idx - индексный файл.
  * Внутри директории с куском:
  *  Column.bin - данные столбца
  *  Column.mrk - засечки, указывающие, откуда начинать чтение, чтобы пропустить n * k строк.
  *
  * Если указано sign_column, то при склейке кусков, также "схлопываются"
  *  пары записей с разными значениями sign_column для одного значения первичного ключа.
  *  (см. CollapsingSortedBlockInputStream.h)
  */

struct StorageMergeTreeSettings
{
	/// Набор кусков разрешено объединить, если среди них максимальный размер не более чем во столько раз больше суммы остальных.
	double max_size_ratio_to_merge_parts;
	
	/// Сколько за раз сливать кусков.
	/// Трудоемкость выбора кусков O(N * max_parts_to_merge_at_once), так что не следует делать это число слишком большим.
	/// С другой стороны, чтобы слияния точно не могли зайти в тупик, нужно хотя бы
	/// log(max_rows_to_merge_parts/index_granularity)/log(max_size_ratio_to_merge_parts).
	size_t max_parts_to_merge_at_once;
	
	/// Куски настолько большого размера объединять нельзя вообще.
	size_t max_rows_to_merge_parts;
	
	/// Сколько потоков использовать для объединения кусков.
	size_t merging_threads;

	/// Если из одного файла читается хотя бы столько строк, чтение можно распараллелить.
	size_t min_rows_for_concurrent_read;
	
	/// Можно пропускать чтение более чем стольки строк ценой одного seek по файлу.
	size_t min_rows_for_seek;
	
	/// Если отрезок индекса может содержать нужные ключи, делим его на столько частей и рекурсивно проверяем их.
	size_t coarse_index_granularity;
	
	StorageMergeTreeSettings() :
		max_size_ratio_to_merge_parts(5),
		max_parts_to_merge_at_once(10),
		max_rows_to_merge_parts(100 * 1024 * 1024),
		merging_threads(2),
		min_rows_for_concurrent_read(20 * 8192),
		min_rows_for_seek(5 * 8192),
		coarse_index_granularity(8) {}
};

/// Пара засечек, определяющая диапазон строк в куске. Именно, диапазон имеет вид [begin * index_granularity, end * index_granularity).
struct MarkRange
{
	size_t begin;
	size_t end;
	
	MarkRange() {}
	MarkRange(size_t begin_, size_t end_) : begin(begin_), end(end_) {}
};

typedef std::vector<MarkRange> MarkRanges;


class StorageMergeTree : public IStorage
{
friend class MergeTreeBlockInputStream;
friend class MergeTreeBlockOutputStream;
friend class MergedBlockOutputStream;

public:
	/** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
	  *  (корректность имён и путей не проверяется)
	  *  состоящую из указанных столбцов.
	  *
	  * primary_expr_ast	- выражение для сортировки;
	  * date_column_name 	- имя столбца с датой;
	  * index_granularity 	- на сколько строчек пишется одно значение индекса.
	  */
	static StoragePtr create(const String & path_, const String & name_, NamesAndTypesListPtr columns_,
		const Context & context_,
		ASTPtr & primary_expr_ast_,
		const String & date_column_name_,
		const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
		size_t index_granularity_,
		const String & sign_column_ = "",
		const StorageMergeTreeSettings & settings_ = StorageMergeTreeSettings());

    ~StorageMergeTree();

	std::string getName() const { return sign_column.empty() ? "MergeTree" : "CollapsingMergeTree"; }
	std::string getTableName() const { return name; }
	std::string getSignColumnName() const { return sign_column; }
	bool supportsSampling() const { return !!sampling_expression; }
	bool supportsFinal() const { return !sign_column.empty(); }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	/** При чтении, выбирается набор кусков, покрывающий нужный диапазон индекса.
	  */
	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	/** При записи, данные сортируются и пишутся в новые куски.
	  */
	BlockOutputStreamPtr write(
		ASTPtr query);

	/** Выполнить очередной шаг объединения кусков.
	  */
	bool optimize()
	{
		merge(1, false);
		return true;
	}

	void dropImpl();
	
	void rename(const String & new_path_to_db, const String & new_name);

	void alter(const ASTAlterQuery::Parameters &params);

private:
	String path;
	String name;
	String full_path;
	NamesAndTypesListPtr columns;

	const Context & context;
	ASTPtr primary_expr_ast;
	String date_column_name;
	ASTPtr sampling_expression;
	size_t index_granularity;
	
	size_t min_marks_for_seek;
	size_t min_marks_for_concurrent_read;

	/// Для схлопывания записей об изменениях, если это требуется.
	String sign_column;
	
	StorageMergeTreeSettings settings;

	ExpressionActionsPtr primary_expr;
	SortDescription sort_descr;
	Block primary_key_sample;

	Increment increment;

	Logger * log;

	/// Регулярное выражение соответсвующее названию директории с кусочками
	Poco::RegularExpression file_name_regexp;

	/// Описание куска с данными.
	struct DataPart
	{
		DataPart(StorageMergeTree & storage_) : storage(storage_), currently_merging(false) {}

		StorageMergeTree & storage;
		Yandex::DayNum_t left_date;
		Yandex::DayNum_t right_date;
		UInt64 left;
		UInt64 right;
		/// Уровень игнорируется. Использовался предыдущей эвристикой слияния.
		UInt32 level;

		std::string name;
		size_t size;	/// в количестве засечек.
		time_t modification_time;

		Yandex::DayNum_t left_month;
		Yandex::DayNum_t right_month;
		
		/// Смотреть и изменять это поле следует под залоченным data_parts_mutex.
		bool currently_merging;

		/// NOTE можно загружать индекс и засечки в оперативку

		void remove() const
		{
			String from = storage.full_path + name + "/";
			String to = storage.full_path + "tmp2_" + name + "/";

			Poco::File(from).renameTo(to);
			Poco::File(to).remove(true);
		}

		bool operator< (const DataPart & rhs) const
		{
			if (left_month < rhs.left_month)
				return true;
			if (left_month > rhs.left_month)
				return false;
			if (right_month < rhs.right_month)
				return true;
			if (right_month > rhs.right_month)
				return false;

			if (left < rhs.left)
				return true;
			if (left > rhs.left)
				return false;
			if (right < rhs.right)
				return true;
			if (right > rhs.right)
				return false;

			if (level < rhs.level)
				return true;

			return false;
		}

		/// Содержит другой кусок (получен после объединения другого куска с каким-то ещё)
		bool contains(const DataPart & rhs) const
		{
			return left_month == rhs.left_month		/// Куски за разные месяцы не объединяются
				&& right_month == rhs.right_month
				&& level > rhs.level
				&& left_date <= rhs.left_date
				&& right_date >= rhs.right_date
				&& left <= rhs.left
				&& right >= rhs.right;
		}
	};

	typedef SharedPtr<DataPart> DataPartPtr;
	struct DataPartPtrLess { bool operator() (const DataPartPtr & lhs, const DataPartPtr & rhs) const { return *lhs < *rhs; } };
	typedef std::set<DataPartPtr, DataPartPtrLess> DataParts;
	
	struct RangesInDataPart
	{
		DataPartPtr data_part;
		MarkRanges ranges;

		RangesInDataPart() {}

		RangesInDataPart(DataPartPtr data_part_)
			: data_part(data_part_)
		{
		}
	};
	
	typedef std::vector<RangesInDataPart> RangesInDataParts;

	/** Множество всех кусков с данными, включая уже слитые в более крупные, но ещё не удалённые. Оно обычно небольшое (десятки элементов).
	  * Ссылки на кусок есть отсюда, из списка актуальных кусков, и из каждого потока чтения, который его сейчас использует.
	  * То есть, если количество ссылок равно 1 - то кусок не актуален и не используется прямо сейчас, и его можно удалить.
	  */
	DataParts all_data_parts;
	Poco::FastMutex all_data_parts_mutex;

	/** Актуальное множество кусков с данными. */
	DataParts data_parts;
	Poco::FastMutex data_parts_mutex;

	StorageMergeTree(const String & path_, const String & name_, NamesAndTypesListPtr columns_,
				  const Context & context_,
				  ASTPtr & primary_expr_ast_,
				  const String & date_column_name_,
				  const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
				  size_t index_granularity_,
				  const String & sign_column_ = "",
				  const StorageMergeTreeSettings & settings_ = StorageMergeTreeSettings());
	
	static String getPartName(Yandex::DayNum_t left_date, Yandex::DayNum_t right_date, UInt64 left_id, UInt64 right_id, UInt64 level);

	BlockInputStreams spreadMarkRangesAmongThreads(RangesInDataParts parts, size_t threads, const Names & column_names, size_t max_block_size);
	BlockInputStreams spreadMarkRangesAmongThreadsFinal(RangesInDataParts parts, size_t threads, const Names & column_names, size_t max_block_size);
	
	/// Создать выражение "Sign == 1".
	void createPositiveSignCondition(ExpressionActionsPtr & out_expression, String & out_column);
	
	/// Загрузить множество кусков с данными с диска. Вызывается один раз - при создании объекта.
	void loadDataParts();

	/// Удалить неактуальные куски.
	void clearOldParts();

	/// Определяет, какие куски нужно объединять, и запускает их слияние в отдельном потоке. Если iterations=0, объединяет, пока это возможно.
	void merge(size_t iterations = 1, bool async = true);
	/// Если while_can, объединяет в цикле, пока можно; иначе выбирает и объединяет только одну пару кусков.
	void mergeThread(bool while_can);
	/// Сразу помечает их как currently_merging.
	/// Если merge_anything_for_old_months, для кусков за прошедшие месяцы снимается ограничение на соотношение размеров.
	bool selectPartsToMerge(std::vector<DataPartPtr> & parts, bool merge_anything_for_old_months);
	void mergeParts(std::vector<DataPartPtr> parts);
	
	/// Дождаться, пока фоновые потоки закончат слияния.
	void joinMergeThreads();

	Poco::SharedPtr<boost::threadpool::pool> merge_threads;

	/// берет нерекурсивные блокировки data_parts_mutex и all_data_parts_mutex
	void removeColumnFiles(String column_name);

	/// Возвращает true если имя директории совпадает с форматом имени директории кусочков
	bool isBlockDirectory(const String dir_name, Poco::RegularExpression::MatchVec & matches);
};

}
