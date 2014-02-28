#pragma once

#include <statdaemons/Increment.h>
#include <statdaemons/threadpool.hpp>

#include <DB/Core/SortDescription.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Storages/IStorage.h>
#include <Poco/RWLock.h>


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
  * Имеется несколько режимов работы, определяющих, что делать при мердже:
  * - Ordinary - ничего дополнительно не делать;
  * - Collapsing - при склейке кусков "схлопывать"
  *   пары записей с разными значениями sign_column для одного значения первичного ключа.
  *   (см. CollapsingSortedBlockInputStream.h)
  * - Summing - при склейке кусков, при совпадении PK суммировать все числовые столбцы, не входящие в PK.
  */

struct StorageMergeTreeSettings
{
	/// Набор кусков разрешено объединить, если среди них максимальный размер не более чем во столько раз больше суммы остальных.
	double max_size_ratio_to_merge_parts = 5;
	
	/// Сколько за раз сливать кусков.
	/// Трудоемкость выбора кусков O(N * max_parts_to_merge_at_once), так что не следует делать это число слишком большим.
	/// С другой стороны, чтобы слияния точно не могли зайти в тупик, нужно хотя бы
	/// log(max_rows_to_merge_parts/index_granularity)/log(max_size_ratio_to_merge_parts).
	size_t max_parts_to_merge_at_once = 10;
	
	/// Куски настолько большого размера в основном потоке объединять нельзя вообще.
	size_t max_rows_to_merge_parts = 100 * 1024 * 1024;
	
	/// Куски настолько большого размера во втором потоке объединять нельзя вообще.
	size_t max_rows_to_merge_parts_second = 1024 * 1024;

	/// Во столько раз ночью увеличиваем коэффициент.
	size_t merge_parts_at_night_inc = 10;

	/// Сколько потоков использовать для объединения кусков.
	size_t merging_threads = 2;

	/// Если из одного файла читается хотя бы столько строк, чтение можно распараллелить.
	size_t min_rows_for_concurrent_read = 20 * 8192;
	
	/// Можно пропускать чтение более чем стольки строк ценой одного seek по файлу.
	size_t min_rows_for_seek = 5 * 8192;
	
	/// Если отрезок индекса может содержать нужные ключи, делим его на столько частей и рекурсивно проверяем их.
	size_t coarse_index_granularity = 8;

	/** Максимальное количество строк на запрос, для использования кэша разжатых данных. Если запрос большой - кэш не используется.
	  * (Чтобы большие запросы не вымывали кэш.)
	  */
	size_t max_rows_to_use_cache = 1024 * 1024;
	
	/// Через сколько секунд удалять old_куски.
	time_t old_parts_lifetime = 5 * 60;
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
friend class MergeTreeReader;
friend class MergeTreeBlockInputStream;
friend class MergeTreeBlockOutputStream;
friend class MergedBlockOutputStream;

public:
	/// Режим работы. См. выше.
	enum Mode
	{
		Ordinary,
		Collapsing,
		Summing,
	};

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
		Mode mode_ = Ordinary,
		const String & sign_column_ = "",
		const StorageMergeTreeSettings & settings_ = StorageMergeTreeSettings());

	void shutdown();
	~StorageMergeTree();

	std::string getName() const
	{
		switch (mode)
		{
			case Ordinary: 		return "MergeTree";
			case Collapsing: 	return "CollapsingMergeTree";
			case Summing: 		return "SummingMergeTree";

			default:
				throw Exception("Unknown mode of operation for StorageMergeTree: " + toString(mode), ErrorCodes::LOGICAL_ERROR);
		}
	}

	std::string getTableName() const { return name; }
	std::string getSignColumnName() const { return sign_column; }
	bool supportsSampling() const { return !!sampling_expression; }
	bool supportsFinal() const { return !sign_column.empty(); }
	bool supportsPrewhere() const { return true; }

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
		merge(1, false, true);
		return true;
	}

	void dropImpl();
	
	void rename(const String & new_path_to_db, const String & new_name);

	/// Метод ALTER позволяет добавлять и удалять столбцы.
	/// Метод ALTER нужно применять, когда обращения к базе приостановлены.
	/// Например если параллельно с INSERT выполнить ALTER, то ALTER выполниться, а INSERT бросит исключение
	void alter(const ASTAlterQuery::Parameters & params);

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
	size_t max_marks_to_use_cache;

	/// Режим работы - какие дополнительные действия делать при мердже.
	Mode mode;
	/// Для схлопывания записей об изменениях, если используется Collapsing режим работы.
	String sign_column;
	
	StorageMergeTreeSettings settings;

	ExpressionActionsPtr primary_expr;
	SortDescription sort_descr;
	Block primary_key_sample;

	Increment increment;

	Logger * log;
	volatile bool shutdown_called;

	/// Регулярное выражение соответсвующее названию директории с кусочками
	Poco::RegularExpression file_name_regexp;

	/// Описание куска с данными.
	struct DataPart
	{
 		DataPart(StorageMergeTree & storage_) : storage(storage_), size_in_bytes(0), currently_merging(false) {}

		StorageMergeTree & storage;
		DayNum_t left_date;
		DayNum_t right_date;
		UInt64 left;
		UInt64 right;
		/// Уровень игнорируется. Использовался предыдущей эвристикой слияния.
		UInt32 level;

		std::string name;
		size_t size;	/// в количестве засечек.
		size_t size_in_bytes; /// размер в байтах, 0 - если не посчитано
		time_t modification_time;

		DayNum_t left_month;
		DayNum_t right_month;

		/// Смотреть и изменять это поле следует под залоченным data_parts_mutex.
		bool currently_merging;

		/// Первичный ключ. Всегда загружается в оперативку.
		typedef std::vector<Field> Index;
		Index index;

		/// NOTE можно загружать засечки тоже в оперативку

		/// Вычисляем сумарный размер всей директории со всеми файлами
		static size_t calcTotalSize(const String &from)
		{
			Poco::File cur(from);
			if (cur.isFile())
				return cur.getSize();
			std::vector<std::string> files;
			cur.list(files);
			size_t res = 0;
			for (size_t i = 0; i < files.size(); ++i)
				res += calcTotalSize(from + files[i]);
			return res;
		}

		void remove() const
		{
			String from = storage.full_path + name + "/";
			String to = storage.full_path + "tmp2_" + name + "/";

			Poco::File(from).renameTo(to);
			Poco::File(to).remove(true);
		}
		
		void renameToOld() const
		{
			String from = storage.full_path + name + "/";
			String to = storage.full_path + "old_" + name + "/";
			
			Poco::File f(from);
			f.setLastModified(Poco::Timestamp::fromEpochTime(time(0)));
			f.renameTo(to);
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

		/// Загрузить индекс и вычислить размер.
		void loadIndex()
		{
			size_t key_size = storage.sort_descr.size();
			index.resize(key_size * size);

			String index_path = storage.full_path + name + "/primary.idx";
			ReadBufferFromFile index_file(index_path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));

			for (size_t i = 0; i < size; ++i)
				for (size_t j = 0; j < key_size; ++j)
					storage.primary_key_sample.getByPosition(j).type->deserializeBinary(index[i * key_size + j], index_file);

			if (!index_file.eof())
				throw Exception("index file " + index_path + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);

			size_in_bytes = calcTotalSize(storage.full_path + name + "/");
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

	/// Пока существует, помечает части как currently_merging и пересчитывает общий объем сливаемых данных.
	/// Вероятно, что части будут помечены заранее.
	class CurrentlyMergingPartsTagger
	{
	public:
		std::vector<DataPartPtr> parts;
		Poco::FastMutex & data_mutex;

		CurrentlyMergingPartsTagger(const std::vector<DataPartPtr> & parts_, Poco::FastMutex & data_mutex_) : parts(parts_), data_mutex(data_mutex_)
		{
			/// Здесь не лочится мьютекс, так как конструктор вызывается внутри selectPartsToMerge, где он уже залочен
			/// Poco::ScopedLock<Poco::FastMutex> lock(data_mutex);
			for (size_t i = 0; i < parts.size(); ++i)
			{
				parts[i]->currently_merging = true;
				StorageMergeTree::total_size_of_currently_merging_parts += parts[i]->size_in_bytes;
			}
		}

		~CurrentlyMergingPartsTagger()
		{
			Poco::ScopedLock<Poco::FastMutex> lock(data_mutex);
			for (size_t i = 0; i < parts.size(); ++i)
			{
				parts[i]->currently_merging = false;
				StorageMergeTree::total_size_of_currently_merging_parts -= parts[i]->size_in_bytes;
			}
		}
	};

	/// Сумарный размер currently_merging кусочков в байтах.
	/// Нужно чтобы оценить количество места на диске, которое может понадобится для завершения этих мерджей.
	static size_t total_size_of_currently_merging_parts;

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

	/** Взятие этого лока на запись, запрещает мердж */
	Poco::RWLock merge_lock;

	/** Взятие этого лока на запись, запрещает запись */
	Poco::RWLock write_lock;

	/** Взятие этого лока на запись, запрещает чтение */
	Poco::RWLock read_lock;

	StorageMergeTree(const String & path_, const String & name_, NamesAndTypesListPtr columns_,
					const Context & context_,
					ASTPtr & primary_expr_ast_,
					const String & date_column_name_,
					const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
					size_t index_granularity_,
					Mode mode_ = Ordinary,
					const String & sign_column_ = "",
					const StorageMergeTreeSettings & settings_ = StorageMergeTreeSettings());
	
	static String getPartName(DayNum_t left_date, DayNum_t right_date, UInt64 left_id, UInt64 right_id, UInt64 level);

	BlockInputStreams spreadMarkRangesAmongThreads(
		RangesInDataParts parts,
		size_t threads,
		const Names & column_names,
		size_t max_block_size,
		bool use_uncompressed_cache,
		ExpressionActionsPtr prewhere_actions,
		const String & prewhere_column);
	
	BlockInputStreams spreadMarkRangesAmongThreadsFinal(
		RangesInDataParts parts,
		size_t threads,
		const Names & column_names,
		size_t max_block_size,
		bool use_uncompressed_cache,
		ExpressionActionsPtr prewhere_actions,
		const String & prewhere_column);
	
	/// Создать выражение "Sign == 1".
	void createPositiveSignCondition(ExpressionActionsPtr & out_expression, String & out_column);
	
	/// Загрузить множество кусков с данными с диска. Вызывается один раз - при создании объекта.
	void loadDataParts();
	
	/// Удалить неактуальные куски.
	void clearOldParts();

	/** Определяет, какие куски нужно объединять, и запускает их слияние в отдельном потоке. Если iterations = 0, объединяет, пока это возможно.
	  * Если aggressive - выбрать куски не обращая внимание на соотношение размеров и их новизну (для запроса OPTIMIZE).
	  */
	void merge(size_t iterations = 1, bool async = true, bool aggressive = false);
	
	/// Если while_can, объединяет в цикле, пока можно; иначе выбирает и объединяет только одну пару кусков.
	void mergeThread(bool while_can, bool aggressive);
	
	/// Сразу помечает их как currently_merging.
	/// Если merge_anything_for_old_months, для кусков за прошедшие месяцы снимается ограничение на соотношение размеров.
	bool selectPartsToMerge(Poco::SharedPtr<CurrentlyMergingPartsTagger> & what, bool merge_anything_for_old_months, bool aggressive);

	void mergeParts(Poco::SharedPtr<CurrentlyMergingPartsTagger> & what);
	
	/// Дождаться, пока фоновые потоки закончат слияния.
	void joinMergeThreads();

	Poco::SharedPtr<boost::threadpool::pool> merge_threads;

	void removeColumnFiles(String column_name);

	/// Возвращает true если имя директории совпадает с форматом имени директории кусочков
	bool isPartDirectory(const String & dir_name, Poco::RegularExpression::MatchVec & matches) const;

	/// Кладет в DataPart данные из имени кусочка.
	void parsePartName(const String & file_name, const Poco::RegularExpression::MatchVec & matches, DataPart & part);
	
	/// Определить, не битые ли данные в директории. Проверяет индекс и засечеки, но не сами данные.
	bool isBrokenPart(const String & path);
	
	/// Найти самые большие old_куски, из которых получен этот кусок.
	/// Переименовать их, убрав префикс old_ и вернуть их имена.
	Strings tryRestorePart(const String & path, const String & file_name, Strings & old_parts);
};

}
