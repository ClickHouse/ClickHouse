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
  *  primary.idx - индексный файл.
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

/** Этот класс хранит список кусков и параметры структуры данных.
  * Для чтения и изменения данных используются отдельные классы:
  *  - MergeTreeDataSelectExecutor
  *  - MergeTreeDataWriter
  *  - MergeTreeDataMerger
  */

struct MergeTreeSettings
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

class MergeTreeData : public ITableDeclaration
{
public:
	/// Описание куска с данными.
	struct DataPart
	{
		/** Контрольные суммы всех не временных файлов.
		  * Для сжатых файлов хранятся чексумма и размер разжатых данных, чтобы не зависеть от способа сжатия.
		  */
		struct Checksums
		{
			struct Checksum
			{
				size_t size;
				uint128 hash;
			};

			typedef std::map<String, Checksum> FileChecksums;
			FileChecksums files;

			/// Проверяет, что множество столбцов и их контрольные суммы совпадают. Если нет - бросает исключение.
			void check(const Checksums & rhs) const;

			/// Сериализует и десериализует в человекочитаемом виде.
			void readText(ReadBuffer & in);
			void writeText(WriteBuffer & out) const;

			bool empty() const
			{
				return files.empty();
			}
		};

 		DataPart(MergeTreeData & storage_) : storage(storage_), size_in_bytes(0) {}

 		MergeTreeData & storage;
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

		/// Первичный ключ. Всегда загружается в оперативку.
		typedef std::vector<Field> Index;
		Index index;

		Checksums checksums;

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

		void remove()
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
			if (left_month != rhs.left_month)
				return left_month < rhs.left_month;
			if (right_month != rhs.right_month)
				return right_month < rhs.right_month;

			if (left != rhs.left)
				return left < rhs.left;
			if (right != rhs.right)
				return right < rhs.right;

			if (level != rhs.level)
				return level < rhs.level;

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

		/// Прочитать контрольные суммы, если есть.
		bool loadChecksums()
		{
			String path = storage.full_path + name + "/checksums.txt";
			if (!Poco::File(path).exists())
				return false;
			ReadBufferFromFile file(path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
			checksums.readText(file);
			assertEOF(file);
			return true;
		}
	};

	typedef std::shared_ptr<DataPart> MutableDataPartPtr;
	/// После добавление в рабочее множество DataPart нельзя изменять.
	typedef std::shared_ptr<const DataPart> DataPartPtr;
	struct DataPartPtrLess { bool operator() (const DataPartPtr & lhs, const DataPartPtr & rhs) const { return *lhs < *rhs; } };
	typedef std::set<DataPartPtr, DataPartPtrLess> DataParts;
	typedef std::vector<DataPartPtr> DataPartsVector;


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
	MergeTreeData(	const String & full_path_, NamesAndTypesListPtr columns_,
					const Context & context_,
					ASTPtr & primary_expr_ast_,
					const String & date_column_name_,
					const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
					size_t index_granularity_,
					Mode mode_,
					const String & sign_column_,
					const MergeTreeSettings & settings_);

	std::string getModePrefix() const;

	std::string getSignColumnName() const { return sign_column; }
	bool supportsSampling() const { return !!sampling_expression; }
	bool supportsFinal() const { return !sign_column.empty(); }
	bool supportsPrewhere() const { return true; }

	UInt64 getMaxDataPartIndex();

	static String getPartName(DayNum_t left_date, DayNum_t right_date, UInt64 left_id, UInt64 right_id, UInt64 level);

	/// Возвращает true если имя директории совпадает с форматом имени директории кусочков
	bool isPartDirectory(const String & dir_name, Poco::RegularExpression::MatchVec & matches) const;

	/// Кладет в DataPart данные из имени кусочка.
	void parsePartName(const String & file_name, const Poco::RegularExpression::MatchVec & matches, DataPart & part);

	std::string getTableName() { return ""; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	String getFullPath() const { return full_path; }

	/** Возвращает копию списка, чтобы снаружи можно было не заботиться о блокировках.
	  */
	DataParts getDataParts();

	/** Удаляет куски old_parts и добавляет кусок new_part. Если какого-нибудь из удаляемых кусков нет, бросает исключение.
	  */
	void replaceParts(DataPartsVector old_parts, DataPartPtr new_part);

	/** Переименовывает временный кусок в постоянный и добавляет его в рабочий набор.
	  * Если increment!=nullptr, индекс куска берется из инкремента. Иначе индекс куска не меняется.
	  */
	void renameTempPartAndAdd(MutableDataPartPtr part, Increment * increment);

	/** Удалить неактуальные куски.
	  */
	void clearOldParts();

	/** После вызова dropAllData больше ничего вызывать нельзя.
	  * Удаляет директорию с данными и сбрасывает кеши разжатых блоков и засечек.
	  */
	void dropAllData();

	/** Перемещает всю директорию с данными.
	  * Сбрасывает кеши разжатых блоков и засечек.
	  * Нужно вызывать под залоченным lockStructure().
	  */
	void setPath(const String & full_path);

	void alter(const ASTAlterQuery::Parameters & params);
	void prepareAlterModify(const ASTAlterQuery::Parameters & params);
	void commitAlterModify(const ASTAlterQuery::Parameters & params);

	ExpressionActionsPtr getPrimaryExpression() const { return primary_expr; }
	SortDescription getSortDescription() const { return sort_descr; }

	const Context & context;
	const String date_column_name;
	const ASTPtr sampling_expression;
	const size_t index_granularity;

	/// Режим работы - какие дополнительные действия делать при мердже.
	const Mode mode;
	/// Для схлопывания записей об изменениях, если используется Collapsing режим работы.
	const String sign_column;

	const MergeTreeSettings settings;

private:
	ExpressionActionsPtr primary_expr;
	SortDescription sort_descr;
	Block primary_key_sample;

	ASTPtr primary_expr_ast;

	String full_path;

	NamesAndTypesListPtr columns;

	Logger * log;
	volatile bool shutdown_called;

	/// Регулярное выражение соответсвующее названию директории с кусочками
	Poco::RegularExpression file_name_regexp;

	/** Актуальное множество кусков с данными. */
	DataParts data_parts;
	Poco::FastMutex data_parts_mutex;

	/** Множество всех кусков с данными, включая уже слитые в более крупные, но ещё не удалённые. Оно обычно небольшое (десятки элементов).
	  * Ссылки на кусок есть отсюда, из списка актуальных кусков и из каждого потока чтения, который его сейчас использует.
	  * То есть, если количество ссылок равно 1 - то кусок не актуален и не используется прямо сейчас, и его можно удалить.
	  */
	DataParts all_data_parts;
	Poco::FastMutex all_data_parts_mutex;

	/// Загрузить множество кусков с данными с диска. Вызывается один раз - при создании объекта.
	void loadDataParts();

	void removeColumnFiles(String column_name);

	/// Определить, не битые ли данные в директории. Проверяет индекс и засечеки, но не сами данные.
	bool isBrokenPart(const String & path);

	/// Найти самые большие old_куски, из которых получен этот кусок.
	/// Переименовать их, убрав префикс old_ и вернуть их имена.
	Strings tryRestorePart(const String & path, const String & file_name, Strings & old_parts);

	void createConvertExpression(const String & in_column_name, const String & out_type, ExpressionActionsPtr & out_expression, String & out_column);
};

}
