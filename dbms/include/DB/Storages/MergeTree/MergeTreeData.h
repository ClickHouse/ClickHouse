#pragma once

#include <statdaemons/Increment.h>

#include <DB/Core/SortDescription.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Storages/IStorage.h>
#include <DB/Storages/MergeTree/ActiveDataPartSet.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/Common/escapeForFileName.h>
#include <Poco/RWLock.h>


#define MERGE_TREE_MARK_SIZE (2 * sizeof(size_t))


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
	/// Опеределяет, насколько разбалансированные объединения мы готовы делать.
	/// Чем больше, тем более разбалансированные. Желательно, чтобы было больше, чем 1/max_parts_to_merge_at_once.
	double size_ratio_coefficient_to_merge_parts = 0.25;

	/// Сколько за раз сливать кусков.
	/// Трудоемкость выбора кусков O(N * max_parts_to_merge_at_once).
	size_t max_parts_to_merge_at_once = 10;

	/// Куски настолько большого размера в основном потоке объединять нельзя вообще.
	size_t max_rows_to_merge_parts = 100 * 1024 * 1024;

	/// Куски настолько большого размера во втором потоке объединять нельзя вообще.
	size_t max_rows_to_merge_parts_second = 1024 * 1024;

	/// Во столько раз ночью увеличиваем коэффициент.
	size_t merge_parts_at_night_inc = 10;

	/// Сколько потоков использовать для объединения кусков (для MergeTree).
	/// Пул потоков общий на весь сервер.
	size_t merging_threads = 6;

	/// Сколько потоков использовать для загрузки кусков с других реплик и объединения кусков (для ReplicatedMergeTree).
	/// Пул потоков на каждую таблицу свой.
	size_t replication_threads = 4;

	/// Если из одного файла читается хотя бы столько строк, чтение можно распараллелить.
	size_t min_rows_for_concurrent_read = 20 * 8192;

	/// Можно пропускать чтение более чем стольки строк ценой одного seek по файлу.
	size_t min_rows_for_seek = 5 * 8192;

	/// Если отрезок индекса может содержать нужные ключи, делим его на столько частей и рекурсивно проверяем их.
	size_t coarse_index_granularity = 8;

	/// Максимальное количество строк на запрос, для использования кэша разжатых данных. Если запрос большой - кэш не используется.
	/// (Чтобы большие запросы не вымывали кэш.)
	size_t max_rows_to_use_cache = 1024 * 1024;

	/// Через сколько секунд удалять ненужные куски.
	time_t old_parts_lifetime = 5 * 60;

	/// Если в таблице хотя бы столько активных кусков, искусственно замедлять вставки в таблицу.
	size_t parts_to_delay_insert = 150;

	/// Если в таблице parts_to_delay_insert + k кусков, спать insert_delay_step^k миллисекунд перед вставкой каждого блока.
	/// Таким образом, скорость вставок автоматически замедлится примерно до скорости слияний.
	double insert_delay_step = 1.1;

	/// Для скольки блоков, вставленных с непустым insert ID, хранить хеши в ZooKeeper.
	size_t replicated_deduplication_window = 10000;
};

class MergeTreeData : public ITableDeclaration
{
public:
	/// Описание куска с данными.
	struct DataPart : public ActiveDataPartSet::Part
	{
		/** Контрольные суммы всех не временных файлов.
		  * Для сжатых файлов хранятся чексумма и размер разжатых данных, чтобы не зависеть от способа сжатия.
		  */
		struct Checksums
		{
			struct Checksum
			{
				size_t file_size;
				uint128 file_hash;

				bool is_compressed = false;
				size_t uncompressed_size;
				uint128 uncompressed_hash;

				void checkEqual(const Checksum & rhs, bool have_uncompressed, const String & name) const;
				void checkSize(const String & path) const;
			};

			typedef std::map<String, Checksum> FileChecksums;
			FileChecksums files;

			/// Проверяет, что множество столбцов и их контрольные суммы совпадают. Если нет - бросает исключение.
			/// Если have_uncompressed, для сжатых файлов сравнивает чексуммы разжатых данных. Иначе сравнивает только чексуммы файлов.
			void checkEqual(const Checksums & rhs, bool have_uncompressed) const;

			/// Проверяет, что в директории есть все нужные файлы правильных размеров. Не проверяет чексуммы.
			void checkSizes(const String & path) const;

			/// Сериализует и десериализует в человекочитаемом виде.
			bool readText(ReadBuffer & in); /// Возвращает false, если чексуммы в слишком старом формате.
			void writeText(WriteBuffer & out) const;

			bool empty() const
			{
				return files.empty();
			}

			/// Контрольная сумма от множества контрольных сумм .bin файлов.
			String summaryDataChecksum() const
			{
				SipHash hash;

				/// Пользуемся тем, что итерирование в детерминированном (лексикографическом) порядке.
				for (const auto & it : files)
				{
					const String & name = it.first;
					const Checksum & sum = it.second;
					if (name.size() < strlen(".bin") || name.substr(name.size() - 4) != ".bin")
						continue;
					size_t len = name.size();
					hash.update(reinterpret_cast<const char *>(&len), sizeof(len));
					hash.update(name.data(), len);
					hash.update(reinterpret_cast<const char *>(&sum.uncompressed_size), sizeof(sum.uncompressed_size));
					hash.update(reinterpret_cast<const char *>(&sum.uncompressed_hash), sizeof(sum.uncompressed_hash));
				}

				UInt64 lo, hi;
				hash.get128(lo, hi);
				return DB::toString(lo) + "_" + DB::toString(hi);
			}

			String toString() const
			{
				String s;
				{
					WriteBufferFromString out(s);
					writeText(out);
				}
				return s;
			}

			static Checksums parse(const String & s)
			{
				ReadBufferFromString in(s);
				Checksums res;
				if (!res.readText(in))
					throw Exception("Checksums format is too old", ErrorCodes::FORMAT_VERSION_TOO_OLD);
				assertEOF(in);
				return res;
			}
		};

 		DataPart(MergeTreeData & storage_) : storage(storage_), size(0), size_in_bytes(0), remove_time(0) {}

 		MergeTreeData & storage;

		size_t size;	/// в количестве засечек.
		size_t size_in_bytes; /// размер в байтах, 0 - если не посчитано
		time_t modification_time;
		mutable time_t remove_time; /// Когда кусок убрали из рабочего набора.

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

		void remove() const
		{
			String from = storage.full_path + name + "/";
			String to = storage.full_path + "tmp2_" + name + "/";

			Poco::File(from).renameTo(to);
			Poco::File(to).remove(true);
		}

		/// Переименовывает кусок, дописав к имени префикс.
		void renameAddPrefix(const String & prefix) const
		{
			String from = storage.full_path + name + "/";
			String to = storage.full_path + prefix + name + "/";

			Poco::File f(from);
			f.setLastModified(Poco::Timestamp::fromEpochTime(time(0)));
			f.renameTo(to);
		}

		/// Загрузить индекс и вычислить размер. Если size=0, вычислить его тоже.
		void loadIndex()
		{
			/// Размер - в количестве засечек.
			if (!size)
				size = Poco::File(storage.full_path + name + "/" + escapeForFileName(storage.columns->front().first) + ".mrk")
					.getSize() / MERGE_TREE_MARK_SIZE;

			size_t key_size = storage.sort_descr.size();
			index.resize(key_size * size);

			String index_path = storage.full_path + name + "/primary.idx";
			ReadBufferFromFile index_file(index_path,
				std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));

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
			if (checksums.readText(file))
				assertEOF(file);
			return true;
		}

		void checkNotBroken()
		{
			String path = storage.full_path + name;

			if (!checksums.empty())
			{
				checksums.checkSizes(path + "/");
			}
			else
			{
				/// Проверяем, что первичный ключ непуст.

				Poco::File index_file(path + "/primary.idx");

				if (!index_file.exists() || index_file.getSize() == 0)
					throw Exception("Part " + path + " is broken: primary key is empty.", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

				/// Проверяем, что все засечки непусты и имеют одинаковый размер.

				ssize_t marks_size = -1;
				for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
				{
					Poco::File marks_file(path + "/" + escapeForFileName(it->first) + ".mrk");

					/// При добавлении нового столбца в таблицу файлы .mrk не создаются. Не будем ничего удалять.
					if (!marks_file.exists())
						continue;

					if (marks_size == -1)
					{
						marks_size = marks_file.getSize();

						if (0 == marks_size)
							throw Exception("Part " + path + " is broken: " + marks_file.path() + " is empty.",
								ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
					}
					else
					{
						if (static_cast<ssize_t>(marks_file.getSize()) != marks_size)
							throw Exception("Part " + path + " is broken: marks have different sizes.",
								ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
					}
				}
			}
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
		Aggregating,
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
					const ASTPtr & sampling_expression_, /// nullptr, если семплирование не поддерживается.
					size_t index_granularity_,
					Mode mode_,
					const String & sign_column_,
					const MergeTreeSettings & settings_,
					const String & log_name_);

	std::string getModePrefix() const;

	std::string getSignColumnName() const { return sign_column; }
	bool supportsSampling() const { return !!sampling_expression; }
	bool supportsFinal() const { return !sign_column.empty(); }
	bool supportsPrewhere() const { return true; }

	UInt64 getMaxDataPartIndex();

	std::string getTableName() const { throw Exception("Logical error: calling method getTableName of not a table.",
		ErrorCodes::LOGICAL_ERROR); }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	String getFullPath() const { return full_path; }

	String getLogName() const { return log_name; }

	/** Возвращает копию списка, чтобы снаружи можно было не заботиться о блокировках.
	  */
	DataParts getDataParts();
	DataParts getAllDataParts();

	/** Максимальное количество кусков в одном месяце.
	  */
	size_t getMaxPartsCountForMonth();

	/** Если в таблице слишком много активных кусков, спит некоторое время, чтобы дать им возможность смерджиться.
	  */
	void delayInsertIfNeeded();

	/** Возвращает кусок с указанным именем или кусок, покрывающий его. Если такого нет, возвращает nullptr.
	  * Если including_inactive, просматриваются также неактивные куски (all_data_parts).
	  * При including_inactive, нахождение куска гарантируется только если есть кусок, совпадающий с part_name;
	  *  строго покрывающий кусок в некоторых случаях может не найтись.
	  */
	DataPartPtr getContainingPart(const String & part_name, bool including_inactive = false);

	/** Переименовывает временный кусок в постоянный и добавляет его в рабочий набор.
	  * Если increment!=nullptr, индекс куска берется из инкремента. Иначе индекс куска не меняется.
	  * Предполагается, что кусок не пересекается с существующими.
	  */
	void renameTempPartAndAdd(MutableDataPartPtr part, Increment * increment = nullptr);

	/** То же, что renameTempPartAndAdd, но кусок может покрывать существующие куски.
	  * Удаляет и возвращает все куски, покрытые добавляемым (в возрастающем порядке).
	  */
	DataPartsVector renameTempPartAndReplace(MutableDataPartPtr part, Increment * increment = nullptr);

	/** Переименовывает кусок в prefix_кусок и убирает его из рабочего набора.
	  * Лучше использовать только когда никто не может читать или писать этот кусок
	  *  (например, при инициализации таблицы).
	  */
	void renameAndDetachPart(DataPartPtr part, const String & prefix);

	/** Удаляет кусок из рабочего набора. clearOldParts удалит его файлы, если на него никто не ссылается.
	  */
	void removePart(DataPartPtr part);

	/** Удалить неактуальные куски. Возвращает имена удаленных кусков.
	  */
	Strings clearOldParts();

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

	const ASTPtr primary_expr_ast;

private:
	ExpressionActionsPtr primary_expr;
	SortDescription sort_descr;
	Block primary_key_sample;

	String full_path;

	NamesAndTypesListPtr columns;

	String log_name;
	Logger * log;

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

	void removeColumnFiles(String column_name, bool remove_array_size_files);

	/// Определить, не битые ли данные в директории. Проверяет индекс и засечеки, но не сами данные.
	bool isBrokenPart(const String & path);

	void createConvertExpression(const String & in_column_name, const String & out_type, ExpressionActionsPtr & out_expression, String & out_column);
};

}
