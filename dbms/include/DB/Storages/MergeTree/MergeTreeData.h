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

/// NOTE: Следующее пока не правда. Сейчас тут практически весь StorageMergeTree. Лишние части нужно перенести отсюда в StorageMergeTree.

/** Этот класс хранит список кусков и параметры структуры данных.
  * Для чтения и изменения данных используются отдельные классы:
  *  - MergeTreeDataReader
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

class MergeTreeData
{
public:
	/// Описание куска с данными.
	struct DataPart
	{
 		DataPart(MergeTreeData & storage_) : storage(storage_), size_in_bytes(0) {}

 		/// Не изменяйте никакие поля для кусков, уже вставленных в таблицу. TODO заменить почти везде const DataPart.

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
	};

	typedef SharedPtr<DataPart> DataPartPtr;
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

	/**
	  * owning_storage используется только чтобы отдавать его потокам блоков.
	  */
	void setOwningStorage(StoragePtr storage) { owning_storage = storage; }

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

	/** Изменяемая часть описания таблицы. Содержит лок, запрещающий изменение описания таблицы.
	  * Если в течение какой-то операции структура таблицы должна оставаться неизменной, нужно держать один лок на все ее время.
	  * Например, нужно держать такой лок на время всего запроса SELECT или INSERT и на все время слияния набора кусков
	  * (но между выбором кусков для слияния и их слиянием структура таблицы может измениться).
	  * NOTE: Можно перенести сюда другие поля, чтобы сделать их динамически изменяемыми.
	  *       Например, index_granularity, sign_column, primary_expr_ast.
	  * NOTE: Можно вынести эту штуку в IStorage и брать ее в Interpreter-ах,
	  *       чтобы избавиться от оставшихся небольших race conditions.
	  *       Скорее всего, даже можно заменить этой штукой весь механизм отложенного дропа таблиц и убрать owned_storage из потоков блоков.
	  */
	class LockedTableStructure : public IColumnsDeclaration
	{
	public:
		const NamesAndTypesList & getColumnsList() const { return *data.columns; }

		String getFullPath() const { return data.full_path; }

	private:
		friend class MergeTreeData;

		const MergeTreeData & data;
		Poco::SharedPtr<Poco::ScopedReadRWLock> parts_lock;
		Poco::ScopedReadRWLock structure_lock;

		LockedTableStructure(const MergeTreeData & data_, bool lock_writing)
			: data(data_), parts_lock(lock_writing ? new Poco::ScopedReadRWLock(data.parts_writing_lock) : nullptr), structure_lock(data.table_structure_lock) {}
	};

	typedef Poco::SharedPtr<LockedTableStructure> LockedTableStructurePtr;

	/** Если в рамках этого лока будут добавлены или удалены куски данных, обязательно указать will_modify_parts=true.
	  * Это возьмет дополнительный лок, не позволяющий начать ALTER MODIFY.
	  */
	LockedTableStructurePtr getLockedStructure(bool will_modify_parts) const
	{
		return new LockedTableStructure(*this, will_modify_parts);
	}

	typedef Poco::SharedPtr<Poco::ScopedWriteRWLock> TableStructureWriteLockPtr;

	TableStructureWriteLockPtr lockStructure() { return new Poco::ScopedWriteRWLock(table_structure_lock); }

	/** Возвращает копию списка, чтобы снаружи можно было не заботиться о блокировках.
	  */
	DataParts getDataParts();

	/** Удаляет куски old_parts и добавляет кусок new_part. Если какого-нибудь из удаляемых кусков нет, бросает исключение.
	  */
	void replaceParts(DataPartsVector old_parts, DataPartPtr new_part);

	/** Переименовывает временный кусок в постоянный и добавляет его в рабочий набор.
	  * Если increment!=nullptr, индекс куска бурется из инкремента. Иначе индекс куска не меняется.
	  */
	void renameTempPartAndAdd(DataPartPtr part, Increment * increment,
		const LockedTableStructurePtr & structure);

	/** Удалить неактуальные куски.
	  */
	void clearOldParts();

	/** После вызова dropAllData больше ничего вызывать нельзя.
	  */
	void dropAllData();

	/** Поменять путь к директории с данными. Предполагается, что все данные из старой директории туда перенесли.
	  * Нужно вызывать под залоченным lockStructure().
	  */
	void setPath(const String & full_path);

	/** Метод ALTER позволяет добавлять и удалять столбцы и менять их тип.
	  * Нужно вызывать под залоченным lockStructure().
	  * TODO: сделать, чтобы ALTER MODIFY не лочил чтения надолго. Для этого есть parts_writing_lock.
	  */
	void alter(const ASTAlterQuery::Parameters & params);

	/// Эти поля не нужно изменять снаружи. NOTE нужно спрятать их и сделать методы get*.
	const Context & context;
	String date_column_name;
	ASTPtr sampling_expression;
	size_t index_granularity;

	/// Режим работы - какие дополнительные действия делать при мердже.
	Mode mode;
	/// Для схлопывания записей об изменениях, если используется Collapsing режим работы.
	String sign_column;

	MergeTreeSettings settings;

	ExpressionActionsPtr primary_expr;
	SortDescription sort_descr;
	Block primary_key_sample;

	StorageWeakPtr owning_storage;

private:
	ASTPtr primary_expr_ast;

	String full_path;

	NamesAndTypesListPtr columns;

	Logger * log;
	volatile bool shutdown_called;

	/// Регулярное выражение соответсвующее названию директории с кусочками
	Poco::RegularExpression file_name_regexp;

	/// Брать следующие два лока всегда нужно в этом порядке.

	/** Берется на чтение на все время запроса INSERT и на все время слияния кусков. Берется на запись на все время ALTER MODIFY.
	  *
	  * Формально:
	  * Ввзятие на запись гарантирует, что:
	  *  1) множество кусков не изменится, пока лок жив,
	  *  2) все операции над множеством кусков после отпускания лока будут основаны на структуре таблицы на момент после отпускания лока.
	  * Взятие на чтение обязательно, если будет добавляться или удалять кусок.
	  * Брать на чтение нужно до получения структуры таблицы, которой будет соответствовать добавляемый кусок.
	  */
	mutable Poco::RWLock parts_writing_lock;

	/** Лок для множества столбцов и пути к таблице. Берется на запись в RENAME и ALTER (для ALTER MODIFY ненадолго).
	  *
	  * Взятие этого лока на запись - строго более "сильная" операция, чем взятие parts_writing_lock на запись.
	  * То есть, если этот лок взят на запись, о parts_writing_lock можно не заботиться.
	  * parts_writing_lock нужен только для случаев, когда не хочется брать table_structure_lock надолго.
	  */
	mutable Poco::RWLock table_structure_lock;

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
