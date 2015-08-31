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
#include <DB/Common/escapeForFileName.h>
#include <DB/Common/SipHash.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <Poco/RWLock.h>


#define MERGE_TREE_MARK_SIZE (2 * sizeof(size_t))


struct SimpleIncrement;


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
  * - Summing - при склейке кусков, при совпадении PK суммировать все числовые столбцы, не входящие в PK.
  * - Aggregating - при склейке кусков, при совпадении PK, делается слияние состояний столбцов-агрегатных функций.
  * - Unsorted - при склейке кусков, данные не упорядочиваются, а всего лишь конкатенируются;
  *            - это позволяет читать данные ровно такими пачками, какими они были записаны.
  */

/** Этот класс хранит список кусков и параметры структуры данных.
  * Для чтения и изменения данных используются отдельные классы:
  *  - MergeTreeDataSelectExecutor
  *  - MergeTreeDataWriter
  *  - MergeTreeDataMerger
  */

class MergeTreeData : public ITableDeclaration
{
public:
	/// Функция, которую можно вызвать, если есть подозрение, что данные куска испорчены.
	typedef std::function<void (const String &)> BrokenPartCallback;

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

				Checksum() {}
				Checksum(size_t file_size_, uint128 file_hash_) : file_size(file_size_), file_hash(file_hash_) {}
				Checksum(size_t file_size_, uint128 file_hash_, size_t uncompressed_size_, uint128 uncompressed_hash_)
					: file_size(file_size_), file_hash(file_hash_), is_compressed(true),
					uncompressed_size(uncompressed_size_), uncompressed_hash(uncompressed_hash_) {}

				void checkEqual(const Checksum & rhs, bool have_uncompressed, const String & name) const;
				void checkSize(const String & path) const;
			};

			typedef std::map<String, Checksum> FileChecksums;
			FileChecksums files;

			void addFile(const String & file_name, size_t file_size, uint128 file_hash)
			{
				files[file_name] = Checksum(file_size, file_hash);
			}

			/// Проверяет, что множество столбцов и их контрольные суммы совпадают. Если нет - бросает исключение.
			/// Если have_uncompressed, для сжатых файлов сравнивает чексуммы разжатых данных. Иначе сравнивает только чексуммы файлов.
			void checkEqual(const Checksums & rhs, bool have_uncompressed) const;

			/// Проверяет, что в директории есть все нужные файлы правильных размеров. Не проверяет чексуммы.
			void checkSizes(const String & path) const;

			/// Сериализует и десериализует в человекочитаемом виде.
			bool read(ReadBuffer & in); /// Возвращает false, если чексуммы в слишком старом формате.
			bool read_v2(ReadBuffer & in);
			bool read_v3(ReadBuffer & in);
			bool read_v4(ReadBuffer & in);
			void write(WriteBuffer & out) const;

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
					write(out);
				}
				return s;
			}

			static Checksums parse(const String & s)
			{
				ReadBufferFromString in(s);
				Checksums res;
				if (!res.read(in))
					throw Exception("Checksums format is too old", ErrorCodes::FORMAT_VERSION_TOO_OLD);
				assertEOF(in);
				return res;
			}
		};

		DataPart(MergeTreeData & storage_) : storage(storage_) {}

 		MergeTreeData & storage;

		size_t size = 0;				/// в количестве засечек.
		volatile size_t size_in_bytes = 0; 	/// размер в байтах, 0 - если не посчитано;
											/// используется из нескольких потоков без блокировок (изменяется при ALTER).
		time_t modification_time = 0;
		mutable time_t remove_time = std::numeric_limits<time_t>::max(); /// Когда кусок убрали из рабочего набора.

		/// Если true, деструктор удалит директорию с куском.
		bool is_temp = false;

		/// Первичный ключ. Всегда загружается в оперативку.
		typedef std::vector<Field> Index;
		Index index;

		/// NOTE Засечки кэшируются в оперативке. См. MarkCache.h.

		Checksums checksums;

		/// Описание столбцов.
		NamesAndTypesList columns;

		using ColumnToSize = std::map<std::string, size_t>;

		/** Блокируется на запись при изменении columns, checksums или любых файлов куска.
		  * Блокируется на чтение при    чтении columns, checksums или любых файлов куска.
		  */
		mutable Poco::RWLock columns_lock;

		/** Берется на все время ALTER куска: от начала записи временных фалов до их переименования в постоянные.
		  * Берется при разлоченном columns_lock.
		  *
		  * NOTE: "Можно" было бы обойтись без этого мьютекса, если бы можно было превращать ReadRWLock в WriteRWLock, не снимая блокировку.
		  * Такое превращение невозможно, потому что создало бы дедлок, если делать его из двух потоков сразу.
		  * Взятие этого мьютекса означает, что мы хотим заблокировать columns_lock на чтение с намерением потом, не
		  *  снимая блокировку, заблокировать его на запись.
		  */
		mutable Poco::FastMutex alter_mutex;

		~DataPart()
		{
			if (is_temp)
			{
				try
				{
					Poco::File dir(storage.full_path + name);
					if (!dir.exists())
						return;

					if (name.substr(0, strlen("tmp")) != "tmp")
					{
						LOG_ERROR(storage.log, "~DataPart() should remove part " << storage.full_path + name
							<< " but its name doesn't start with tmp. Too suspicious, keeping the part.");
						return;
					}

					dir.remove(true);
				}
				catch (...)
				{
					tryLogCurrentException(__PRETTY_FUNCTION__);
				}
			}
		}

		/// Вычисляем суммарный размер всей директории со всеми файлами
		static size_t calcTotalSize(const String & from)
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

			Poco::File from_dir{from};
			Poco::File to_dir{to};

			if (to_dir.exists())
			{
				LOG_WARNING(storage.log, "Directory " << to << " (to which part must be renamed before removing) already exists."
					" Most likely this is due to unclean restart. Removing it.");

				try
				{
					to_dir.remove(true);
				}
				catch (...)
				{
					LOG_ERROR(storage.log, "Cannot remove directory " << to << ". Check owner and access rights.");
					throw;
				}
			}

			try
			{
				from_dir.renameTo(to);
			}
			catch (const Poco::FileNotFoundException & e)
			{
				/// Если директория уже удалена. Такое возможно лишь при ручном вмешательстве.
				LOG_WARNING(storage.log, "Directory " << from << " (part to remove) doesn't exist or one of nested files has gone."
					" Most likely this is due to manual removing. This should be discouraged. Ignoring.");

				return;
			}

			to_dir.remove(true);
		}

		void renameTo(const String & new_name) const
		{
			String from = storage.full_path + name + "/";
			String to = storage.full_path + new_name + "/";

			Poco::File f(from);
			f.setLastModified(Poco::Timestamp::fromEpochTime(time(0)));
			f.renameTo(to);
		}

		/// Переименовывает кусок, дописав к имени префикс. to_detached - также перенести в директорию detached.
		void renameAddPrefix(bool to_detached, const String & prefix) const
		{
			unsigned try_no = 0;
			auto dst_name = [&, this] { return (to_detached ? "detached/" : "") + prefix + name + (try_no ? "_try" + toString(try_no) : ""); };

			if (to_detached)
			{
				/** Если нужно отцепить кусок, и директория, в которую мы хотим его переименовать, уже существует,
				  *  то будем переименовывать в директорию с именем, в которое добавлен суффикс в виде "_tryN".
				  * Это делается только в случае to_detached, потому что считается, что в этом случае, точное имя не имеет значения.
				  * Больше 10 попыток не делается, чтобы не оставалось слишком много мусорных директорий.
				  */
				while (try_no < 10 && Poco::File(dst_name()).exists())
				{
					LOG_WARNING(storage.log, "Directory " << dst_name() << " (to detach to) is already exist."
						" Will detach to directory with '_tryN' suffix.");
					++try_no;
				}
			}

			renameTo(dst_name());
		}

		/// Загрузить индекс и вычислить размер. Если size=0, вычислить его тоже.
		void loadIndex()
		{
			/// Размер - в количестве засечек.
			if (!size)
			{
				if (columns.empty())
					throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

				size = Poco::File(storage.full_path + name + "/" + escapeForFileName(columns.front().name) + ".mrk")
					.getSize() / MERGE_TREE_MARK_SIZE;
			}

			size_t key_size = storage.sort_descr.size();

			if (key_size)
			{
				index.resize(key_size * size);

				String index_path = storage.full_path + name + "/primary.idx";
				ReadBufferFromFile index_file(index_path,
					std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));

				for (size_t i = 0; i < size; ++i)
					for (size_t j = 0; j < key_size; ++j)
						storage.primary_key_sample.getByPosition(j).type->deserializeBinary(index[i * key_size + j], index_file);

				if (!index_file.eof())
					throw Exception("index file " + index_path + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);
			}

			size_in_bytes = calcTotalSize(storage.full_path + name + "/");
		}

		/// Прочитать контрольные суммы, если есть.
		void loadChecksums(bool require)
		{
			String path = storage.full_path + name + "/checksums.txt";
			if (!Poco::File(path).exists())
			{
				if (require)
					throw Exception("No checksums.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

				return;
			}
			ReadBufferFromFile file(path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
			if (checksums.read(file))
				assertEOF(file);
		}

		void accumulateColumnSizes(ColumnToSize & column_to_size) const
		{
			Poco::ScopedReadRWLock part_lock(columns_lock);
			for (const NameAndTypePair & column : *storage.columns)
				if (Poco::File(storage.full_path + name + "/" + escapeForFileName(column.name) + ".bin").exists())
					column_to_size[column.name] += Poco::File(storage.full_path + name + "/" + escapeForFileName(column.name) + ".bin").getSize();
		}

		void loadColumns(bool require)
		{
			String path = storage.full_path + name + "/columns.txt";
			if (!Poco::File(path).exists())
			{
				if (require)
					throw Exception("No columns.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

				/// Если нет файла со списком столбцов, запишем его.
				for (const NameAndTypePair & column : *storage.columns)
				{
					if (Poco::File(storage.full_path + name + "/" + escapeForFileName(column.name) + ".bin").exists())
						columns.push_back(column);
				}

				if (columns.empty())
					throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

				{
					WriteBufferFromFile out(path + ".tmp", 4096);
					columns.writeText(out);
				}
				Poco::File(path + ".tmp").renameTo(path);

				return;
			}

			ReadBufferFromFile file(path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
			columns.readText(file);
		}

		void checkNotBroken(bool require_part_metadata)
		{
			String path = storage.full_path + name;

			if (!checksums.empty())
			{
				if (!storage.sort_descr.empty() && !checksums.files.count("primary.idx"))
					throw Exception("No checksum for primary.idx", ErrorCodes::NO_FILE_IN_DATA_PART);

				if (require_part_metadata)
				{
					for (const NameAndTypePair & it : columns)
					{
						String name = escapeForFileName(it.name);
						if (!checksums.files.count(name + ".mrk") ||
							!checksums.files.count(name + ".bin"))
							throw Exception("No .mrk or .bin file checksum for column " + name, ErrorCodes::NO_FILE_IN_DATA_PART);
					}
				}

				checksums.checkSizes(path + "/");
			}
			else
			{
				if (!storage.sort_descr.empty())
				{
					/// Проверяем, что первичный ключ непуст.
					Poco::File index_file(path + "/primary.idx");

					if (!index_file.exists() || index_file.getSize() == 0)
						throw Exception("Part " + path + " is broken: primary key is empty.", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
				}

				/// Проверяем, что все засечки непусты и имеют одинаковый размер.

				ssize_t marks_size = -1;
				for (const NameAndTypePair & it : columns)
				{
					Poco::File marks_file(path + "/" + escapeForFileName(it.name) + ".mrk");

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

		bool hasColumnFiles(const String & column) const
		{
			String escaped_column = escapeForFileName(column);
			return Poco::File(storage.full_path + name + "/" + escaped_column + ".bin").exists() &&
			       Poco::File(storage.full_path + name + "/" + escaped_column + ".mrk").exists();
		}
	};

	typedef std::shared_ptr<DataPart> MutableDataPartPtr;
	/// После добавление в рабочее множество DataPart нельзя изменять.
	typedef std::shared_ptr<const DataPart> DataPartPtr;
	struct DataPartPtrLess { bool operator() (const DataPartPtr & lhs, const DataPartPtr & rhs) const { return *lhs < *rhs; } };
	typedef std::set<DataPartPtr, DataPartPtrLess> DataParts;
	typedef std::vector<DataPartPtr> DataPartsVector;


	/// Некоторые операции над множеством кусков могут возвращать такой объект.
	/// Если не был вызван commit, деструктор откатывает операцию.
	class Transaction : private boost::noncopyable
	{
	public:
		Transaction() {}

		void commit()
		{
			data = nullptr;
			removed_parts.clear();
			added_parts.clear();
		}

		~Transaction()
		{
			try
			{
				if (data && (!removed_parts.empty() || !added_parts.empty()))
				{
					LOG_DEBUG(data->log, "Undoing transaction");
					data->replaceParts(removed_parts, added_parts, true);
				}
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
		DataPartsVector removed_parts;
		DataPartsVector added_parts;
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
		Poco::ScopedLockWithUnlock<Poco::FastMutex> alter_lock;

		DataPart::Checksums new_checksums;
		NamesAndTypesList new_columns;
		/// Если значение - пустая строка, файл нужно удалить, и он не временный.
		NameToNameMap rename_map;
	};

	typedef std::unique_ptr<AlterDataPartTransaction> AlterDataPartTransactionPtr;

	/// Режим работы. См. выше.
	enum Mode
	{
		Ordinary,
		Collapsing,
		Summing,
		Aggregating,
		Unsorted,
	};

	static void doNothing(const String & name) {}

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
					const Context & context_,
					ASTPtr & primary_expr_ast_,
					const String & date_column_name_,
					const ASTPtr & sampling_expression_, /// nullptr, если семплирование не поддерживается.
					size_t index_granularity_,
					Mode mode_,
					const String & sign_column_,			/// Для Collapsing режима.
					const Names & columns_to_sum_,			/// Для Summing режима. Если пустое - то выбирается автоматически.
					const MergeTreeSettings & settings_,
					const String & log_name_,
					bool require_part_metadata_,
					BrokenPartCallback broken_part_callback_ = &MergeTreeData::doNothing);

	/// Загрузить множество кусков с данными с диска. Вызывается один раз - сразу после создания объекта.
	void loadDataParts(bool skip_sanity_checks);

	std::string getModePrefix() const;

	bool supportsSampling() const { return !!sampling_expression; }
	bool supportsPrewhere() const { return true; }

	bool supportsFinal() const
	{
		return mode == Mode::Collapsing
			|| mode == Mode::Summing
			|| mode == Mode::Aggregating;
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
			return NameAndTypePair("_part", new DataTypeString);
		if (column_name == "_part_index")
			return NameAndTypePair("_part_index", new DataTypeUInt64);
		return ITableDeclaration::getColumn(column_name);
	}

	bool hasColumn(const String & column_name) const override
	{
		if (column_name == "_part")
			return true;
		if (column_name == "_part_index")
			return true;
		return ITableDeclaration::hasColumn(column_name);
	}

	String getFullPath() const { return full_path; }

	String getLogName() const { return log_name; }

	/** Возвращает копию списка, чтобы снаружи можно было не заботиться о блокировках.
	  */
	DataParts getDataParts();
	DataPartsVector getDataPartsVector();
	DataParts getAllDataParts();

	/** Размер активной части в количестве байт.
	  */
	size_t getTotalActiveSizeInBytes();

	/** Максимальное количество кусков в одном месяце.
	  */
	size_t getMaxPartsCountForMonth();

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

	/** Переименовывает временный кусок в постоянный и добавляет его в рабочий набор.
	  * Если increment != nullptr, индекс куска берется из инкремента. Иначе индекс куска не меняется.
	  * Предполагается, что кусок не пересекается с существующими.
	  * Если out_transaction не nullptr, присваивает туда объект, позволяющий откатить добавление куска (но не переименование).
	  */
	void renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr);

	/** То же, что renameTempPartAndAdd, но кусок может покрывать существующие куски.
	  * Удаляет и возвращает все куски, покрытые добавляемым (в возрастающем порядке).
	  */
	DataPartsVector renameTempPartAndReplace(MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr);

	/** Убирает из рабочего набора куски remove и добавляет куски add. add должны уже быть в all_data_parts.
	  * Если clear_without_timeout, данные будут удалены при следующем clearOldParts, игнорируя old_parts_lifetime.
	  */
	void replaceParts(const DataPartsVector & remove, const DataPartsVector & add, bool clear_without_timeout);

	/** Добавляет новый кусок в список известных кусков и в рабочий набор.
	  */
	void attachPart(const DataPartPtr & part);

	/** Переименовывает кусок в detached/prefix_кусок и забывает про него. Данные не будут удалены в clearOldParts.
	  * Если restore_covered, добавляет в рабочий набор неактивные куски, слиянием которых получен удаляемый кусок.
	  */
	void renameAndDetachPart(const DataPartPtr & part, const String & prefix = "", bool restore_covered = false, bool move_to_detached = true);

	/** Убирает кусок из списка кусков (включая all_data_parts), но не перемещщает директорию.
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
	AlterDataPartTransactionPtr alterDataPart(const DataPartPtr & part, const NamesAndTypesList & new_columns, bool skip_sanity_checks = false);

	/// Нужно вызывать под залоченным lockStructureForAlter().
	void setColumnsList(const NamesAndTypesList & new_columns) { columns = new NamesAndTypesList(new_columns); }

	/// Нужно вызвать, если есть подозрение, что данные куска испорчены.
	void reportBrokenPart(const String & name)
	{
		broken_part_callback(name);
	}

	ExpressionActionsPtr getPrimaryExpression() const { return primary_expr; }
	SortDescription getSortDescription() const { return sort_descr; }

	/// Проверить, что кусок не сломан и посчитать для него чексуммы, если их нет.
	MutableDataPartPtr loadPartAndFixMetadata(const String & relative_path);

	/** Сделать локальный бэкап (снэпшот) для кусков, начинающихся с указанного префикса.
	  * Бэкап создаётся в директории clickhouse_dir/shadow/i/, где i - инкрементное число.
	  */
	void freezePartition(const std::string & prefix);

	size_t getColumnSize(const std::string & name) const
	{
		Poco::ScopedLock<Poco::FastMutex> lock{data_parts_mutex};

		const auto it = column_sizes.find(name);
		return it == std::end(column_sizes) ? 0 : it->second;
	}

	using ColumnSizes = std::unordered_map<std::string, size_t>;
	ColumnSizes getColumnSizes() const
	{
		Poco::ScopedLock<Poco::FastMutex> lock{data_parts_mutex};
		return column_sizes;
	}

	/// Для ATTACH/DETACH/DROP PARTITION.
	static String getMonthName(const Field & partition);
	static DayNum_t getMonthDayNum(const Field & partition);

	const Context & context;
	const String date_column_name;
	const ASTPtr sampling_expression;
	const size_t index_granularity;

	/// Режим работы - какие дополнительные действия делать при мердже.
	const Mode mode;
	/// Для схлопывания записей об изменениях, если используется Collapsing режим работы.
	const String sign_column;
	/// Для суммирования, если используется Summing режим работы.
	const Names columns_to_sum;

	const MergeTreeSettings settings;

	const ASTPtr primary_expr_ast;
	Block primary_key_sample;

private:
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
	mutable Poco::FastMutex data_parts_mutex;

	/** Множество всех кусков с данными, включая уже слитые в более крупные, но ещё не удалённые. Оно обычно небольшое (десятки элементов).
	  * Ссылки на кусок есть отсюда, из списка актуальных кусков и из каждого потока чтения, который его сейчас использует.
	  * То есть, если количество ссылок равно 1 - то кусок не актуален и не используется прямо сейчас, и его можно удалить.
	  */
	DataParts all_data_parts;
	Poco::FastMutex all_data_parts_mutex;

	/** Выражение, преобразующее типы столбцов.
	  * Если преобразований типов нет, out_expression=nullptr.
	  * out_rename_map отображает файлы-столбцы на выходе выражения в новые файлы таблицы.
	  * Файлы, которые нужно удалить, в out_rename_map отображаются в пустую строку.
	  * Если !part, просто проверяет, что все нужные преобразования типов допустимы.
	  */
	void createConvertExpression(const DataPartPtr & part, const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns,
		ExpressionActionsPtr & out_expression, NameToNameMap & out_rename_map);

	/// Рассчитывает размеры столбцов в сжатом виде для текущего состояния data_parts
	void calculateColumnSizes();
	/// Добавляет или вычитывает вклад part в размеры столбцов в сжатом виде
	void addPartContributionToColumnSizes(const DataPartPtr & part);
	void removePartContributionToColumnSizes(const DataPartPtr & part);
};

}
