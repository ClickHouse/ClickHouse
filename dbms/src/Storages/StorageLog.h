#pragma once

#include <map>

#include <ext/shared_ptr_helper.hpp>

#include <Poco/File.h>
#include <Poco/RWLock.h>

#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


/** Смещение до каждой некоторой пачки значений.
  * Эти пачки имеют одинаковый размер в разных столбцах.
  * Они нужны, чтобы можно было читать данные в несколько потоков.
  */
struct Mark
{
    size_t rows;    /// Сколько строк содержится в этой пачке и всех предыдущих.
    size_t offset;    /// Смещение до пачки в сжатом файле.
};

using Marks = std::vector<Mark>;


/** Реализует хранилище, подходящее для логов.
  * Ключи не поддерживаются.
  * Данные хранятся в сжатом виде.
  */
class StorageLog : private ext::shared_ptr_helper<StorageLog>, public IStorage
{
friend class ext::shared_ptr_helper<StorageLog>;
friend class LogBlockInputStream;
friend class LogBlockOutputStream;

public:
    /** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
      *  (корректность имён и путей не проверяется)
      *  состоящую из указанных столбцов; создать файлы, если их нет.
      */
    static StoragePtr create(
        const std::string & path_,
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        size_t max_compress_block_size_ = DEFAULT_MAX_COMPRESS_BLOCK_SIZE);

    static StoragePtr create(
        const std::string & path_,
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        size_t max_compress_block_size_ = DEFAULT_MAX_COMPRESS_BLOCK_SIZE);

    std::string getName() const override { return "Log"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    virtual BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    /// Данные столбца
    struct ColumnData
    {
        /// Задает номер столбца в файле с засечками.
        /// Не обязательно совпадает с номером столбца среди столбцов таблицы: здесь нумеруются также столбцы с длинами массивов.
        size_t column_index;

        Poco::File data_file;
        Marks marks;
    };
    using Files_t = std::map<String, ColumnData>;

    bool checkData() const override;

protected:
    String path;
    String name;
    NamesAndTypesListPtr columns;

    Poco::RWLock rwlock;

    StorageLog(
        const std::string & path_,
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        size_t max_compress_block_size_);

    /// Прочитать файлы с засечками, если они ещё не прочитаны.
    /// Делается лениво, чтобы при большом количестве таблиц, сервер быстро стартовал.
    /// Нельзя вызывать с залоченным на запись rwlock.
    void loadMarks();

    /// Можно вызывать при любом состоянии rwlock.
    size_t marksCount();

    BlockInputStreams read(
        size_t from_mark,
        size_t to_mark,
        size_t from_null_mark,
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1);

private:
    Files_t files; /// name -> data

    Names column_names; /// column_index -> name
    Names null_map_filenames;

    Poco::File marks_file;
    Poco::File null_marks_file;

    void loadMarksImpl(bool load_null_marks);

    /// Порядок добавления файлов не должен меняться: он соответствует порядку столбцов в файле с засечками.
    void addFile(const String & column_name, const IDataType & type, size_t level = 0);

    bool loaded_marks;
    bool has_nullable_columns = false;

    size_t max_compress_block_size;
    size_t file_count = 0;
    size_t null_file_count = 0;

protected:
    FileChecker file_checker;

private:
    /** Для обычных столбцов, в засечках указано количество строчек в блоке.
      * Для столбцов-массивов и вложенных структур, есть более одной группы засечек, соответствующих разным файлам:
      *  - для внутренностей (файла name.bin) - указано суммарное количество элементов массивов в блоке,
      *  - для размеров массивов (файла name.size0.bin) - указано количество строчек (самих целых массивов) в блоке.
      *
      * Вернуть первую попавшуюся группу засечек, в которых указано количество строчек, а не внутренностей массивов.
      */
    const Marks & getMarksWithRealRowCount() const;

    std::string getFullPath() const { return path + escapeForFileName(name) + '/';}
};

}
