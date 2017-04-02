#pragma once

#include <map>

#include <ext/shared_ptr_helper.hpp>

#include <Poco/File.h>

#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>


namespace DB
{

/** Реализует хранилище, подходящее для маленьких кусочков лога.
  * Отличается от StorageLog отсутствием файлов с засечками.
  */
class StorageTinyLog : private ext::shared_ptr_helper<StorageTinyLog>, public IStorage
{
friend class ext::shared_ptr_helper<StorageTinyLog>;
friend class TinyLogBlockInputStream;
friend class TinyLogBlockOutputStream;

public:
    /** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
      *  (корректность имён и путей не проверяется)
      *  состоящую из указанных столбцов.
      * Если не указано attach - создать директорию, если её нет.
      */
    static StoragePtr create(
        const std::string & path_,
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        bool attach,
        size_t max_compress_block_size_ = DEFAULT_MAX_COMPRESS_BLOCK_SIZE);

    std::string getName() const override { return "TinyLog"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

    void drop() override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    bool checkData() const override;

    /// Данные столбца
    struct ColumnData
    {
        Poco::File data_file;
    };
    using Files_t = std::map<String, ColumnData>;

    std::string full_path() { return path + escapeForFileName(name) + '/';}

private:
    String path;
    String name;
    NamesAndTypesListPtr columns;

    size_t max_compress_block_size;

    Files_t files;

    FileChecker file_checker;

    Logger * log;

    StorageTinyLog(
        const std::string & path_,
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        bool attach,
        size_t max_compress_block_size_);

    void addFile(const String & column_name, const IDataType & type, size_t level = 0);
};

}
