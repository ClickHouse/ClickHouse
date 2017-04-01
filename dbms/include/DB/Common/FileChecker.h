#pragma once

#include <string>
#include <common/logger_useful.h>
#include <Poco/File.h>


namespace DB
{

/// хранит размеры всех столбцов, и может проверять не побились ли столбцы
class FileChecker
{
private:
    /// Имя файла -> размер.
    using Map = std::map<std::string, size_t>;

public:
    using Files = std::vector<Poco::File>;

    FileChecker(const std::string & file_info_path_);
    void setPath(const std::string & file_info_path_);
    void update(const Poco::File & file);
    void update(const Files::const_iterator & begin, const Files::const_iterator & end);

    /// Проверяем файлы, параметры которых указаны в sizes.json
    bool check() const;

private:
    void initialize();
    void updateImpl(const Poco::File & file);
    void save() const;
    void load(Map & map) const;

    std::string files_info_path;
    std::string tmp_files_info_path;

    /// Данные из файла читаются лениво.
    Map map;
    bool initialized = false;

    Logger * log = &Logger::get("FileChecker");
};

}
