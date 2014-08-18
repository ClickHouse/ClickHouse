#pragma once

#include <Yandex/logger_useful.h>
#include <DB/Columns/IColumn.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Path.h>
#include <string>
#include <Poco/File.h>
#include <DB/Common/escapeForFileName.h>
#include <jsonxx.h>
#include <fstream>

namespace DB
{

/// хранит размеры всех столбцов, и может проверять не побились ли столбцы
template <class Storage>
class FileChecker
{
public:
	FileChecker(const std::string &file_info_path_, Storage & storage_) :
		files_info_path(file_info_path_), storage(storage_), log(&Logger::get("FileChecker"))
	{
		Poco::Path path(files_info_path);
		tmp_files_info_path = path.parent().toString() + "tmp_" + path.getFileName();

		std::ifstream istr(files_info_path);
		files_info.parse(istr);
	}

	void setPath(const std::string & file_info_path_)
	{
		files_info_path = file_info_path_;
	}

	using Files = std::vector<Poco::File>;

	void update(const Poco::File & file)
	{
		updateTree(file);
		saveTree();
	}

	void update(const Files::iterator & begin, const Files::iterator & end)
	{
		for (auto it = begin; it != end; ++it)
			updateTree(*it);
		saveTree();
	}

	/// Проверяем файлы, параметры которых указаны в sizes.json
	bool check() const
	{
		bool correct = true;
		for (auto & node : files_info.kv_map())
		{
			std::string filename = unescapeForFileName(node.first);
			size_t expected_size = std::stoull(node.second->get<jsonxx::Object>().get<std::string>("size"));

			Poco::File file(Poco::Path(files_info_path).parent().toString() + "/" + filename);
			if (!file.exists())
			{
				LOG_ERROR(log, "File " << file.path() << " doesn't exists");
				correct = false;
				continue;
			}

			size_t real_size = file.getSize();
			if (real_size != expected_size)
			{
				LOG_ERROR(log, "Size of " << file.path() << " is wrong. Size is " << real_size << " but should be " << expected_size);
				correct = false;
			}
		}
		return correct;
	}

private:
	void updateTree(const Poco::File & file)
	{
		files_info.import(escapeForFileName(Poco::Path(file.path()).getFileName()),
											jsonxx::Object("size", std::to_string(file.getSize())));
	}

	void saveTree()
	{
		std::ofstream file(tmp_files_info_path, std::ofstream::trunc);
		file  << files_info.write(jsonxx::JSON);
		file.close();

		std::string old_file_name = files_info_path + ".old";
		Poco::File new_file(files_info_path);
		if (new_file.exists())
		{
			new_file.renameTo(old_file_name);
			Poco::File(tmp_files_info_path).renameTo(files_info_path);
			Poco::File(old_file_name).remove();
		}
		else
			Poco::File(tmp_files_info_path).renameTo(files_info_path);
	}

	std::string files_info_path;
	std::string tmp_files_info_path;

	jsonxx::Object files_info;

	Storage & storage;
	Logger * log;
};
}
