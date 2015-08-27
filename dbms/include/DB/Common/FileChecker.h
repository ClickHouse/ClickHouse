#pragma once

#include <Yandex/logger_useful.h>
#include <DB/Columns/IColumn.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Path.h>
#include <string>
#include <Poco/File.h>
#include <DB/Common/escapeForFileName.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace DB
{

/// хранит размеры всех столбцов, и может проверять не побились ли столбцы
class FileChecker
{
public:
	FileChecker(const std::string & file_info_path_) :
		files_info_path(file_info_path_), files_info(), log(&Logger::get("FileChecker"))
	{
		Poco::Path path(files_info_path);
		tmp_files_info_path = path.parent().toString() + "tmp_" + path.getFileName();

		if (Poco::File(files_info_path).exists())
			boost::property_tree::read_json(files_info_path, files_info);
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
		if (!files_info.empty())
			for (auto & node : files_info.get_child("yandex"))
			{
				std::string filename = unescapeForFileName(node.first);
				size_t expected_size = std::stoull(node.second.template get<std::string>("size"));

				Poco::File file(Poco::Path(files_info_path).parent().toString() + "/" + filename);
				if (!file.exists())
				{
					LOG_ERROR(log, "File " << file.path() << " doesn't exist");
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
		files_info.put(std::string("yandex.") + escapeForFileName(Poco::Path(file.path()).getFileName()) + ".size", std::to_string(file.getSize()));
	}

	void saveTree()
	{
		boost::property_tree::write_json(tmp_files_info_path, files_info, std::locale());

		Poco::File current_file(files_info_path);

		if (current_file.exists())
		{
			std::string old_file_name = files_info_path + ".old";
			current_file.renameTo(old_file_name);
			Poco::File(tmp_files_info_path).renameTo(files_info_path);
			Poco::File(old_file_name).remove();
		}
		else
			Poco::File(tmp_files_info_path).renameTo(files_info_path);
	}

	std::string files_info_path;
	std::string tmp_files_info_path;

	using PropertyTree = boost::property_tree::ptree;
	PropertyTree files_info;

	Logger * log;
};
}
