#pragma once

#include <Yandex/logger_useful.h>
#include <DB/Columns/IColumn.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <string>
#include <Poco/File.h>
#include <DB/Common/escapeForFileName.h>

namespace DB
{

/// хранит размеры всех столбцов, и может проверять не побились ли столбцы
template <class Storage>
class FileChecker
{
public:
	FileChecker(const std::string &file_info_path_, Storage & storage_) :
		files_info_path(file_info_path_), files_info(new Poco::Util::XMLConfiguration), storage(storage_), log(&Logger::get("FileChecker"))
	{
		try
		{
			files_info->load(files_info_path);
		}
		catch (Poco::FileNotFoundException & e)
		{
			files_info->loadEmpty("yandex");
		}
	}

	void setPath(const std::string & file_info_path_)
	{
		files_info_path = file_info_path_;
	}

	using Files = std::vector<Poco::File>;

	void update(const Files::iterator & begin, const Files::iterator & end)
	{
		for (auto it = begin; it != end; ++it)
			files_info->setString(escapeForFileName(Poco::Path(it->path()).getFileName()) + ".size", std::to_string(it->getSize()));

		files_info->save(files_info_path);
	}

	bool check(const Files::iterator & begin, const Files::iterator & end) const
	{
		bool sizes_are_correct = true;
		for (auto it = begin; it != end; ++it)
		{
			auto & file = *it;
			std::string filename = escapeForFileName(Poco::Path(it->path()).getFileName());
			if (files_info->has(filename))
			{
				size_t expected_size = std::stoull(files_info->getString(filename + ".size"));
				size_t real_size = file.getSize();
				if (real_size != expected_size)
				{
					LOG_ERROR(log, "Size of " << file.path() << "is wrong. Size is " << real_size << " but should be " << expected_size);
					sizes_are_correct = false;
				}
			}
		}
		return sizes_are_correct;
	}

private:
	std::string files_info_path;

	using FileInfo = Poco::AutoPtr<Poco::Util::XMLConfiguration>;
	FileInfo files_info;

	Storage & storage;
	Logger * log;
};

}
