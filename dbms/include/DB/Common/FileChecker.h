#pragma once

#include <Yandex/logger_useful.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <string>

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

	template <class Iterator>
	void update(const Iterator & begin, const Iterator & end)
	{
		for (auto it = begin; it != end; ++it)
		{
			auto & column_name = *it;
			auto & file = storage.getFiles()[column_name].data_file;
			files_info->setString(column_name + ".size", std::to_string(file.getSize()));
		}
		files_info->save(files_info_path);
	}

	bool check() const
	{
		bool sizes_are_correct = true;
		for (auto & pair : storage.getFiles())
		{
			if (files_info->has(pair.first))
			{
				auto & file = pair.second.data_file;
				size_t expected_size = std::stoull(files_info->getString(pair.first + ".size"));
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
