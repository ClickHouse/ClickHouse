#pragma once

#include <Yandex/logger_useful.h>
#include <DB/Columns/IColumn.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <string>
#include <Poco/File.h>
#include <DB/Common/escapeForFileName.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

namespace DB
{

/// хранит размеры всех столбцов, и может проверять не побились ли столбцы
template <class Storage>
class FileChecker
{
public:
	FileChecker(const std::string &file_info_path_, Storage & storage_) :
		files_info_path(file_info_path_), files_info(), storage(storage_), log(&Logger::get("FileChecker"))
	{
		if (Poco::File(files_info_path).exists())
			boost::property_tree::read_xml(files_info_path, files_info);
	}

	void setPath(const std::string & file_info_path_)
	{
		files_info_path = file_info_path_;
	}

	using Files = std::vector<Poco::File>;

	void update(const Files::iterator & begin, const Files::iterator & end)
	{
		for (auto it = begin; it != end; ++it)
			files_info.put(std::string("yandex.") + escapeForFileName(Poco::Path(it->path()).getFileName()) + ".size", std::to_string(it->getSize()));

		boost::property_tree::write_xml(files_info_path, files_info, std::locale(),
										boost::property_tree::xml_parser::xml_writer_settings<char>('\t', 1));
	}

	bool check(const Files::iterator & begin, const Files::iterator & end) const
	{
		bool sizes_are_correct = true;
		for (auto it = begin; it != end; ++it)
		{
			auto & file = *it;
			std::string filename = escapeForFileName(Poco::Path(it->path()).getFileName());
			auto file_size = files_info.get_optional<std::string>(std::string("yandex.") + filename + ".size");
			if (file_size)
			{
				size_t expected_size = std::stoull(*file_size);
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

	using PropertyTree = boost::property_tree::ptree;
	PropertyTree files_info;

	Storage & storage;
	Logger * log;
};
}
