#pragma once

#include <string>
#include <Yandex/logger_useful.h>
#include <statdaemons/JSON.h>
#include <Poco/Path.h>
#include <Poco/File.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/copyData.h>
#include <DB/Common/escapeForFileName.h>


namespace DB
{

/// хранит размеры всех столбцов, и может проверять не побились ли столбцы
class FileChecker
{
private:
	/// Имя файла -> размер.
	using Map = std::map<std::string, size_t>;

public:
	FileChecker(const std::string & file_info_path_) :
		files_info_path(file_info_path_)
	{
		Poco::Path path(files_info_path);
		tmp_files_info_path = path.parent().toString() + "tmp_" + path.getFileName();
	}

	void setPath(const std::string & file_info_path_)
	{
		files_info_path = file_info_path_;
	}

	using Files = std::vector<Poco::File>;

	void update(const Poco::File & file)
	{
		initialize();
		updateImpl(file);
		save();
	}

	void update(const Files::iterator & begin, const Files::iterator & end)
	{
		initialize();
		for (auto it = begin; it != end; ++it)
			updateImpl(*it);
		save();
	}

	/// Проверяем файлы, параметры которых указаны в sizes.json
	bool check() const
	{
		/** Читаем файлы заново при каждом вызове check - чтобы не нарушать константность.
		  * Метод check вызывается редко.
		  */
		Map local_map;
		load(local_map);

		if (local_map.empty())
			return true;

		for (const auto & name_size : local_map)
		{
			Poco::File file(Poco::Path(files_info_path).parent().toString() + "/" + name_size.first);
			if (!file.exists())
			{
				LOG_ERROR(log, "File " << file.path() << " doesn't exist");
				return false;
			}

			size_t real_size = file.getSize();
			if (real_size != name_size.second)
			{
				LOG_ERROR(log, "Size of " << file.path() << " is wrong. Size is " << real_size << " but should be " << name_size.second);
				return false;
			}
		}

		return true;
	}

private:
	void initialize()
	{
		if (initialized)
			return;

		load(map);
		initialized = true;
	}

	void updateImpl(const Poco::File & file)
	{
		map[Poco::Path(file.path()).getFileName()] = file.getSize();
	}

	void save() const
	{
		{
			WriteBufferFromFile out(tmp_files_info_path);

			/// Столь сложная структура JSON-а - для совместимости со старым форматом.
			writeCString("{\"yandex\":{", out);

			for (auto it = map.begin(); it != map.end(); ++it)
			{
				if (it != map.begin())
					writeString(",", out);

				/// escapeForFileName на самом деле не нужен. Но он оставлен для совместимости со старым кодом.
				writeJSONString(escapeForFileName(it->first), out);
				writeString(":{\"size\":\"", out);
				writeIntText(it->second, out);
				writeString("\"}", out);
			}

			writeCString("}}", out);
			out.next();
		}

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

	void load(Map & map) const
	{
		map.clear();

		if (!Poco::File(files_info_path).exists())
			return;

		std::string content;
		{
			ReadBufferFromFile in(files_info_path);
			WriteBufferFromString out(content);

			/// Библиотека JSON не поддерживает пробельные символы. Удаляем их. Неэффективно.
			while (!in.eof())
			{
				char c;
				readChar(c, in);
				if (!isspace(c))
					writeChar(c, out);
			}
		}
		JSON json(content);

		JSON files = json["yandex"];
		for (const auto & name_value : files)
			map[unescapeForFileName(name_value.getName())] = name_value.getValue()["size"].toUInt();
	}

	std::string files_info_path;
	std::string tmp_files_info_path;

	/// Данные из файла читаются лениво.
	Map map;
	bool initialized = false;

	Logger * log = &Logger::get("FileChecker");
};
}
