#pragma once

#include <dlfcn.h>
#include <stdio.h>

#include <string>
#include <mutex>
#include <functional>
#include <unordered_set>
#include <unordered_map>

#include <Poco/DirectoryIterator.h>

#include <Yandex/Revision.h>
#include <Yandex/logger_useful.h>
#include <statdaemons/threadpool.hpp>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Common/SipHash.h>
#include <DB/Common/UInt128.h>
#include <DB/IO/Operators.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/copyData.h>
#include <DB/IO/WriteBufferFromFile.h>


namespace DB
{


/** Позволяет открыть динамическую библиотеку и получить из неё указатель на функцию.
  */
class SharedLibrary : private boost::noncopyable
{
public:
	SharedLibrary(const std::string & path)
	{
		handle = dlopen(path.c_str(), RTLD_LAZY);
		if (!handle)
			throw Exception(std::string("Cannot dlopen: ") + dlerror());
	}

	~SharedLibrary()
	{
		if (handle && dlclose(handle))
			throw Exception("Cannot dlclose");
	}

	template <typename Func>
	Func get(const std::string & name)
	{
		dlerror();

		Func res = reinterpret_cast<Func>(dlsym(handle, name.c_str()));

		if (char * error = dlerror())
			throw Exception(std::string("Cannot dlsym: ") + error);

		return res;
	}

private:
	void * handle;
};


/** Позволяет скомпилировать кусок кода, использующий заголовочные файлы сервера, в динамическую библиотеку.
  * Ведёт статистику вызовов, и инициирует компиляцию только на N-ый по счёту вызов для одного ключа.
  * Компиляция выполняется асинхронно, в отдельных потоках, если есть свободные потоки.
  * NOTE: Нет очистки устаревших и ненужных результатов.
  */
class Compiler
{
public:
	/** path - путь к директории с временными файлами - результатами компиляции.
	  * Результаты компиляции сохраняются при перезапуске сервера,
	  *  но используют в качестве части ключа номер ревизии. То есть, устаревают при обновлении сервера.
	  */
	Compiler(const std::string & path_, size_t threads)
		: path(path_), pool(threads)
	{
		Poco::DirectoryIterator dir_end;
		for (Poco::DirectoryIterator dir_it(path); dir_end != dir_it; ++dir_it)
		{
			std::string name = dir_it.name();
			if (name.length() > strlen(".so") && 0 == name.compare(name.size() - 3, 3, ".so"))
			{
				files.insert(name.substr(0, name.size() - 3));
			}
		}

		LOG_INFO(log, "Having " << files.size() << " compiled files from previous start.");
	}

	~Compiler()
	{
 		LOG_DEBUG(log, "Waiting for threads to finish.");
		pool.wait();
	}

	/** Увеличить счётчик для заданного ключа key на единицу.
	  * Если результат компиляции уже есть (уже открыт, или есть файл с библиотекой),
	  *  то вернуть готовую SharedLibrary.
	  * Иначе, если счётчик достиг min_count_to_compile,
	  *  инициировать компиляцию в отдельном потоке, если есть свободные потоки, и вернуть nullptr.
	  * Иначе вернуть nullptr.
	  */
	std::shared_ptr<SharedLibrary> getOrCount(
		const std::string & key,
		UInt32 min_count_to_compile,
		std::function<std::string()> get_code)
	{
		HashedKey hashed_key = getHash(key);

		std::lock_guard<std::mutex> lock(mutex);

		UInt32 count = ++counts[hashed_key];

		/// Есть готовая открытая библиотека? Или, если библиотека в процессе компиляции, там будет nullptr.
		Libraries::iterator it = libraries.find(hashed_key);
		if (libraries.end() != it)
		{
			if (!it->second)
				LOG_INFO(log, "Library " << hashedKeyToFileName(hashed_key) << " is compiling.");

			return it->second;
		}

		/// Есть файл с библиотекой, оставшийся от предыдущего запуска?
		std::string file_name = hashedKeyToFileName(hashed_key);
		if (files.count(file_name))
			return libraries.emplace(std::piecewise_construct,
				std::forward_as_tuple(hashed_key),
				std::forward_as_tuple(new SharedLibrary(path + '/' + file_name + ".so"))).first->second;

		/// Достигнуто ли min_count_to_compile?
		if (count >= min_count_to_compile)
		{
			/// Есть ли свободные потоки.
			if (pool.active() < pool.size())
			{
				/// Обозначает, что библиотека в процессе компиляции.
				libraries[hashed_key] = nullptr;

				pool.schedule([=]
				{
					try
					{
						compile(hashed_key, file_name, get_code);
					}
					catch (...)
					{
						tryLogCurrentException("Compiler");
					}
				});
			}
			else
				LOG_INFO(log, "All threads are busy.");
		}

		return nullptr;
	}


private:
	using HashedKey = UInt128;
	using Counts = std::unordered_map<HashedKey, UInt32, UInt128Hash>;
	using Libraries = std::unordered_map<HashedKey, std::shared_ptr<SharedLibrary>, UInt128Hash>;
	using Files = std::unordered_set<std::string>;

	const std::string path;
	boost::threadpool::pool pool;

	/// Количество вызовов функции getOrCount.
	Counts counts;

	/// Скомпилированные и открытые библиотеки. Или nullptr для библиотек в процессе компиляции.
	Libraries libraries;

	/// Скомпилированные файлы, оставшиеся от предыдущих запусков, но ещё не открытые.
	Files files;

	std::mutex mutex;

	Logger * log = &Logger::get("Compiler");


	static HashedKey getHash(const std::string & key)
	{
		SipHash hash;

		auto revision = Revision::get();
		hash.update(reinterpret_cast<const char *>(&revision), sizeof(revision));
		hash.update(key.data(), key.size());

		HashedKey res;
		hash.get128(res.first, res.second);
		return res;
	}

	/// Без расширения .so.
	static std::string hashedKeyToFileName(HashedKey hashed_key)
	{
		std::string file_name;

		{
			WriteBufferFromString out(file_name);
			out << hashed_key.first << '_' << hashed_key.second;
		}

		return file_name;
	}

	static HashedKey fileNameToHashedKey(const std::string & file_name)
	{
		HashedKey hashed_key;

		{
			ReadBufferFromString in(file_name);
			in >> hashed_key.first >> "_" >> hashed_key.second;
		}

		return hashed_key;
	}

	struct Pipe : private boost::noncopyable
	{
		FILE * f;

		Pipe(const std::string & command)
		{
			errno = 0;
			f = popen(command.c_str(), "r");

			if (!f)
				throwFromErrno("Cannot popen");
		}

		~Pipe()
		{
			try
			{
				errno = 0;
				if (f && -1 == pclose(f))
					throwFromErrno("Cannot pclose");
			}
			catch (...)
			{
				tryLogCurrentException("Pipe");
			}
		}
	};

	void compile(HashedKey hashed_key, std::string file_name, std::function<std::string()> get_code)
	{
		LOG_INFO(log, "Compiling code " << file_name);

		std::string prefix = path + "/" + file_name;
		std::string cpp_file_path = prefix + ".cpp";
		std::string so_file_path = prefix + ".so";

		{
			WriteBufferFromFile out(cpp_file_path);
			out << get_code();
		}

		std::stringstream command;

		/// Слегка неудобно.
		command <<
			"/usr/share/clickhouse/bin/clang"
			" -x c++ -std=gnu++11 -O3 -g -Wall -Werror -Wnon-virtual-dtor -march=native -D NDEBUG"
			" -shared -fPIC -fvisibility=hidden -fno-implement-inlines"
			" -isystem /usr/share/clickhouse/headers/usr/local/include/"
			" -isystem /usr/share/clickhouse/headers/usr/include/"
			" -isystem /usr/share/clickhouse/headers/usr/include/c++/4.8/"
			" -isystem /usr/share/clickhouse/headers/usr/include/x86_64-linux-gnu/c++/4.8/"
			" -isystem /usr/share/clickhouse/headers/usr/lib/gcc/x86_64-linux-gnu/4.8/include/"
			" -I /usr/share/clickhouse/headers/dbms/include/"
			" -I /usr/share/clickhouse/headers/libs/libcityhash/"
			" -I /usr/share/clickhouse/headers/libs/libcommon/include/"
			" -I /usr/share/clickhouse/headers/libs/libdouble-conversion/src/"
			" -I /usr/share/clickhouse/headers/libs/libmysqlxx/include/"
			" -I /usr/share/clickhouse/headers/libs/libstatdaemons/include/"
			" -I /usr/share/clickhouse/headers/libs/libstats/include/"
			" -o " << so_file_path << " " << cpp_file_path
			<< " 2>&1 || echo Exit code: $?";

		std::string compile_result;

		{
			Pipe pipe(command.str());

			int pipe_fd = fileno(pipe.f);
			if (-1 == pipe_fd)
				throwFromErrno("Cannot fileno");

			{
				ReadBufferFromFileDescriptor command_output(pipe_fd);
				WriteBufferFromString res(compile_result);

				copyData(command_output, res);
			}
		}

		if (!compile_result.empty())
			throw Exception("Cannot compile code:\n\n" + command.str() + "\n\n" + compile_result);

		/// Если до этого была ошибка, то файл с кодом остаётся для возможности просмотра.
		Poco::File(cpp_file_path).remove();

		{
			std::lock_guard<std::mutex> lock(mutex);
			libraries[hashed_key].reset(new SharedLibrary(so_file_path));
		}

		LOG_INFO(log, "Compiled code " << file_name);
	}
};

}
