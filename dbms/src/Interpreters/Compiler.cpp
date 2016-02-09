#include <Poco/DirectoryIterator.h>
#include <common/ClickHouseRevision.h>
#include <ext/unlock_guard.hpp>

#include <DB/Common/SipHash.h>
#include <DB/Common/ShellCommand.h>

#include <DB/IO/Operators.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/copyData.h>
#include <DB/IO/WriteBufferFromFile.h>

#include <DB/Interpreters/Compiler.h>


namespace DB
{


Compiler::Compiler(const std::string & path_, size_t threads)
	: path(path_), pool(threads)
{
	Poco::File(path).createDirectory();

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

Compiler::~Compiler()
{
	LOG_DEBUG(log, "Waiting for threads to finish.");
	pool.wait();
}


static Compiler::HashedKey getHash(const std::string & key)
{
	SipHash hash;

	auto revision = ClickHouseRevision::get();
	hash.update(reinterpret_cast<const char *>(&revision), sizeof(revision));
	hash.update(key.data(), key.size());

	Compiler::HashedKey res;
	hash.get128(res.first, res.second);
	return res;
}


/// Без расширения .so.
static std::string hashedKeyToFileName(Compiler::HashedKey hashed_key)
{
	std::string file_name;

	{
		WriteBufferFromString out(file_name);
		out << hashed_key.first << '_' << hashed_key.second;
	}

	return file_name;
}


SharedLibraryPtr Compiler::getOrCount(
	const std::string & key,
	UInt32 min_count_to_compile,
	const std::string & additional_compiler_flags,
	CodeGenerator get_code,
	ReadyCallback on_ready)
{
	HashedKey hashed_key = getHash(key);

	std::lock_guard<std::mutex> lock(mutex);

	UInt32 count = ++counts[hashed_key];

	/// Есть готовая открытая библиотека? Или, если библиотека в процессе компиляции, там будет nullptr.
	Libraries::iterator it = libraries.find(hashed_key);
	if (libraries.end() != it)
	{
		if (!it->second)
			LOG_INFO(log, "Library " << hashedKeyToFileName(hashed_key) << " is already compiling or compilation was failed.");

		/// TODO В этом случае, после окончания компиляции, не будет дёрнут колбэк.

		return it->second;
	}

	/// Есть файл с библиотекой, оставшийся от предыдущего запуска?
	std::string file_name = hashedKeyToFileName(hashed_key);
	if (files.count(file_name))
	{
		std::string so_file_path = path + '/' + file_name + ".so";
		LOG_INFO(log, "Loading existing library " << so_file_path);

		SharedLibraryPtr lib(new SharedLibrary(so_file_path));
		libraries[hashed_key] = lib;
		return lib;
	}

	/// Достигнуто ли min_count_to_compile?
	if (count >= min_count_to_compile)
	{
		/// Значение min_count_to_compile, равное нулю, обозначает необходимость синхронной компиляции.

		/// Есть ли свободные потоки.
		if (min_count_to_compile == 0 || pool.active() < pool.size())
		{
			/// Обозначает, что библиотека в процессе компиляции.
			libraries[hashed_key] = nullptr;

			LOG_INFO(log, "Compiling code " << file_name << ", key: " << key);

			if (min_count_to_compile == 0)
			{
				{
					ext::unlock_guard<std::mutex> unlock(mutex);
					compile(hashed_key, file_name, additional_compiler_flags, get_code, on_ready);
				}

				return libraries[hashed_key];
			}
			else
			{
				pool.schedule([=]
				{
					try
					{
						compile(hashed_key, file_name, additional_compiler_flags, get_code, on_ready);
					}
					catch (...)
					{
						tryLogCurrentException("Compiler");
					}
				});
			}
		}
		else
			LOG_INFO(log, "All threads are busy.");
	}

	return nullptr;
}


void Compiler::compile(
	HashedKey hashed_key,
	std::string file_name,
	const std::string & additional_compiler_flags,
	CodeGenerator get_code,
	ReadyCallback on_ready)
{
	ProfileEvents::increment(ProfileEvents::CompileAttempt);

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
		"LD_LIBRARY_PATH=/usr/share/clickhouse/bin/"
		" /usr/share/clickhouse/bin/clang"
		" -x c++ -std=gnu++1y -O3 -g -Wall -Werror -Wnon-virtual-dtor -march=native -D NDEBUG"
		" -shared -fPIC -fvisibility=hidden -fno-implement-inlines"
		" -isystem /usr/share/clickhouse/headers/usr/local/include/"
		" -isystem /usr/share/clickhouse/headers/usr/include/"
		" -isystem /usr/share/clickhouse/headers/usr/include/c++/*/"
		" -isystem /usr/share/clickhouse/headers/usr/include/x86_64-linux-gnu/"
		" -isystem /usr/share/clickhouse/headers/usr/include/x86_64-linux-gnu/c++/*/"
		" -isystem /usr/share/clickhouse/headers/usr/local/lib/clang/*/include/"
		" -I /usr/share/clickhouse/headers/dbms/include/"
		" -I /usr/share/clickhouse/headers/contrib/libcityhash/"
		" -I /usr/share/clickhouse/headers/contrib/libdouble-conversion/"
		" -I /usr/share/clickhouse/headers/contrib/libpoco/Foundation/include/"
		" -I /usr/share/clickhouse/headers/contrib/libpoco/Util/include/"
		" -I /usr/share/clickhouse/headers/contrib/libboost-threadpool/"
		" -I /usr/share/clickhouse/headers/libs/libcommon/include/"
		" -I /usr/share/clickhouse/headers/libs/libmysqlxx/include/"
		" " << additional_compiler_flags <<
		" -o " << so_file_path << " " << cpp_file_path
		<< " 2>&1 || echo Exit code: $?";

	std::string compile_result;

	{
		auto process = ShellCommand::execute(command.str());

		{
			WriteBufferFromString res(compile_result);
			copyData(process->out, res);
		}

		process->wait();
	}

	if (!compile_result.empty())
		throw Exception("Cannot compile code:\n\n" + command.str() + "\n\n" + compile_result);

	/// Если до этого была ошибка, то файл с кодом остаётся для возможности просмотра.
	Poco::File(cpp_file_path).remove();

	SharedLibraryPtr lib(new SharedLibrary(so_file_path));

	{
		std::lock_guard<std::mutex> lock(mutex);
		libraries[hashed_key] = lib;
	}

	LOG_INFO(log, "Compiled code " << file_name);
	ProfileEvents::increment(ProfileEvents::CompileSuccess);

	on_ready(lib);
}


}
