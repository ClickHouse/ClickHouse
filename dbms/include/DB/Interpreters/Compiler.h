#pragma once

#include <dlfcn.h>

#include <string>
#include <mutex>
#include <functional>
#include <unordered_set>
#include <unordered_map>

#include <common/logger_useful.h>
#include <threadpool.hpp>

#include <DB/Core/Types.h>
#include <DB/Common/Exception.h>
#include <DB/Common/UInt128.h>


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

using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;


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
	Compiler(const std::string & path_, size_t threads);
	~Compiler();

	using HashedKey = UInt128;

	using CodeGenerator = std::function<std::string()>;
	using ReadyCallback = std::function<void(SharedLibraryPtr&)>;

	/** Увеличить счётчик для заданного ключа key на единицу.
	  * Если результат компиляции уже есть (уже открыт, или есть файл с библиотекой),
	  *  то вернуть готовую SharedLibrary.
	  * Иначе, если min_count_to_compile == 0, то инициировать компиляцию в том же потоке, дождаться её, и вернуть результат.
	  * Иначе, если счётчик достиг min_count_to_compile,
	  *  инициировать компиляцию в отдельном потоке, если есть свободные потоки, и вернуть nullptr.
	  * Иначе вернуть nullptr.
	  */
	SharedLibraryPtr getOrCount(
		const std::string & key,
		UInt32 min_count_to_compile,
		const std::string & additional_compiler_flags,
		CodeGenerator get_code,
		ReadyCallback on_ready);

private:
	using Counts = std::unordered_map<HashedKey, UInt32, UInt128Hash>;
	using Libraries = std::unordered_map<HashedKey, SharedLibraryPtr, UInt128Hash>;
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


	void compile(
		HashedKey hashed_key,
		std::string file_name,
		const std::string & additional_compiler_flags,
		CodeGenerator get_code,
		ReadyCallback on_ready);
};

}
