#pragma once

#include <boost/core/noncopyable.hpp>

#include <iostream>
#include <string>
#include <mutex>
#include <functional>
#include <unordered_set>
#include <unordered_map>

#include <common/logger_useful.h>

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/UInt128.h>
#include <Common/SharedLibrary.h>
#include <common/ThreadPool.h>

namespace DB
{

/** Lets you compile a piece of code that uses the server's header files into the dynamic library.
  * Conducts statistic of calls, and initiates compilation only on the N-th call for one key.
  * Compilation is performed asynchronously, in separate threads, if there are free threads.
  * NOTE: There is no cleaning of obsolete and unnecessary results.
  */
class Compiler
{
public:
    /** path - path to the directory with temporary files - the results of the compilation.
      * The compilation results are saved when the server is restarted,
      *  but use the revision number as part of the key. That is, they become obsolete when the server is updated.
      */
    Compiler(const std::string & path_, size_t threads);
    ~Compiler();

    using HashedKey = UInt128;

    using CodeGenerator = std::function<std::string()>;
    using ReadyCallback = std::function<void(SharedLibraryPtr&)>;

    /** Increase the counter for the given key `key` by one.
      * If the compilation result already exists (already open, or there is a file with the library),
      *  then return ready SharedLibrary.
      * Otherwise, if min_count_to_compile == 0, then initiate the compilation in the same thread, wait for it, and return the result.
      * Otherwise, if the counter has reached min_count_to_compile,
      *  initiate compilation in a separate thread, if there are free threads, and return nullptr.
      * Otherwise, return nullptr.
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
    ThreadPool pool;

    /// Number of calls to `getOrCount`.
    Counts counts;

    /// Compiled and open libraries. Or nullptr for libraries in the compilation process.
    Libraries libraries;

    /// Compiled files remaining from previous runs, but not yet open.
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
