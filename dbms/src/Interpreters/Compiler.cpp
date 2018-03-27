#include <Poco/DirectoryIterator.h>
#include <Common/ClickHouseRevision.h>
#include <ext/unlock_guard.h>

#include <Common/SipHash.h>
#include <Common/ShellCommand.h>
#include <Common/StringUtils/StringUtils.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>

#include <Interpreters/Compiler.h>
#include <Interpreters/config_compile.h>


namespace ProfileEvents
{
    extern const Event CompileAttempt;
    extern const Event CompileSuccess;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DLOPEN;
}

Compiler::Compiler(const std::string & path_, size_t threads)
    : path(path_), pool(threads)
{
    Poco::File(path).createDirectory();

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(path); dir_end != dir_it; ++dir_it)
    {
        const std::string & name = dir_it.name();
        if (endsWith(name, ".so"))
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
    hash.update(revision);
    hash.update(key.data(), key.size());

    Compiler::HashedKey res;
    hash.get128(res.low, res.high);
    return res;
}


/// Without .so extension.
static std::string hashedKeyToFileName(Compiler::HashedKey hashed_key)
{
    WriteBufferFromOwnString out;
    out << hashed_key.low << '_' << hashed_key.high;
    return out.str();
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

    /// Is there a ready open library? Or, if the library is in the process of compiling, there will be nullptr.
    Libraries::iterator libraries_it = libraries.find(hashed_key);
    if (libraries.end() != libraries_it)
    {
        if (!libraries_it->second)
            LOG_INFO(log, "Library " << hashedKeyToFileName(hashed_key) << " is already compiling or compilation was failed.");

        /// TODO In this case, after the compilation is finished, the callback will not be called.

        return libraries_it->second;
    }

    /// Is there a file with the library left over from the previous launch?
    std::string file_name = hashedKeyToFileName(hashed_key);
    Files::iterator files_it = files.find(file_name);
    if (files.end() != files_it)
    {
        std::string so_file_path = path + '/' + file_name + ".so";
        LOG_INFO(log, "Loading existing library " << so_file_path);

        SharedLibraryPtr lib;

        try
        {
            lib = std::make_shared<SharedLibrary>(so_file_path);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::CANNOT_DLOPEN)
                throw;

            /// Found broken .so file (or file cannot be dlopened by whatever reason).
            /// This could happen when filesystem is corrupted after server restart.
            /// We remove the file - it will be recompiled on next attempt.

            tryLogCurrentException(log);

            files.erase(files_it);
            Poco::File(so_file_path).remove();
            return nullptr;
        }

        libraries[hashed_key] = lib;
        return lib;
    }

    /// Has min_count_to_compile been reached?
    if (count >= min_count_to_compile)
    {
        /// The min_count_to_compile value of zero indicates the need for synchronous compilation.

        /// Are there any free threads?
        if (min_count_to_compile == 0 || pool.active() < pool.size())
        {
            /// Indicates that the library is in the process of compiling.
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


/// This will guarantee that code will compile only when version of headers match version of running server.
static void addCodeToAssertHeadersMatch(WriteBuffer & out)
{
    out <<
        "#define STRING2(x) #x\n"
        "#define STRING(x) STRING2(x)\n"
        "#include <Common/config_version.h>\n"
        "#if VERSION_REVISION != " << ClickHouseRevision::get() << "\n"
        "#pragma message \"ClickHouse headers revision = \" STRING(VERSION_REVISION) \n"
        "#error \"ClickHouse headers revision doesn't match runtime revision of the server (" << ClickHouseRevision::get() << ").\"\n"
        "#endif\n\n";
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
    std::string so_tmp_file_path = prefix + ".so.tmp";

    {
        WriteBufferFromFile out(cpp_file_path);

        addCodeToAssertHeadersMatch(out);
        out << get_code();
    }

    std::stringstream command;

    /// Slightly unconvenient.
    command <<
        "("
            INTERNAL_COMPILER_ENV
            " " INTERNAL_COMPILER_EXECUTABLE
            " " INTERNAL_COMPILER_FLAGS
            /// It is hard to correctly call a ld program manually, because it is easy to skip critical flags, which might lead to
            /// unhandled exceptions. Therefore pass path to llvm's lld directly to clang.
            " -fuse-ld=" INTERNAL_LINKER_EXECUTABLE


    #if INTERNAL_COMPILER_CUSTOM_ROOT
            /// To get correct order merge this results carefully:
            /// echo | clang -x c++ -E -Wp,-v -
            /// echo | g++ -x c++ -E -Wp,-v -

            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/include/c++/*"
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/include/" CMAKE_LIBRARY_ARCHITECTURE "/c++/*"
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/include/c++/*/backward"
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/include/clang/*/include"                  /// if compiler is clang (from package)
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/local/lib/clang/*/include"                /// if clang installed manually
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/lib/gcc/" CMAKE_LIBRARY_ARCHITECTURE "/*/include-fixed"
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/lib/gcc/" CMAKE_LIBRARY_ARCHITECTURE "/*/include"
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/local/include"                            /// if something installed manually
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/include/" CMAKE_LIBRARY_ARCHITECTURE
            " -isystem " INTERNAL_COMPILER_HEADERS_ROOT "/usr/include"
    #endif
            " -I " INTERNAL_COMPILER_HEADERS "/dbms/src/"
            " -I " INTERNAL_COMPILER_HEADERS "/contrib/libcityhash/include/"
            " -I " INTERNAL_COMPILER_HEADERS "/contrib/libpcg-random/include/"
            " -I " INTERNAL_DOUBLE_CONVERSION_INCLUDE_DIR
            " -I " INTERNAL_Poco_Foundation_INCLUDE_DIR
            " -I " INTERNAL_Boost_INCLUDE_DIRS
            " -I " INTERNAL_COMPILER_HEADERS "/libs/libcommon/include/"
            " " << additional_compiler_flags <<
            " -shared -o " << so_tmp_file_path << " " << cpp_file_path
            << " 2>&1"
        ") || echo Return code: $?";

    std::string compile_result;

    {
        auto process = ShellCommand::execute(command.str());
        readStringUntilEOF(compile_result, process->out);
        process->wait();
    }

    if (!compile_result.empty())
        throw Exception("Cannot compile code:\n\n" + command.str() + "\n\n" + compile_result);

    /// If there was an error before, the file with the code remains for viewing.
    Poco::File(cpp_file_path).remove();

    Poco::File(so_tmp_file_path).renameTo(so_file_path);
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
