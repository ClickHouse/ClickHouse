#include <gtest/gtest.h>

#include <Common/DynamicLoader/DynamicLoader.h>
#include <Common/Exception.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include <base/scope_guard.h>

/** These tests exercise the userspace dynamic loader by compiling tiny freestanding shared objects
  * (with -nostdlib, so they depend on nothing) at run time and loading them. If no C compiler is
  * available, or the platform is not supported, the tests skip rather than fail.
  */

namespace
{

namespace fs = std::filesystem;
using DB::DynamicLinker::DynamicLoader;
using DB::DynamicLinker::LoadedLibrary;

std::string findCompiler()
{
    for (const char * compiler : {"cc", "gcc", "clang"})
        if (std::system((std::string("command -v ") + compiler + " > /dev/null 2>&1").c_str()) == 0)
            return compiler;
    return {};
}

/// Compile `source` into a freestanding shared object at `output`, returning whether it succeeded.
bool compileSharedObject(
    const std::string & compiler, const fs::path & directory, const std::string & name,
    const std::string & source, const std::string & extra_flags = {})
{
    fs::path source_path = directory / (name + ".c");
    fs::path output_path = directory / (name + ".so");
    {
        std::ofstream stream(source_path);
        stream << source;
    }
    std::string command = compiler + " -O1 -nostdlib -shared -fPIC " + extra_flags + " "
        + source_path.string() + " -o " + output_path.string() + " > /dev/null 2>&1";
    return std::system(command.c_str()) == 0;
}

/// The TLS dialect flag differs by architecture; the loader implements the traditional general-dynamic model.
std::string traditionalTLSFlag()
{
#if defined(__aarch64__)
    return "-ftls-model=global-dynamic -mtls-dialect=trad";
#else
    return "-ftls-model=global-dynamic";
#endif
}

}


TEST(DynamicLoader, SelfContainedLibrary)
{
    std::string compiler = findCompiler();
    if (compiler.empty())
        GTEST_SKIP() << "no C compiler available";

    fs::path directory = fs::temp_directory_path() / "gtest_dynamic_loader_simple";
    fs::create_directories(directory);
    SCOPE_EXIT({ fs::remove_all(directory); });

    const std::string source = R"(
        static const char * message = "hello";
        const char * get_message(void) { return message; }
        int add(int a, int b) { return a + b; }
    )";
    if (!compileSharedObject(compiler, directory, "self_contained", source))
        GTEST_SKIP() << "cannot compile a freestanding shared object on this platform";

    DynamicLoader loader;
    LoadedLibrary * library = loader.open((directory / "self_contained.so").string());

    auto add = loader.getSymbol<int (*)(int, int)>(library, "add");
    auto get_message = loader.getSymbol<const char * (*)()>(library, "get_message");

    ASSERT_NE(add, nullptr);
    ASSERT_NE(get_message, nullptr);
    EXPECT_EQ(add(2, 3), 5);
    EXPECT_STREQ(get_message(), "hello");
    EXPECT_EQ(loader.getSymbol(library, "does_not_exist"), nullptr);

    loader.close(library);
}


TEST(DynamicLoader, DependencyResolution)
{
    std::string compiler = findCompiler();
    if (compiler.empty())
        GTEST_SKIP() << "no C compiler available";

    fs::path directory = fs::temp_directory_path() / "gtest_dynamic_loader_deps";
    fs::create_directories(directory);
    SCOPE_EXIT({ fs::remove_all(directory); });

    /// The provider exports a function (reached via JUMP_SLOT), a variable (GLOB_DAT), and an indirect
    /// function (IRELATIVE), so the consumer exercises all three cross-module relocation kinds.
    const std::string provider_source = R"(
        int provided_value(void) { return 42; }
        int provided_global = 55;
        static int real_choice(void) { return 7; }
        static void * choose(void) { return (void *) real_choice; }
        int chosen(void) __attribute__((ifunc("choose")));
    )";
    if (!compileSharedObject(compiler, directory, "provider", provider_source, "-Wl,-soname,provider.so"))
        GTEST_SKIP() << "cannot compile a freestanding shared object on this platform";

    /// The consumer references those symbols and links against provider.so, recording it in DT_NEEDED.
    std::string consumer_source = R"(
        extern int provided_value(void);
        extern int provided_global;
        extern int chosen(void);
        int consume(void) { return provided_value() + provided_global + chosen(); }
    )";
    fs::path consumer_source_path = directory / "consumer.c";
    {
        std::ofstream stream(consumer_source_path);
        stream << consumer_source;
    }
    std::string command = compiler + " -O1 -nostdlib -shared -fPIC -Wl,-soname,consumer.so "
        + consumer_source_path.string() + " " + (directory / "provider.so").string()
        + " -o " + (directory / "consumer.so").string() + " > /dev/null 2>&1";
    if (std::system(command.c_str()) != 0)
        GTEST_SKIP() << "cannot link a freestanding shared object against a dependency";

    DynamicLoader loader;
    loader.addSearchPath(directory.string());

    LoadedLibrary * library = loader.open((directory / "consumer.so").string());
    auto consume = loader.getSymbol<int (*)()>(library, "consume");
    ASSERT_NE(consume, nullptr);
    EXPECT_EQ(consume(), 42 + 55 + 7);

    loader.close(library);
}


TEST(DynamicLoader, ThreadLocalStorage)
{
    std::string compiler = findCompiler();
    if (compiler.empty())
        GTEST_SKIP() << "no C compiler available";

    fs::path directory = fs::temp_directory_path() / "gtest_dynamic_loader_tls";
    fs::create_directories(directory);
    SCOPE_EXIT({ fs::remove_all(directory); });

    const std::string source = R"(
        __thread int counter = 100;
        int increment(void) { return ++counter; }
        int get(void) { return counter; }
    )";
    if (!compileSharedObject(compiler, directory, "tls", source, traditionalTLSFlag()))
        GTEST_SKIP() << "cannot compile a general-dynamic thread-local shared object on this platform";

    DynamicLoader loader;
    LoadedLibrary * library = loader.open((directory / "tls.so").string());

    auto increment = loader.getSymbol<int (*)()>(library, "increment");
    auto get = loader.getSymbol<int (*)()>(library, "get");
    ASSERT_NE(increment, nullptr);
    ASSERT_NE(get, nullptr);

    EXPECT_EQ(get(), 100);
    EXPECT_EQ(increment(), 101);
    EXPECT_EQ(increment(), 102);

    /// A separate thread must see its own, independent copy starting from the template value.
    int other_thread_value = 0;
    std::thread worker([&] { other_thread_value = increment(); });
    worker.join();
    EXPECT_EQ(other_thread_value, 101);
    EXPECT_EQ(get(), 102);

    loader.close(library);
}
