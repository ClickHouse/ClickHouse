#include <gtest/gtest.h>

#include <Common/DynamicLoader/DynamicLoader.h>
#include <Common/Exception.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

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

/// Run a shell command, discarding its output, and tell whether it succeeded.
bool runCommand(const std::string & command)
{
    /// The command is composed by the test itself, so going through the command processor is fine here.
    return std::system((command + " > /dev/null 2>&1").c_str()) == 0; // NOLINT(bugprone-command-processor,cert-env33-c,concurrency-mt-unsafe)
}

std::string findCompiler()
{
    for (const char * compiler : {"cc", "gcc", "clang"})
        if (runCommand(std::string("command -v ") + compiler))
            return compiler;
    return {};
}

/// Compile `source` into a freestanding shared object `name`.so in `directory`, returning whether it succeeded.
/// `dependencies` are the shared objects to link against (recorded in DT_NEEDED); they go after the source
/// file, because a linker defaulting to `--as-needed` drops the libraries that precede their users.
bool compileSharedObject(
    const std::string & compiler, const fs::path & directory, const std::string & name,
    const std::string & source, const std::string & extra_flags = {}, const std::string & dependencies = {})
{
    fs::path source_path = directory / (name + ".c");
    fs::path output_path = directory / (name + ".so");
    {
        std::ofstream stream(source_path);
        stream << source;
    }
    return runCommand(compiler + " -O1 -nostdlib -shared -fPIC " + extra_flags + " "
        + source_path.string() + " " + dependencies + " -o " + output_path.string());
}

/// Records what the loaded libraries report through the `host_record` symbol the tests provide to them.
std::vector<int> recorded_events;

void hostRecord(int event)
{
    recorded_events.push_back(event);
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
        + " -o " + (directory / "consumer.so").string();
    if (!runCommand(command))
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


/// An indirect function resolver is code of the library that defines the function. It must see that library
/// relocated, even when the reference that triggers it comes from an object that is earlier in the scope.
TEST(DynamicLoader, IndirectFunctionResolverSeesRelocatedProvider)
{
    std::string compiler = findCompiler();
    if (compiler.empty())
        GTEST_SKIP() << "no C compiler available";

    fs::path directory = fs::temp_directory_path() / "gtest_dynamic_loader_ifunc_order";
    fs::create_directories(directory);
    SCOPE_EXIT({ fs::remove_all(directory); });

    /// The resolver reads a table of function pointers: the table is filled by a relative relocation, and it is
    /// reached through the GOT (it is a preemptible global), which is filled by another relocation.
    const std::string provider_source = R"(
        static int first(void) { return 7; }
        static int second(void) { return 8; }
        int (*selection_table[2])(void) = { first, second };
        int selected_index = 1;
        static void * choose(void) { return (void *) selection_table[selected_index]; }
        int chosen(void) __attribute__((ifunc("choose")));
    )";
    if (!compileSharedObject(compiler, directory, "ifunc_provider", provider_source, "-Wl,-soname,ifunc_provider.so"))
        GTEST_SKIP() << "cannot compile a shared object with an indirect function on this platform";

    const std::string consumer_source = R"(
        extern int chosen(void);
        int (*chosen_pointer)(void) = chosen;
        int consume(void) { return chosen() + chosen_pointer(); }
    )";
    if (!compileSharedObject(compiler, directory, "ifunc_consumer", consumer_source,
            "-Wl,-soname,ifunc_consumer.so", (directory / "ifunc_provider.so").string()))
        GTEST_SKIP() << "cannot link a freestanding shared object against a dependency";

    DynamicLoader loader;
    loader.addSearchPath(directory.string());

    LoadedLibrary * library = loader.open((directory / "ifunc_consumer.so").string());
    auto consume = loader.getSymbol<int (*)()>(library, "consume");
    ASSERT_NE(consume, nullptr);
    EXPECT_EQ(consume(), 8 + 8);

    /// The lookup by name goes through the resolver as well.
    auto chosen = loader.getSymbol<int (*)()>(library, "chosen");
    ASSERT_NE(chosen, nullptr);
    EXPECT_EQ(chosen(), 8);

    loader.close(library);
}


/// A reference from an object to its own protected symbol binds to that symbol, even if an object that is
/// earlier in the scope defines a symbol with the same name.
TEST(DynamicLoader, ProtectedSymbolIsNotPreempted)
{
    std::string compiler = findCompiler();
    if (compiler.empty())
        GTEST_SKIP() << "no C compiler available";

    fs::path directory = fs::temp_directory_path() / "gtest_dynamic_loader_protected";
    fs::create_directories(directory);
    SCOPE_EXIT({ fs::remove_all(directory); });

    /// Depending on the toolchain, the function may reach the protected variable without any symbolic relocation;
    /// the pointer to it in the data always needs one (`GLOB_DAT` or an absolute relocation against the symbol).
    const std::string dependency_source = R"(
        __attribute__((visibility("protected"))) int protected_value = 2;
        int preemptible_value = 20;
        int * protected_value_pointer = &protected_value;
        int read_protected_value(void) { return protected_value; }
        int read_protected_value_through_pointer(void) { return *protected_value_pointer; }
        int read_preemptible_value(void) { return preemptible_value; }
    )";
    if (!compileSharedObject(compiler, directory, "protected_dependency", dependency_source, "-Wl,-soname,protected_dependency.so"))
        GTEST_SKIP() << "cannot compile a freestanding shared object on this platform";

    /// The root object comes first in the scope and defines both names too.
    const std::string root_source = R"(
        extern int read_protected_value(void);
        extern int read_protected_value_through_pointer(void);
        extern int read_preemptible_value(void);
        int protected_value = 1;
        int preemptible_value = 10;
        int read_all(void)
        {
            return read_protected_value() * 10000 + read_protected_value_through_pointer() * 100 + read_preemptible_value();
        }
    )";
    if (!compileSharedObject(compiler, directory, "protected_root", root_source,
            "-Wl,-soname,protected_root.so", (directory / "protected_dependency.so").string()))
        GTEST_SKIP() << "cannot link a freestanding shared object against a dependency";

    DynamicLoader loader;
    loader.addSearchPath(directory.string());

    LoadedLibrary * library = loader.open((directory / "protected_root.so").string());
    auto read_all = loader.getSymbol<int (*)()>(library, "read_all");
    ASSERT_NE(read_all, nullptr);
    /// The protected symbol is the dependency's own; the ordinary one is preempted by the root object.
    EXPECT_EQ(read_all(), 2 * 10000 + 2 * 100 + 10);

    loader.close(library);
}


/// Two plugins, each with its own library of the same name next to it, must each get their own.
TEST(DynamicLoader, SameNameDependenciesInDifferentDirectories)
{
    std::string compiler = findCompiler();
    if (compiler.empty())
        GTEST_SKIP() << "no C compiler available";

    fs::path directory = fs::temp_directory_path() / "gtest_dynamic_loader_same_name";
    SCOPE_EXIT({ fs::remove_all(directory); });

    for (const char * variant : {"1", "2"})
    {
        fs::path subdirectory = directory / variant;
        fs::create_directories(subdirectory);

        const std::string codec_source = std::string("int codec_version(void) { return ") + variant + "; }";
        if (!compileSharedObject(compiler, subdirectory, "libcodec", codec_source, "-Wl,-soname,libcodec.so"))
            GTEST_SKIP() << "cannot compile a freestanding shared object on this platform";

        const std::string plugin_source = R"(
            extern int codec_version(void);
            int plugin_codec_version(void) { return codec_version(); }
        )";
        if (!compileSharedObject(compiler, subdirectory, "plugin", plugin_source,
                "-Wl,-rpath,'$ORIGIN'", (subdirectory / "libcodec.so").string()))
            GTEST_SKIP() << "cannot link a freestanding shared object against a dependency";
    }

    DynamicLoader loader;
    LoadedLibrary * first = loader.open((directory / "1" / "plugin.so").string());
    LoadedLibrary * second = loader.open((directory / "2" / "plugin.so").string());

    auto first_version = loader.getSymbol<int (*)()>(first, "plugin_codec_version");
    auto second_version = loader.getSymbol<int (*)()>(second, "plugin_codec_version");
    ASSERT_NE(first_version, nullptr);
    ASSERT_NE(second_version, nullptr);
    EXPECT_EQ(first_version(), 1);
    EXPECT_EQ(second_version(), 2);

    loader.close(second);
    loader.close(first);
}


/// A failed `open` leaves nothing behind, so it can be retried once the reason of the failure is gone.
TEST(DynamicLoader, FailedOpenIsRolledBack)
{
    std::string compiler = findCompiler();
    if (compiler.empty())
        GTEST_SKIP() << "no C compiler available";

    fs::path directory = fs::temp_directory_path() / "gtest_dynamic_loader_rollback";
    fs::path dependencies_directory = directory / "dependencies";
    fs::create_directories(dependencies_directory);
    SCOPE_EXIT({ fs::remove_all(directory); });

    recorded_events.clear();

    /// The first dependency is found and initialized; the second one is not on the search path at first.
    const std::string present_source = R"(
        extern void host_record(int);
        __attribute__((constructor)) static void initialize(void) { host_record(1); }
        __attribute__((destructor)) static void finalize(void) { host_record(-1); }
        int present_value(void) { return 30; }
    )";
    if (!compileSharedObject(compiler, directory, "present", present_source, "-Wl,-soname,present.so"))
        GTEST_SKIP() << "cannot compile a freestanding shared object on this platform";

    const std::string missing_source = "int missing_value(void) { return 12; }";
    if (!compileSharedObject(compiler, dependencies_directory, "missing", missing_source, "-Wl,-soname,missing.so"))
        GTEST_SKIP() << "cannot compile a freestanding shared object on this platform";

    const std::string plugin_source = R"(
        extern int present_value(void);
        extern int missing_value(void);
        int plugin_value(void) { return present_value() + missing_value(); }
    )";
    if (!compileSharedObject(compiler, directory, "rollback_plugin", plugin_source,
            {}, (directory / "present.so").string() + " " + (dependencies_directory / "missing.so").string()))
        GTEST_SKIP() << "cannot link a freestanding shared object against a dependency";

    DynamicLoader loader;
    loader.provideSymbol("host_record", reinterpret_cast<void *>(&hostRecord));
    loader.addSearchPath(directory.string());

    const std::string plugin_path = (directory / "rollback_plugin.so").string();
    EXPECT_THROW(loader.open(plugin_path), DB::Exception);
    /// Nothing was initialized, because the failure happened before the initializers.
    EXPECT_TRUE(recorded_events.empty());

    /// The retry must notice the dependency that could not be found the first time.
    loader.addSearchPath(dependencies_directory.string());
    LoadedLibrary * library = loader.open(plugin_path);
    auto plugin_value = loader.getSymbol<int (*)()>(library, "plugin_value");
    ASSERT_NE(plugin_value, nullptr);
    EXPECT_EQ(plugin_value(), 30 + 12);
    EXPECT_EQ(recorded_events, (std::vector<int>{1}));

    loader.close(library);
    EXPECT_EQ(recorded_events, (std::vector<int>{1, -1}));
}


/// Finalizers of a dependent run while its dependencies are still mapped and not yet finalized - both on
/// `close` and when the loader is destroyed with libraries still open.
TEST(DynamicLoader, DependentsAreFinalizedBeforeDependencies)
{
    std::string compiler = findCompiler();
    if (compiler.empty())
        GTEST_SKIP() << "no C compiler available";

    fs::path directory = fs::temp_directory_path() / "gtest_dynamic_loader_teardown";
    fs::create_directories(directory);
    SCOPE_EXIT({ fs::remove_all(directory); });

    /// The dependency is named so that it sorts before its dependent by path.
    const std::string dependency_source = R"(
        extern void host_record(int);
        static int state = 0;
        __attribute__((constructor)) static void initialize(void) { state = 5; host_record(1); }
        __attribute__((destructor)) static void finalize(void) { state = 0; host_record(-1); }
        int dependency_state(void) { return state; }
    )";
    if (!compileSharedObject(compiler, directory, "a_dependency", dependency_source, "-Wl,-soname,a_dependency.so"))
        GTEST_SKIP() << "cannot compile a freestanding shared object on this platform";

    /// The dependent calls into the dependency from both its initializer and its finalizer.
    const std::string dependent_source = R"(
        extern void host_record(int);
        extern int dependency_state(void);
        __attribute__((constructor)) static void initialize(void) { host_record(2 + 10 * dependency_state()); }
        __attribute__((destructor)) static void finalize(void) { host_record(-2 - 10 * dependency_state()); }
        int dependent_value(void) { return dependency_state(); }
    )";
    if (!compileSharedObject(compiler, directory, "z_dependent", dependent_source,
            "-Wl,-soname,z_dependent.so", (directory / "a_dependency.so").string()))
        GTEST_SKIP() << "cannot link a freestanding shared object against a dependency";

    const std::vector<int> expected_events{1, 52, -52, -1};

    /// An explicit `close`.
    {
        recorded_events.clear();
        DynamicLoader loader;
        loader.provideSymbol("host_record", reinterpret_cast<void *>(&hostRecord));
        loader.addSearchPath(directory.string());
        LoadedLibrary * library = loader.open((directory / "z_dependent.so").string());
        loader.close(library);
        EXPECT_EQ(recorded_events, expected_events);
    }

    /// The loader is destroyed while the library is still open.
    {
        recorded_events.clear();
        {
            DynamicLoader loader;
            loader.provideSymbol("host_record", reinterpret_cast<void *>(&hostRecord));
            loader.addSearchPath(directory.string());
            loader.open((directory / "z_dependent.so").string());
        }
        EXPECT_EQ(recorded_events, expected_events);
    }
}
