#include <gtest/gtest.h>

#include <Common/StackTrace.h>

namespace
{

/// A frame inside libc++'s `std::function` plumbing. Its name spells out the whole captured type and
/// tells nothing that the neighbouring frames do not, so only "?" is displayed for it.
const String std_function_trampoline
    = "std::__1::__function::__func<DB::AsyncLoader::AsyncLoader(std::__1::vector<DB::AsyncLoader::PoolInitializer, "
      "std::__1::allocator<DB::AsyncLoader::PoolInitializer>>, bool)::$_0, "
      "std::__1::allocator<DB::AsyncLoader::AsyncLoader(std::__1::vector<DB::AsyncLoader::PoolInitializer, "
      "std::__1::allocator<DB::AsyncLoader::PoolInitializer>>, bool)::$_0>, void ()>::operator()()";

const String execute_query = "DB::executeQuery(std::__1::basic_string_view<char, std::__1::char_traits<char>>, "
                             "std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum)";

constexpr std::string_view function_h = "./contrib/llvm-project/libcxx/include/__functional/function.h";
constexpr std::string_view invoke_h = "./contrib/llvm-project/libcxx/include/__functional/invoke.h";
constexpr std::string_view hash_h = "./contrib/llvm-project/libcxx/include/__functional/hash.h";
constexpr std::string_view execute_query_cpp = "./src/Interpreters/executeQuery.cpp";

}

TEST(StackTraceCollapseNames, HidesStdFunctionPlumbing)
{
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, std_function_trampoline), "?");
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::__function::__value_func<void ()>::operator()() const"), "?");
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void ()>::operator()() const"), "?");
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void (std::__1::vector<int>)>::function(std::__1::function<void (std::__1::vector<int>)> const&)"), "?");
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void ()>::~function()"), "?");
    /// The constructor that takes a callable is a function template, so its own template arguments follow
    /// the member name - this is the "construct from a lambda" frame, the most common one of them all.
    EXPECT_EQ(
        StackTrace::collapseDemangledNames(
            function_h, "std::__1::function<void ()>::function<DB::AsyncLoader::AsyncLoader()::$_0, void>(DB::AsyncLoader::AsyncLoader()::$_0&&)"),
        "?");
    /// The assignment operators do the same type-erasing work: the copy and move assignment, and the
    /// callable-taking overload, which is a function template just like the corresponding constructor -
    /// this is the "assign a lambda" frame of `f = [capture] { ... };`.
    EXPECT_EQ(
        StackTrace::collapseDemangledNames(
            function_h, "std::__1::function<void ()>::operator=(std::__1::function<void ()>&&)"),
        "?");
    EXPECT_EQ(
        StackTrace::collapseDemangledNames(
            function_h, "std::__1::function<void ()>::operator=<DB::AsyncLoader::AsyncLoader()::$_0, void>(DB::AsyncLoader::AsyncLoader()::$_0&&)"),
        "?");
}

/// Only the type erasure of `std::function` is noise. Its other members do work of their own, so a frame
/// of one of them names the code that is actually running and must keep its name.
TEST(StackTraceCollapseNames, KeepsOrdinaryStdFunctionMembers)
{
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void ()>::swap(std::__1::function<void ()>&)"),
              "std::function<void ()>::swap(std::function<void ()>&)");
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void ()>::target_type() const"),
              "std::function<void ()>::target_type() const");
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void ()>::operator bool() const"),
              "std::function<void ()>::operator bool() const");
    /// Unlike the copy and move assignment, assigning `nullptr` does not copy a captured callable around:
    /// it just resets the object, and its name is short and says exactly that, so it stays visible.
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void ()>::operator=(std::nullptr_t)"),
              "std::function<void ()>::operator=(std::nullptr_t)");
    /// The same goes for construction of an empty `std::function`: the default constructor and the
    /// `nullptr` one do not type-erase a callable, and their names are just as short and informative.
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void ()>::function()"),
              "std::function<void ()>::function()");
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, "std::__1::function<void ()>::function(std::nullptr_t)"),
              "std::function<void ()>::function(std::nullptr_t)");
}

/// The file of a frame is the source line the faulting instruction maps to, and an ordinary function can
/// have individual instructions attributed to a libc++ `__functional` header - an inlined `std::function`
/// operation, or compiler-generated code reported with line 0. Such a frame must keep its own name: it is
/// the only useful part of the frame. Getting this wrong left the frame that actually crashed displayed as
/// a bare `?` in `system.crash_log` and in the fatal log, in builds with ThinLTO enabled.
TEST(StackTraceCollapseNames, KeepsOrdinaryFunctionAttributedToFunctionalHeader)
{
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, execute_query),
              "DB::executeQuery(std::basic_string_view<char, std::char_traits<char>>, std::shared_ptr<DB::Context>, "
              "DB::QueryFlags, DB::QueryProcessingStage::Enum)");
}

/// Not every symbol that lives in a `__functional` header is `std::function` plumbing: libc++ puts
/// `std::hash`, `std::less`, `std::identity` and friends there too, as well as the generic invocation
/// helpers `std::invoke` / `std::__invoke` / `std::mem_fn`, which are used far beyond `std::function`.
/// A frame of one of those is where the code really is, and its name can name the callable it
/// dispatches to, so it must keep its name - only the type-erasing wrappers of `std::function` are noise.
TEST(StackTraceCollapseNames, KeepsMeaningfulStdSymbolFromFunctionalHeader)
{
    EXPECT_EQ(
        StackTrace::collapseDemangledNames(
            hash_h, "std::__1::hash<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>>::operator()"),
        "std::hash<String>::operator()");
    EXPECT_EQ(StackTrace::collapseDemangledNames(hash_h, "std::__1::__murmur2_or_cityhash<unsigned long, 64ul>::operator()"),
              "std::__murmur2_or_cityhash<unsigned long, 64ul>::operator()");
    EXPECT_EQ(StackTrace::collapseDemangledNames(invoke_h, "std::__1::__invoke<DB::AsyncLoader::Pool&>(DB::AsyncLoader::Pool&)"),
              "std::__invoke<DB::AsyncLoader::Pool&>(DB::AsyncLoader::Pool&)");
    EXPECT_EQ(StackTrace::collapseDemangledNames(invoke_h, "std::__1::invoke<void (&)(int), int>(void (&)(int), int&&)"),
              "std::invoke<void (&)(int), int>(void (&)(int), int&&)");
}

TEST(StackTraceCollapseNames, ShortensKnownSpellings)
{
    /// `::__1` is dropped, which also turns the libc++ spelling of `std::string` into the one that is
    /// then collapsed to `String`.
    EXPECT_EQ(StackTrace::collapseDemangledNames(execute_query_cpp, "DB::f(std::__1::vector<int>)"), "DB::f(std::vector<int>)");
    EXPECT_EQ(
        StackTrace::collapseDemangledNames(
            execute_query_cpp,
            "DB::f(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&)"),
        "DB::f(String const&)");
}

TEST(StackTraceCollapseNames, EmptyAndMissingFile)
{
    EXPECT_EQ(StackTrace::collapseDemangledNames(function_h, ""), "?");
    EXPECT_EQ(StackTrace::collapseDemangledNames(std::nullopt, ""), "?");
    EXPECT_EQ(StackTrace::collapseDemangledNames(std::nullopt, "DB::f()"), "DB::f()");
    /// A frame with no file at all is not suppressed - there is nothing to recognise it by.
    EXPECT_NE(StackTrace::collapseDemangledNames(std::nullopt, std_function_trampoline), "?");
}
