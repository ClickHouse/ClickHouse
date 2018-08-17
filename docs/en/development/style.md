# How to Write C++ Code

## General Recommendations

**1.** The following are recommendations, not requirements.

**2.** If you are editing code, it makes sense to follow the formatting of the existing code.

**3.** Code style is needed for consistency. Consistency makes it easier to read the code, and it also makes it easier to search the code.

**4.** Many of the rules do not have logical reasons; they are dictated by established practices.

## Formatting

**1.** Most of the formatting will be done automatically by `clang-format`.

**2.** Indents are 4 spaces. Configure your development environment so that a tab adds four spaces.

**3.** A left curly bracket must be separated on a new line. (And the right one, as well.)

```cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.**
But if the entire function body is quite short (a single statement), you can place it entirely on one line if you wish. Place spaces around curly braces (besides the space at the end of the line).

```cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** For functions, don't put spaces around brackets.

```cpp
void reinsert(const Value & x)
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** When using statements such as `if`, `for`, and `while` (unlike function calls), put a space before the opening bracket.

 ```cpp
 for (size_t i = 0; i < rows; i += storage.index_granularity)
 ```

**7.** Put spaces around binary operators (`+`, `-`, `*`, `/`, `%`, ...), as well as the ternary operator `?:`.

```cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** If a line feed is entered, put the operator on a new line and increase the indent before it.

```cpp
if (elapsed_ns)
    message << " ("
         << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** You can use spaces for alignment within a line, if desired.

```cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** Don't use spaces around the operators `.`, `->` .

If necessary, the operator can be wrapped to the next line. In this case, the offset in front of it is increased.

**11.** Do not use a space to separate unary operators (`-`, `+`, `*`, `&`, ...) from the argument.

**12.** Put a space after a comma, but not before it. The same rule goes for a semicolon inside a for expression.

**13.** Do not use spaces to separate the `[]` operator.

**14.** In a `template <...>` expression, use a space between `template` and `<`. No spaces after `<` or before `>`.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** In classes and structures, public, private, and protected are written on the same level as the `class/struct`, but all other internal elements should be deeper.

```cpp
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;
    ...
}
```

**16.** If the same namespace is used for the entire file, and there isn't anything else significant, an offset is not necessary  inside namespace.

**17.** If the block for `if`, `for`, `while`... expressions consists of a single statement, you don't need to use curly brackets. Place the statement on a separate line, instead. The same is true for a nested if, for, while... statement. But if the inner statement contains curly brackets or else, the external block should be written in curly brackets.

```cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** There should be any spaces at the ends of lines.

**19.** Sources are UTF-8 encoded.

**20.** Non-ASCII characters can be used in string literals.

```cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** Do not write multiple expressions in a single line.

**22.** Group sections of code inside functions and separate them with no more than one empty line.

**23.** Separate functions, classes, and so on with one or two empty lines.

**24.** A `const` (related to a value) must be written before the type name.

```cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** When declaring a pointer or reference, the `*` and `&` symbols should be separated by spaces on both sides.

```cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** When using template types, alias them with the `using` keyword (except in the simplest cases).

In other words, the template parameters are specified only in `using` and aren't repeated in the code.

`using` can be declared locally, such as inside a function.

```cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** Do not declare several variables of different types in one statement.

```cpp
//incorrect
int x, *y;
```

**28.** Do not use C-style casts.

```cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** In classes and structs, group members and functions separately inside each visibility scope.

**30.** For small classes and structs, it is not necessary to separate the method declaration from the implementation.

The same is true for small methods in any classes or structs.

For templated classes and structs, don't separate the method declarations from the implementation (because otherwise they must be defined in the same translation unit).

**31.** You can wrap lines at 140 characters, instead of 80.

**32.** Always use the prefix increment/decrement operators if postfix is not required.

```cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

## Comments

**1.** Be sure to add comments for all non-trivial parts of code.

This is very important. Writing the comment might help you realize that the code isn't necessary, or that it is designed wrong.

```cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to the position right after those 10 bytes available for read).
*/
```

**2.** Comments can be as detailed as necessary.

**3.** Place comments before the code they describe. In rare cases, comments can come after the code, on the same line.

```cpp
/** Parses and executes the query.
*/
void executeQuery(
    ReadBuffer & istr, /// Where to read the query from (and data for INSERT, if applicable)
    WriteBuffer & ostr, /// Where to write the result
    Context & context, /// DB, tables, data types, engines, functions, aggregate functions...
    BlockInputStreamPtr & query_plan, /// A description of query processing can be included here
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete /// The last stage to process the SELECT query to
    )
```

**4.** Comments should be written in English only.

**5.** If you are writing a library, include detailed comments explaining it in the main header file.

**6.** Do not add comments that do not provide additional information. In particular, do not leave empty comments like this:

```cpp
/*
* Procedure Name:
* Original procedure name:
* Author:
* Date of creation:
* Dates of modification:
* Modification authors:
* Original file name:
* Purpose:
* Intent:
* Designation:
* Classes used:
* Constants:
* Local variables:
* Parameters:
* Date of creation:
* Purpose:
*/
```

The example is borrowed from  [http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/](http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/).

**7.** Do not write garbage comments (author, creation date ..) at the beginning of each file.

**8.** Single-line comments begin with three slashes: `///`  and multi-line comments begin with `/**`. These comments are considered "documentation".

Note: You can use Doxygen to generate documentation from these comments. But Doxygen is not generally used because it is more convenient to navigate the code in the IDE.

**9.** Multi-line comments must not have empty lines at the beginning and end (except the line that closes a multi-line comment).

**10.** For commenting out code, use basic comments, not "documenting" comments.

**11.** Delete the commented out parts of the code before commiting.

**12.** Do not use profanity in comments or code.

**13.** Do not use uppercase letters. Do not use excessive punctuation.

```cpp
/// WHAT THE FAIL???
```

**14.** Do not make delimeters from comments.

```
///******************************************************
```

**15.** Do not start discussions in comments.

```
/// Why did you do this stuff?
```

**16.** There's no need to write a comment at the end of a block describing what it was about.

```
/// for
```

## Names

**1.** The names of variables and class members use lowercase letters with underscores.

```cpp
size_t max_block_size;
```

**2.** The names of functions (methods) use camelCase beginning with a lowercase letter.

```cpp
std::string getName() const override { return "Memory"; }
```

**3.** The names of classes (structures) use CamelCase beginning with an uppercase letter. Prefixes other than I are not used for interfaces.

```cpp
class StorageMemory : public IStorage
```

**4.** The names of usings follow the same rules as classes, or you can add _t at the end.

**5.** Names of template type arguments for simple cases: T; T, U; T1, T2.

For more complex cases, either follow the rules for class names, or add the prefix T.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Names of template constant arguments: either follow the rules for variable names, or use N in simple cases.

```cpp
template <bool without_www>
struct ExtractDomain
```

**7.** For abstract classes (interfaces) you can add the I prefix.

```cpp
class IBlockInputStream
```

**8.** If you use a variable locally, you can use the short name.

In other cases, use a descriptive name that conveys the meaning.

```cpp
bool info_successfully_loaded = false;
```

**9.** `define`‘s should be in ALL_CAPS with underscores. The same is true for global constants.

```cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** File names should use the same style as their contents.

If a file contains a single class, name the file the same way as the class, in CamelCase.

If the file contains a single function, name the file the same way as the function, in camelCase.

**11.** If the name contains an abbreviation, then:

- For variable names, the abbreviation should use lowercase letters `mysql_connection` (not `mySQL_connection`).
- For names of classes and functions, keep the uppercase letters in the abbreviation `MySQLConnection` (not `MySqlConnection`).

**12.** Constructor arguments that are used just to initialize the class members should be named the same way as the class members, but with an underscore at the end.

```cpp
FileQueueProcessor(
    const std::string & path_,
    const std::string & prefix_,
    std::shared_ptr<FileHandler> handler_)
    : path(path_),
    prefix(prefix_),
    handler(handler_),
    log(&Logger::get("FileQueueProcessor"))
{
}
```

The underscore suffix can be omitted if the argument is not used in the constructor body.

**13.** There is no difference in the names of local variables and class members (no prefixes required).

```cpp
timer (not m_timer)
```

**14.** Constants in enums use CamelCase beginning with an uppercase letter. ALL_CAPS is also allowed. If the enum is not local, use enum class.

```cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** All names must be in English. Transliteration of Russian words is not allowed.

```cpp
not Stroka
```

**16.** Abbreviations are acceptable if they are well known (when you can easily find the meaning of the abbreviation in Wikipedia or in a search engine).

```
`AST`, `SQL`.

Not `NVDH` (some random letters)
```

Incomplete words are acceptable if the shortened version is common use.

You can also use an abbreviation if the full name is included next to it in the comments.

**17.** File names with C++ source code must have the `.cpp` extension. Header files must have the `.h` extension.

## How to Write Code

**1.** Memory management.

Manual memory deallocation (delete) can only be used in library code.

In library code, the delete operator can only be used in destructors.

In application code, memory must be freed by the object that owns it.

Examples:

- The easiest way is to place an object on the stack, or make it a member of another class.
- For a large number of small objects, use containers.
- For automatic deallocation of a small number of objects that reside in the heap, use shared_ptr/unique_ptr.

**2.** Resource management.

Use RAII and see the previous point.

**3.** Error handling.

Use exceptions. In most cases, you only need to throw an exception, and don't need to catch it (because of RAII).

In offline data processing applications, it's often acceptable to not catch exceptions.

In servers that handle user requests, it's usually enough to catch exceptions at the top level of the connection handler.

```cpp
/// If there were no other calculations yet, do it synchronously
if (!started)
{
    calculate();
    started = true;
}
else    /// If the calculations are already in progress, wait for results
    pool.wait();

if (exception)
    exception->rethrow();
```
Never hide exceptions without handling. Never just blindly put all exceptions to log.

Not `catch (...) {}`.

If you need to ignore some exceptions, do so only for specific ones and rethrow the rest.

```cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

When using functions with response codes or errno, always check the result and throw an exception in case of error.

```cpp
if (0 != close(fd))
    throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);
```

Asserts are not used.

**4.** Exception types.

There is no need to use complex exception hierarchy in application code. The exception text should be understandable to a system administrator.

**5.** Throwing exceptions from destructors.

This is not recommended, but it is allowed.

Use the following options:

- Create a (done() or finalize()) function that will do all the work in advance that might lead to an exception. If that function was called, there should be no exceptions in the destructor later.
- Tasks that are too complex (such as sending messages over the network) can be put in separate method that the class user will have to call before destruction.
- If there is an exception in the destructor, it’s better to log it than to hide it (if the logger is available).
- In simple applications, it is acceptable to rely on std::terminate (for cases of noexcept by default in C++11) to handle exceptions.

**6.** Anonymous code blocks.

You can create a separate code block inside a single function in order to make certain variables local, so that the destructors are called when exiting the block.

```cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** Multithreading.

For offline data processing applications:

- Try to get the best possible performance on a single CPU core. You can then parallelize your code if necessary.

In server applications:

- Use the thread pool to process requests. At this point, we haven't had any tasks that required userspace context switching.

Fork is not used for parallelization.

**8.** Synchronizing threads.

 Often it is possible to make different threads use different memory cells (even better: different cache lines,) and to not use any thread synchronization (except joinAll).

 If synchronization is required, in most cases, it is sufficient to use mutex under lock_guard.

 In other cases use system synchronization primitives. Do not use busy wait.

 Atomic operations should be used only in the simplest cases.

 Do not try to implement lock-free data structures unless it is your primary area of expertise.

**9.** Pointers vs references.

In most cases, prefer references.

**10.** const.

 Use constant references, pointers to constants, `const_iterator`, `const` methods.

 Consider `const` to be default and use non-const only when necessary.

 When passing variable by value, using `const` usually does not make sense.

**11.** unsigned.

Use `unsigned`, if needed.

**12.** Numeric types

Use `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`, and `size_t`, `ssize_t`, `ptrdiff_t`.

Don't use `signed/unsigned long`, `long long`, `short`, `signed char`, `unsigned char`, or `char` types for numbers.

**13.** Passing arguments.

Pass complex values by reference (including `std::string`).

If a function captures ownership of an objected created in the heap, make the argument type `shared_ptr` or `unique_ptr`.

**14.** Returning values.

In most cases, just use return. Do not write `[return std::move(res)]{.strike}`.

If the function allocates an object on heap and returns it, use `shared_ptr` or `unique_ptr`.

In rare cases you might need to return the value via an argument. In this case, the argument should be a reference.

```
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Creates an aggregate function by name.
 */
class AggregateFunctionFactory
{
public:
   AggregateFunctionFactory();
   AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** namespace.

There is no need to use a separate namespace for application code or small libraries.

or small libraries.

For medium to large libraries, put everything in the namespace.

You can use the additional detail namespace in a library's `.h` file to hide implementation details.

In a `.cpp` file, you can use the static or anonymous namespace to hide symbols.

You can also use namespace for enums to prevent its names from polluting the outer namespace, but it’s better to use the enum class.

**16.** Delayed initialization.

If arguments are required for initialization then do not write a default constructor.

If later you’ll need to delay initialization, you can add a default constructor that will create an invalid object. Or, for a small number of objects, you can use `shared_ptr/unique_ptr`.

```cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For delayed initialization
Loader() {}
```

**17.** Virtual functions.

If the class is not intended for polymorphic use, you do not need to make functions virtual. This also applies to the destructor.

**18.** Encodings.

Use UTF-8 everywhere. Use `std::string`and`char *`. Do not use `std::wstring`and`wchar_t`.

**19.** Logging.

See the examples everywhere in the code.

Before committing, delete all meaningless and debug logging, and any other types of debug output.

Logging in cycles should be avoided, even on the Trace level.

Logs must be readable at any logging level.

Logging should only be used in application code, for the most part.

Log messages must be written in English.

The log should preferably be understandable for the system administrator.

Do not use profanity in the log.

Use UTF-8 encoding in the log. In rare cases you can use non-ASCII characters in the log.

**20.** I/O.

Don't use iostreams in internal cycles that are critical for application performance (and never use stringstream).

Use the DB/IO library instead.

**21.** Date and time.

See the `DateLUT` library.

**22.** include.

Always use `#pragma once` instead of include guards.

**23.** using.

The `using namespace` is not used.

It's fine if you are 'using' something specific, but make it local inside a class or function.

**24.** Do not use trailing return type for functions unless necessary.

```
[auto f() -&gt; void;]{.strike}
```

**25.** Do not declare and init variables like this:

```cpp
auto s = std::string{"Hello"};
```

Do it like this:

```cpp
std::string s = "Hello";
std::string s{"Hello"};
```

**26.** For virtual functions, write `virtual` in the base class, but write `override` in descendent classes.

## Unused Features of C++

**1.** Virtual inheritance is not used.

**2.** Exception specifiers from C++03 are not used.

## Platform

**1.** We write code for a specific platform.

But other things being equal, cross-platform or portable code is preferred.

**2.** The language is C++17.

**3.** The compiler is `gcc`. At this time (December 2017), the code is compiled using version 7.2. (It can also be compiled using clang 5.)

The standard library is used (implementation of `libstdc++` or `libc++`).

**4.** OS: Linux Ubuntu, not older than Precise.

**5.** Code is written for x86_64 CPU architecture.

The CPU instruction set is the minimum supported set among our servers. Currently, it is SSE 4.2.

**6.** Use `-Wall -Wextra -Werror` compilation flags.

**7.** Use static linking with all libraries except those that are difficult to connect to statically (see the output of the `ldd` command).

**8.** Code is developed and debugged with release settings.

## Tools

**1.** `KDevelop` is a good IDE.

**2.** For debugging, use `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=`, ..., `tcmalloc_minimal_debug`.

**3.** For profiling, use Linux Perf `valgrind` (`callgrind`), `strace-cf`.

**4.** Sources are in Git.

**5.** Compilation is managed by `CMake`.

**6.** Releases are in `deb` packages.

**7.** Commits to master must not break the build.

Though only selected revisions are considered workable.

**8.** Make commits as often as possible, even if the code is only partially ready.

Use branches for this purpose.

If your code is not buildable yet, exclude it from the build before pushing to master. You'll need to finish it or remove it from master within a few days.

**9.** For non-trivial changes, used branches and publish them on the server.

**10.** Unused code is removed from the repository.

## Libraries

**1.** The C++14 standard library is used (experimental extensions are fine), as well as boost and Poco frameworks.

**2.** If necessary, you can use any well-known libraries available in the OS package.

If there is a good solution already available, then use it, even if it means you have to install another library.

(But be prepared to remove bad libraries from code.)

**3.** You can install a library that isn't in the packages, if the packages don't have what you need or have an outdated version or the wrong type of compilation.

**4.** If the library is small and doesn't have its own complex build system, put the source files in the contrib folder.

**5.** Preference is always given to libraries that are  already used.

## General Recommendations

**1.** Write as little code as possible.

**2.** Try the simplest solution.

**3.** Don't write code until you know how it's going to work and how the inner loop will function.

**4.** In the simplest cases, use 'using' instead of classes or structs.

**5.** If possible, do not write copy constructors, assignment operators, destructors (other than a virtual one, if the class contains at least one virtual function), mpve-constructors and move assignment operators. In other words, the compiler-generated functions must work correctly. You can use 'default'.

**6.** Code simplification is encouraged. Reduce the size of your code where possible.

## Additional Recommendations

**1.** Explicit `std::` for types from `stddef.h` is not recommended.

We recommend writing `size_t` instead `std::size_t` because it's shorter.

But if you prefer, `std::` is acceptable.

**2.** Explicit `std::` for functions from the standard C library is not recommended.

Write `memcpy` instead of `std::memcpy`.

The reason is that there are similar non-standard functions, such as `memmem`. We do use these functions on occasion. These functions do not exist in namespace `std`.

If you write `std::memcpy` instead of `memcpy` everywhere, then `memmem` without `std::` will look awkward.

Nevertheless, `std::` is allowed if you prefer it.

**3.** Using functions from C when the ones are available in the standard C++ library.

 This is acceptable if it is more efficient.

 For example, use `memcpy` instead of `std::copy` for copying large chunks of memory.

**4.** Multiline function arguments.

Any of the following wrapping styles are allowed:

```cpp
function(
    T1 x1,
    T2 x2)
```

```cpp
function(
    size_t left, size_t right,
    const & RangesInDataParts ranges,
    size_t limit)
```

```cpp
function(size_t left, size_t right,
    const & RangesInDataParts ranges,
    size_t limit)
```

```cpp
function(size_t left, size_t right,
        const & RangesInDataParts ranges,
        size_t limit)
```

```cpp
function(
        size_t left,
        size_t right,
        const & RangesInDataParts ranges,
        size_t limit)
```
