..  role:: strike
    :class: strike

How to write C++ code
=====================

General
-------

#. This text should be considered as recommendations.
#. If you edit some code, it makes sense to keep style in changes consistent with the rest of it.
#. Style is needed to keep code consistent. Consistency is required to make it easier (more convenient) to read the code. And also for code navigation.
#. Many rules do not have any logical explanation and just come from existing practice.

Formatting
----------

#. Most of formatting is done automatically by ``clang-format``.
#. Indent is 4 spaces wide. Configure your IDE to insert 4 spaces on pressing Tab.
#. Curly braces on separate lines.

    .. code-block:: cpp

        inline void readBoolText(bool & x, ReadBuffer & buf)
        {
            char tmp = '0';
            readChar(tmp, buf);
            x = tmp != '0';
        }


#. But if function body is short enough (one statement) you can put the whole thing on one line. In this case put spaces near curly braces, except for the last one in the end.

    .. code-block:: cpp

        inline size_t mask() const                { return buf_size() - 1; }
        inline size_t place(HashValue x) const    { return x & mask(); }


#. For functions there are no spaces near brackets.

    .. code-block:: cpp

        void reinsert(const Value & x)

    .. code-block:: cpp

        memcpy(&buf[place_value], &x, sizeof(x));


#. When using expressions if, for, while, ... (in contrast to function calls) there should be space before opening bracket.

    .. code-block:: cpp

        for (size_t i = 0; i < rows; i += storage.index_granularity)

#. There should be spaces around binary operators (+, -, \*, /, %, ...) and ternary operator ?:.

    .. code-block:: cpp

        UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
        UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
        UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');


#. If there's a line break, operator is written on new line and it has additional indent.

    .. code-block:: cpp

        if (elapsed_ns)
            message << " ("
                << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
                << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";

    #. It is ok to insert additional spaces to align the code.

        .. code-block:: cpp

            dst.ClickLogID         = click.LogID;
            dst.ClickEventID       = click.EventID;
            dst.ClickGoodEvent     = click.GoodEvent;


#. No spaces around ``.``, ``->`` operators.
    If necessary these operators can be moved to next line with additional indent.

#. Unary operators (``--, ++, *, &``, ...) are not delimited from argument.

#. Space is put after comma or semicolon, not before.

#. Operator ``[]`` is not delimited with spaces.

#. In ``template <...>``, put space between ``template`` and ``<``; after ``<`` and before ``>`` - do not.

    .. code-block:: cpp

        template <typename TKey, typename TValue>
        struct AggregatedStatElement


#. In classes and structs keywords public, private, protected are written on same indention level as class/struct, while other contents - deeper.

    .. code-block:: cpp

        template <typename T, typename Ptr = std::shared_ptr<T>>
        class MultiVersion
        {
        public:
            /// Конкретная версия объекта для использования. shared_ptr определяет время жизни версии.
            using Version = Ptr;


#. If there's only one namespace in a file and there's nothing else significant - no need to indent the namespace.

#. If ``if, for, while...`` block consists of only one statement, it's not required to wrap it in curly braces. Instead you can put the statement on separate line. This statements can also be a ``if, for, while...`` block. But if internal statement contains curly braces or else, this option should not be used.

    .. code-block:: cpp

        /// Если файлы не открыты, то открываем их.
        if (streams.empty())
            for (const auto & name : column_names)
                streams.emplace(name, std::make_unique<Stream>(
                    storage.files[name].data_file.path(),
                    storage.files[name].marks[mark_number].offset));

#. No spaces before end of line.

#. Source code should be in UTF-8 encoding.

#. It's ok to have non-ASCII characters in string literals.

    .. code-block:: cpp

        << ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";


#. Don't put multiple statements on single line.

#. Inside functions do not delimit logical blocks by more than one empty line.

#. Functions, classes and similar constructs are delimited by one or two empty lines.

#. const (related to value) is written before type name.

    .. code-block:: cpp

        const char * pos

    .. code-block:: cpp

        const std::string & s

    :strike:`char const * pos`

#. When declaring pointer or reference symbols \* and & should be surrounded by spaces.

    .. code-block:: cpp

        const char * pos

    :strike:`const char\* pos`
    :strike:`const char \*pos`

#. Alias template types with ``using`` keyword (except the most simple cases). It can be declared even locally, for example inside functions.

    .. code-block:: cpp

        using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
        FileStreams streams;

    :strike:`std::map<std::string, std::shared_ptr<Stream>> streams;`

#. Do not declare several variables of different types in one statements.

    :strike:`int x, *y;`

#. C-style casts should be avoided.

    :strike:`std::cerr << (int)c << std::endl;`

    .. code-block:: cpp

        std::cerr << static_cast<int>(c) << std::endl;


#. In classes and structs group members and functions separately inside each visibility scope.

#. For small classes and structs, it is not necessary to split method declaration and implementation.
    The same for small methods.
    For templated classes and structs it is better not to split declaration and implementations (because anyway they should be defined in the same translation unit).

#. Lines should be wrapped at 140 symbols, not 80.

#. Always use prefix increment/decrement if postfix is not required.

    .. code-block:: cpp

        for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)


Comments
--------

#. You shoud write comments in all not trivial places.
    It is very important. While writing comment you could even understand that code does the wrong thing or is completely unnecessary.

    .. code-block:: cpp

        /** Part of piece of memory, that can be used.
          * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
          * then working_buffer will have size of only 10 bytes
          * (working_buffer.end() will point to position right after those 10 bytes available for read).
          */


#. Comments can be as detailed as necessary.

#. Comments are written before the relevant code. In rare cases - after on the same line.

    .. code-block:: text

        /** Parses and executes the query.
          */
        void executeQuery(
            ReadBuffer & istr,                                                  /// Where to read the query from (and data for INSERT, if applicable)
            WriteBuffer & ostr,                                                 /// Where to write the result
            Context & context,                                                  /// DB, tables, data types, engines, functions, aggregate functions...
            BlockInputStreamPtr & query_plan,                                   /// Here could be written the description on how query was executed
            QueryProcessingStage::Enum stage = QueryProcessingStage::Complete); /// Up to which stage process the SELECT query

#. Comments should be written only in english

#. When writing a library, put it's detailed description in it's main header file.

#. You shouldn't write comments not providing additional information. For instance, you *CAN'T* write empty comments like this one:

    .. code-block:: cpp

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

    (example is borrowed from here: http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/)

#. You shouldn't write garbage comments (author, creation date...) in the beginning of each file.

#. One line comments should start with three slashes: ``///``, multiline - with ``/**``. This comments are considered "documenting".
    Note: such comments could be used to generate docs using Doxygen. But in reality Doxygen is not used because it is way more convenient to use IDE for code navigation.

#. In beginning and end of multiline comments there should be no empty lines (except the one where the comment ends).

#. For commented out code use simple, not "documenting" comments. Delete commented out code before commits.

#. Do not use profanity in comments or code.

#. Do not use too many question signs, exclamation points or capital letters.
    :strike:`/// WHAT THE FAIL???`

#. Do not make delimeters from comments.
    :strike:`/*******************************************************/`

#. Do not create discussions in comments.
    :strike:`/// Why you did this?`

#. Do not comment end of block describing what kind of block it was.
    :strike:`} /// for`


Names
-----

#. Names of variables and class members — in lowercase with underscores.

    .. code-block:: cpp

        size_t max_block_size;

#. Names of functions (methids) - in camelCase starting with lowercase letter.

    .. code-block:: cpp

        std::string getName() const override { return "Memory"; }

#. Names of classes (structs) - CamelCase starting with uppercase letter. Prefixes are not used, except I for interfaces.

    .. code-block:: cpp

        class StorageMemory : public IStorage


#. Names of ``using``'s - same as classes and can have _t suffix.

#. Names of template type arguments: in simple cases - T; T, U; T1, T2.
    In complex cases - like class names or can have T prefix.

    .. code-block:: cpp

        template <typename TKey, typename TValue>
        struct AggregatedStatElement

#. Names of template constant arguments: same as variable names or N in simple cases.

    .. code-block:: cpp

        template <bool without_www>
        struct ExtractDomain

#. For abstract classes (interfaces) you can add I to the start of name.

    .. code-block:: cpp

        class IBlockInputStream

#. If variable is used pretty locally, you can use short name.
    In other cases - use descriptive name.

    .. code-block:: cpp

        bool info_successfully_loaded = false;


#. ``define``'s should be in ALL_CAPS with underlines. Global constants - too.

    .. code-block:: cpp

        #define MAX_SRC_TABLE_NAMES_TO_STORE 1000

#. Names of files should match it's contents.
    If file contains one class - name it like class in CamelCase.
    If file contains one function - name it like function in camelCase.

#. If name contains an abbreviation:
    * for variables names it should be all lowercase;
        ``mysql_connection``
        :strike:`mySQL_connection`

    * for class and function names it should be all uppercase;
        ``MySQLConnection``
        :strike:`MySqlConnection`

#. Constructor arguments used just to initialize the class members, should have the matching name, but with underscore suffix.

    .. code-block:: cpp

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

    The underscore suffix can be omitted if argument is not used in constructor body.

#. Naming of local variables and class members do not have any differences (no prefixes required).
    ``timer``
    :strike:`m_timer`

#. Constants in enums - CamelCase starting with uppercase letter. ALL_CAPS is also ok. If enum is not local, use enum class.

    .. code-block:: cpp

        enum class CompressionMethod
        {
            QuickLZ = 0,
            LZ4     = 1,
        };

#. All names - in English. Transliteration from Russian is not allowed.
    :strike:`Stroka`

#. Abbreviations are fine only if they are well known (when you can find what it means in wikipedia or with web search query).

    ``AST`` ``SQL``
    :strike:`NVDH (some random letters)`

    Using incomplete words is ok if it is commonly used. Also you can put the whole word in comments.

#. C++ source code extensions should be .cpp. Header files - only .h.
    :strike:`.hpp` :strike:`.cc` :strike:`.C` :strike:`.inl`
    ``.inl.h`` is ok, but not :strike:`.h.inl:strike:`


How to write code
-----------------

#. Memory management.
    Manual memory deallocation (delete) is ok only in destructors in library code.
    In application code memory should be freed by some object that owns it.
    Examples:
    * you can put object on stack or make it a member of another class.
    * use containers for many small objects.
    * for automatic deallocation of not that many objects residing in heap, use shared_ptr/unique_ptr.

#. Resource management.
    Use RAII and see abovee.

#. Error handling.
    Use exceptions. In most cases you should only throw exception, but not catch (because of RAII).
    In offline data processing applications it's often ok not to catch exceptions.
    In server code serving user requests usually you should catch exceptions only on top level of connection handler.
    In thread functions you should catch and keep all exceptions to rethrow it in main thread after join.

    .. code-block:: cpp

        /// If there were no other calculations yet - lets do it synchronously
        if (!started)
        {
            calculate();
            started = true;
        }
        else    /// If the calculations are already in progress - lets wait
            pool.wait();

        if (exception)
            exception->rethrow();

    Never hide exceptions without handling. Never just blindly put all exceptions to log.
    :strike:`catch (...) {}`
    If you need to ignore some exceptions, do so only for specific ones and rethrow the rest..

    .. code-block:: cpp

        catch (const DB::Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
                return nullptr;
            else
                throw;
        }

    When using functions with error codes - always check it and throw exception in case of error.

    .. code-block:: cpp

        if (0 != close(fd))
            throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);

    Asserts are not used.

#. Exception types.
    No need to use complex exception hierarchy in application code. Exception code should be understandable by operations engineer.

#. Throwing exception from destructors.
    Not recommended, but allowed.
    Use the following options:
    * Create function (done() or finalize()) that will in advance do all the work that might lead to exception. If that function was called, later there should be no exceptions in destructor.
    * Too complex work (for example, sending messages via network) can be put in separate method that class user will have to call before destruction.
    * If nevertheless there's an exception in destructor it's better to log it that to hide it.
    * In simple applications it is ok to rely on std::terminate (in case of noexcept by default in C++11) to handle exception.

#. Anonymous code blocks.
    It is ok to declare anonymous code block to make some variables local to it and make them be destroyed earlier than they otherwise would.

    .. code-block:: cpp

        Block block = data.in->read();

        {
            std::lock_guard<std::mutex> lock(mutex);
            data.ready = true;
            data.block = block;
        }

        ready_any.set();

#. Multithreading.
    In case of offline data processing applications:
    * Try to make code as fast as possible on single core.
    * Make it parallel only if single core performance appeared to be not enough.
    In server application:
    * use thread pool for request handling;
    * for now there were no tasks where userspace context switching was really necessary.
    Fork is not used to parallelize code.

#. Synchronizing threads.
    Often it is possible to make different threads use different memory cells (better - different cache lines) and do not use any synchronization (except joinAll).
    If synchronization is necessary in most cases mutex under lock_guard is enough.
    In other cases use system synchronization primitives. Do not use busy wait.
    Atomic operations should be used only in the most simple cases.
    Do not try to implement lock-free data structures unless it is your primary area of expertise.

#. Pointers vs reference.
    Prefer references.

#. const.
    Use constant references, pointers to constants, const_iterator, const methods.
    Consider const to be default and use non-const only when necessary.
    When passing variable by value using const usually do not make sense.

#. unsigned.
    unsinged is ok if necessary.

#. Numeric types.
    Use types UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, as well as size_t, ssize_t, ptrdiff_t.
    Do not use типы signed/unsigned long, long long, short; signed char, unsigned char, аnd char.

#. Passing arguments.
    Pass complex values by reference (including std::string).
    If functions captures object ownership created in heap, make an argument to be shared_ptr or unique_ptr.

#. Returning values.
    In most cases just use return. Do not write :strike:`return std::move(res)`.
    If function allocates an object on heap and returns it, use shared_ptr or unique_ptr.
    In rare cases you might need to return value via argument, in this cases the argument should be a reference.

    .. code-block:: cpp

        using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

        /** Creates aggregate function by it's name
          */
        class AggregateFunctionFactory
        {
        public:
            AggregateFunctionFactory();
            AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;

#. namespace.
    No need to use separate namespace for application code or small libraries.
    For medium to large libraries - put everything in namespace.
    You can use additional detail namespace in .h file to hide implementation details.
    In .cpp you can use static or anonymous namespace to hide symbols.
    You can also use namespace for enums to prevent it's names to pollute outer namespace, but it's better to use enum class.

#. Delayed initialization.
    If arguments are required for initialization then do not write default constructor.
    If later you'll need to delay initialization you can add default constructor creating invalid object.
    For small number of object you could use shared_ptr/unique_ptr.

    .. code-block:: cpp

        Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

        /// For delayed initialization
        Loader() {}

#. Virtual methods.
    Do not mark methods or destructor as virtual if class is not intended for polymorph usage.

#. Encoding.
    Always use UTF-8. Use ``std::string``, ``char *``. Do not use ``std::wstring``, ``wchar_t``.

#. Logging.
    See examples in code.
    Remove debug logging before commit.
    Logging in cycles should be avoided, even on Trace level.
    Logs should be readable even with most detailed settings.
    Log mostly in application code.
    Log messages should be in English and understandable by operations engineers.

#. I/O.
    In internal cycle (critical for application performance) you can't use iostreams (especially stringstream).
    Instead use DB/IO library.

#. Date and time.
    See DateLUT library.

#. include.
    Always use ``#pragma once`` instead of include guards.

#. using.
    Don't use ``using namespace``. ``using`` something specific is fine, try to put it as locally as possible.

#. Do not use trailing return unless necessary.
    :strike:`auto f() -> void;`

#. Do not delcare and init variables like this:
    :strike:`auto s = std::string{"Hello"};`
    Do it like this instead:
    ``std::string s = "Hello";``
    ``std::string s{"Hello"};``

#. For virtual functions write virtual in base class and don't forget to write override in descendant classes.


Unused C++ features
-------------------

#. Virtual inheritance.

#. Exception specifiers from C++03.

#. Function try block, except main function in tests.


Platform
--------

#. We write code for specific platform. But other things equal cross platform and portable code is preferred.

#. Language is C++17.

#. Compiler is gcc. As of December 2017 version 7.2 is used. It is also compilable with clang 5.
    Standard library is used (libstdc++ or libc++).

#. OS - Linux Ubuntu, not older than Precise.

#. Code is written for x86_64 CPU architecture.
    CPU instruction set is SSE4.2 is currently required.

#. ``-Wall -Werror`` compilation flags are used.

#. Static linking is used by default. See ldd output for list of exceptions from this rule.

#. Code is developed and debugged with release settings.


Tools
-----

#. Good IDE - KDevelop.

#. For debugging gdb, valgrind (memcheck), strace, -fsanitize=..., tcmalloc_minimal_debug are used.

#. For profiling - Linux Perf, valgrind (callgrind), strace -cf.

#. Source code is in Git.

#. Compilation is managed by CMake.

#. Releases are in deb packages.

#. Commits to master should not break build.
    Though only selected revisions are considered workable.

#. Commit as often as possible, even if code is not quite ready yet.
    Use branches for this.
    If your code is not buildable yet, exclude it from build before pushing to master;
    you'll have few days to fix or delete it from master after that.

#. For non-trivial changes use branches and publish them on server.

#. Unused code is removed from repository.


Libraries
---------

#. C++14 standard library is used (experimental extensions are fine), as well as boost and Poco frameworks.

#. If necessary you can use any well known library available in OS with packages.
    If there's a good ready solution it is used even if it requires to install one more dependency.
    (Buy be prepared to remove bad libraries from code.)

#. It is ok to use library not from packages if it is not available or too old.

#. If the library is small and does not have it's own complex build system, you should put it's sources in contrib folder.

#. Already used libraries are preferred.


General
-------

#. Write as short code as possible.

#. Try the most simple solution.

#. Do not write code if you do not know how it will work yet.

#. In the most simple cases prefer using over classes and structs.

#. Write copy constructors, assignment operators, destructor (except virtual), mpve-constructor and move assignment operators only if there's no other option. You can use ``default``.

#. It is encouraged to simplify code.


Additional
----------

#. Explicit std:: for types from stddef.h.
    Not recommended, but allowed.

#. Explicit std:: for functions from C standard library.
    Not recommended. For example, write memcpy instead of std::memcpy.
    Sometimes there are non standard functions with similar names, like memmem. It will look weird to have memcpy with std:: prefix near memmem without. Though specifying std:: is not prohibited.

#. Usage of C functions when there are alternatives in C++ standard library.
    Allowed if they are more effective. For example, use memcpy instead of std::copy for copying large chunks of memory.

#. Multiline function arguments.
    All of the following styles are allowed:

    .. code-block:: cpp

        function(
            T1 x1,
            T2 x2)

    .. code-block:: cpp

        function(
            size_t left, size_t right,
            const & RangesInDataParts ranges,
            size_t limit)

    .. code-block:: cpp

        function(size_t left, size_t right,
            const & RangesInDataParts ranges,
            size_t limit)

    .. code-block:: cpp

        function(size_t left, size_t right,
                const & RangesInDataParts ranges,
                size_t limit)

    .. code-block:: cpp

        function(
                size_t left,
                size_t right,
                const & RangesInDataParts ranges,
                size_t limit)
