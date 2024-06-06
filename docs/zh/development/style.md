---
slug: /zh/development/style
---
# 如何编写 C++ 代码 {#ru-he-bian-xie-c-dai-ma}

## 一般建议 {#yi-ban-jian-yi}

**1.** 以下是建议，而不是要求。

**2.** 如果你在修改代码，遵守已有的风格是有意义的。

**3.** 代码的风格需保持一致。一致的风格有利于阅读代码，并且方便检索代码。

**4.** 许多规则没有逻辑原因； 它们是由既定的做法决定的。

## 格式化 {#ge-shi-hua}

**1.** 大多数格式化可以用 `clang-format` 自动完成。

**2.** 缩进是4个空格。 配置开发环境，使得 TAB 代表添加四个空格。

**3.** 左右花括号需在单独的行。

``` cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** 若整个方法体仅有一行 `描述`， 则可以放到单独的行上。 在花括号周围放置空格（除了行尾的空格）。

``` cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** 对于函数。 不要在括号周围放置空格。

``` cpp
void reinsert(const Value & x)
```

``` cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** 在`if`，`for`，`while`和其他表达式中，在开括号前面插入一个空格（与函数声明相反）。

``` cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** 在二元运算符（`+`，`-`，`*`，`/`，`％`，...）和三元运算符 `?:` 周围添加空格。

``` cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** 若有换行，新行应该以运算符开头，并且增加对应的缩进。

``` cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** 如果需要，可以在一行内使用空格来对齐。

``` cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** 不要在 `.`，`->` 周围加入空格

如有必要，运算符可以包裹到下一行。 在这种情况下，它前面的偏移量增加。

**11.** 不要使用空格来分开一元运算符 (`--`, `++`, `*`, `&`, ...) 和参数。

**12.** 在逗号后面加一个空格，而不是在之前。同样的规则也适合 `for` 循环中的分号。

**13.** 不要用空格分开 `[]` 运算符。

**14.** 在 `template <...>` 表达式中，在 `template` 和 `<` 中加入一个空格，在 `<` 后面或在 `>` 前面都不要有空格。

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** 在类和结构体中， `public`， `private` 以及 `protected` 同 `class/struct` 无需缩进，其他代码须缩进。

``` cpp
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;
    ...
}
```

**16.** 如果对整个文件使用相同的 `namespace`，并且没有其他重要的东西，则 `namespace` 中不需要偏移量。

**17.** 在 `if`, `for`, `while` 中包裹的代码块中，若代码是一个单行的 `statement`，那么大括号是可选的。 可以将 `statement` 放到一行中。这个规则同样适用于嵌套的 `if`， `for`， `while`， ...

但是如果内部 `statement` 包含大括号或 `else`，则外部块应该用大括号括起来。

``` cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** 行的末尾不应该包含空格。

**19.** 源文件应该用 UTF-8 编码。

**20.** 非ASCII字符可用于字符串文字。

``` cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21** 不要在一行中写入多个表达式。

**22.** 将函数内部的代码段分组，并将它们与不超过一行的空行分开。

**23.** 将 函数，类用一个或两个空行分开。

**24.** `const` 必须写在类型名称之前。

``` cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** 声明指针或引用时，`*` 和 `＆` 符号两边应该都用空格分隔。

``` cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** 使用模板类型时，使用 `using` 关键字对它们进行别名（最简单的情况除外）。

换句话说，模板参数仅在 `using` 中指定，并且不在代码中重复。

`using`可以在本地声明，例如在函数内部。

``` cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** 不要在一个语句中声明不同类型的多个变量。

``` cpp
//incorrect
int x, *y;
```

**28.** 不要使用C风格的类型转换。

``` cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** 在类和结构中，组成员和函数分别在每个可见范围内。

**30.** 对于小类和结构，没有必要将方法声明与实现分开。

对于任何类或结构中的小方法也是如此。

对于模板化类和结构，不要将方法声明与实现分开（因为否则它们必须在同一个转换单元中定义）

**31.** 您可以将换行规则定在140个字符，而不是80个字符。

**32.** 如果不需要 postfix，请始终使用前缀增量/减量运算符。

``` cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

## 评论 {#comments}

**1.** 请务必为所有非常重要的代码部分添加注释。

这是非常重要的。 编写注释可能会帮助您意识到代码不是必需的，或者设计错误。

``` cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** 注释可以尽可能详细。

**3.** 在他们描述的代码之前放置注释。 在极少数情况下，注释可以在代码之后，在同一行上。

``` cpp
/** Parses and executes the query.
*/
void executeQuery(
    ReadBuffer & istr, /// Where to read the query from (and data for INSERT, if applicable)
    WriteBuffer & ostr, /// Where to write the result
    Context & context, /// DB, tables, data types, engines, functions, aggregate functions...
    BlockInputStreamPtr & query_plan, /// Here could be written the description on how query was executed
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete /// Up to which stage process the SELECT query
    )
```

**4.** 注释应该只用英文撰写。

**5.** 如果您正在编写库，请在主头文件中包含解释它的详细注释。

**6.** 请勿添加无效的注释。 特别是，不要留下像这样的空注释：

``` cpp
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

这个示例来源于 http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/。

**7.** 不要在每个文件的开头写入垃圾注释（作者，创建日期...）。

**8.** 单行注释用三个斜杆： `///` ，多行注释以 `/**`开始。 这些注释会当做文档。

注意：您可以使用 Doxygen 从这些注释中生成文档。 但是通常不使用 Doxygen，因为在 IDE 中导航代码更方便。

**9.** 多行注释的开头和结尾不得有空行（关闭多行注释的行除外）。

**10.** 要注释掉代码，请使用基本注释，而不是“文档”注释。

**11.** 在提交之前删除代码的无效注释部分。

**12.** 不要在注释或代码中使用亵渎语言。

**13.** 不要使用大写字母。 不要使用过多的标点符号。

``` cpp
/// WHAT THE FAIL???
```

**14.** 不要使用注释来制作分隔符。

``` cpp
///******************************************************
```

**15.** 不要在注释中开始讨论。

``` cpp
/// Why did you do this stuff?
```

**16.** 没有必要在块的末尾写一条注释来描述它的含义。

``` cpp
/// for
```

## 姓名 {#names}

**1.** 在变量和类成员的名称中使用带下划线的小写字母。

``` cpp
size_t max_block_size;
```

**2.** 对于函数（方法）的名称，请使用以小写字母开头的驼峰标识。

``` cpp
std::string getName() const override { return "Memory"; }
```

**3.** 对于类（结构）的名称，使用以大写字母开头的驼峰标识。接口名称用I前缀。

``` cpp
class StorageMemory : public IStorage
```

**4.** `using` 的命名方式与类相同，或者以__t\`命名。

**5.** 模板类型参数的名称：在简单的情况下，使用`T`; `T`，`U`; `T1`，`T2`。

对于更复杂的情况，要么遵循类名规则，要么添加前缀`T`。

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** 模板常量参数的名称：遵循变量名称的规则，或者在简单的情况下使用 `N`。

``` cpp
template <bool without_www>
struct ExtractDomain
```

**7.** 对于抽象类（接口），用 `I` 前缀。

``` cpp
class IBlockInputStream
```

**8.** 如果在本地使用变量，则可以使用短名称。

在所有其他情况下，请使用能描述含义的名称。

``` cpp
bool info_successfully_loaded = false;
```

**9.** `define` 和全局常量的名称使用全大写带下划线的形式，如 `ALL_CAPS`。

``` cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** 文件名应使用与其内容相同的样式。

如果文件包含单个类，则以与该类名称相同的方式命名该文件（CamelCase）。

如果文件包含单个函数，则以与函数名称相同的方式命名文件（camelCase）。

**11.** 如果名称包含缩写，则：

-   对于变量名，缩写应使用小写字母 `mysql_connection`（不是 `mySQL_connection` ）。
-   对于类和函数的名称，请将大写字母保留在缩写 `MySQLConnection`（不是 `MySqlConnection`）。

**12.** 仅用于初始化类成员的构造方法参数的命名方式应与类成员相同，但最后使用下划线。

``` cpp
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

如果构造函数体中未使用该参数，则可以省略下划线后缀。

**13.** 局部变量和类成员的名称没有区别（不需要前缀）。

``` cpp
timer (not m_timer)
```

**14.** 对于 `enum` 中的常量，请使用带大写字母的驼峰标识。ALL_CAPS 也可以接受。如果 `enum` 是非本地的，请使用 `enum class`。

``` cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** 所有名字必须是英文。不允许音译俄语单词。

    not Stroka

**16.** 缩写须是众所周知的（当您可以在维基百科或搜索引擎中轻松找到缩写的含义时）。

    `AST`, `SQL`.

    Not `NVDH` (some random letters)

如果缩短版本是常用的，则可以接受不完整的单词。

如果旁边有注释包含全名，您也可以使用缩写。

**17.** C++ 源码文件名称必须为 `.cpp` 拓展名。 头文件必须为 `.h` 拓展名。

## 如何编写代码 {#ru-he-bian-xie-dai-ma}

**1.** 内存管理。

手动内存释放 (`delete`) 只能在库代码中使用。

在库代码中， `delete` 运算符只能在析构函数中使用。

在应用程序代码中，内存必须由拥有它的对象释放。

示例：

-   最简单的方法是将对象放在堆栈上，或使其成为另一个类的成员。
-   对于大量小对象，请使用容器。
-   对于自动释放少量在堆中的对象，可以用 `shared_ptr/unique_ptr`。

**2.** 资源管理。

使用 `RAII` 以及查看以上说明。

**3.** 错误处理。

在大多数情况下，您只需要抛出一个异常，而不需要捕获它（因为`RAII`）。

在离线数据处理应用程序中，通常可以接受不捕获异常。

在处理用户请求的服务器中，捕获连接处理程序顶层的异常通常就足够了。

在线程函数中，你应该在 `join` 之后捕获并保留所有异常以在主线程中重新抛出它们。

``` cpp
/// If there weren't any calculations yet, calculate the first block synchronously
if (!started)
{
    calculate();
    started = true;
}
else /// If calculations are already in progress, wait for the result
    pool.wait();

if (exception)
    exception->rethrow();
```

不处理就不要隐藏异常。 永远不要盲目地把所有异常都记录到日志中。

``` cpp
//Not correct
catch (...) {}
```

如果您需要忽略某些异常，请仅针对特定异常执行此操作并重新抛出其余异常。

``` cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

当使用具有返回码或 `errno` 的函数时，请始终检查结果并在出现错误时抛出异常。

``` cpp
if (0 != close(fd))
    throw ErrnoException(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file {}", file_name);
```

`不要使用断言`。

**4.** 异常类型。

不需要在应用程序代码中使用复杂的异常层次结构。 系统管理员应该可以理解异常文本。

**5.** 从析构函数中抛出异常。

不建议这样做，但允许这样做。

按照以下选项：

-   创建一个函数（ `done（）` 或 `finalize（）` ），它将提前完成所有可能导致异常的工作。 如果调用了该函数，则稍后在析构函数中应该没有异常。
-   过于复杂的任务（例如通过网络发送消息）可以放在单独的方法中，类用户必须在销毁之前调用它们。
-   如果析构函数中存在异常，则最好记录它而不是隐藏它（如果 logger 可用）。
-   在简单的应用程序中，依赖于`std::terminate`（对于C++ 11中默认情况下为 `noexcept` 的情况）来处理异常是可以接受的。

**6.** 匿名代码块。

您可以在单个函数内创建单独的代码块，以使某些变量成为局部变量，以便在退出块时调用析构函数。

``` cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** 多线程。

在离线数据处理程序中：

-   尝试在单个CPU核心上获得最佳性能。 然后，您可以根据需要并行化代码。

在服务端应用中：

-   使用线程池来处理请求。 此时，我们还没有任何需要用户空间上下文切换的任务。

Fork不用于并行化。

**8.** 同步线程。

通常可以使不同的线程使用不同的存储单元（甚至更好：不同的缓存线），并且不使用任何线程同步（除了`joinAll`）。

如果需要同步，在大多数情况下，在 `lock_guard` 下使用互斥量就足够了。

在其他情况下，使用系统同步原语。不要使用忙等待。

仅在最简单的情况下才应使用原子操作。

除非是您的主要专业领域，否则不要尝试实施无锁数据结构。

**9.** 指针和引用。

大部分情况下，请用引用。

**10.** 常量。

使用 const 引用、指针，指向常量、`const_iterator`和 const 方法。

将 `const` 视为默认值，仅在必要时使用非 `const`。

当按值传递变量时，使用 `const` 通常没有意义。

**11.** 无符号。

必要时使用`unsigned`。

**12.** 数值类型。

使用 `UInt8`， `UInt16`， `UInt32`， `UInt64`， `Int8`， `Int16`， `Int32` 和 `Int64`，同样还有 `size_t`， `ssize_t` 和 `ptrdiff_t`。

不要使用这些类型：`signed / unsigned long`，`long long`，`short`，`signed / unsigned char`，`char`。

**13.** 参数传递。

通过引用传递复杂类型 （包括 `std::string`）。

如果函数中传递堆中创建的对象，则使参数类型为 `shared_ptr` 或者 `unique_ptr`.

**14.** 返回值

大部分情况下使用 `return`。不要使用 `return std::move(res)`。

如果函数在堆上分配对象并返回它，请使用 `shared_ptr` 或 `unique_ptr`。

在极少数情况下，您可能需要通过参数返回值。 在这种情况下，参数应该是引用传递的。

``` cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Allows creating an aggregate function by its name.
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** 命名空间。

没有必要为应用程序代码使用单独的 `namespace` 。

小型库也不需要这个。

对于中大型库，须将所有代码放在 `namespace` 中。

在库的 `.h` 文件中，您可以使用 `namespace detail` 来隐藏应用程序代码不需要的实现细节。

在 `.cpp` 文件中，您可以使用 `static` 或匿名命名空间来隐藏符号。

同样 `namespace` 可用于 `enum` 以防止相应的名称落入外部 `namespace`（但最好使用`enum class`）。

**16.** 延迟初始化。

如果初始化需要参数，那么通常不应该编写默认构造函数。

如果稍后您需要延迟初始化，则可以添加将创建无效对象的默认构造函数。 或者，对于少量对象，您可以使用 `shared_ptr / unique_ptr`。

``` cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** 虚函数。

如果该类不是用于多态使用，则不需要将函数设置为虚拟。这也适用于析构函数。

**18.** 编码。

在所有情况下使用 UTF-8 编码。使用 `std::string` 和 `char *`。不要使用 `std::wstring` 和 `wchar_t`。

**19.** 日志。

请参阅代码中的示例。

在提交之前，删除所有无意义和调试日志记录，以及任何其他类型的调试输出。

应该避免循环记录日志，即使在 Trace 级别也是如此。

日志必须在任何日志记录级别都可读。

在大多数情况下，只应在应用程序代码中使用日志记录。

日志消息必须用英文写成。

对于系统管理员来说，日志最好是可以理解的。

不要在日志中使用亵渎语言。

在日志中使用UTF-8编码。 在极少数情况下，您可以在日志中使用非ASCII字符。

**20.** 输入-输出。

不要使用 `iostreams` 在对应用程序性能至关重要的内部循环中（并且永远不要使用 `stringstream` ）。

使用 `DB/IO` 库替代。

**21.** 日期和时间。

参考 `DateLUT` 库。

**22.** 引入头文件。

一直用 `#pragma once` 而不是其他宏。

**23.** using 语法

`using namespace` 不会被使用。 您可以使用特定的 `using`。 但是在类或函数中使它成为局部的。

**24.** 不要使用 `trailing return type` 为必要的功能。

``` cpp
auto f() -> void
```

**25.** 声明和初始化变量。

``` cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** 对于虚函数，在基类中编写 `virtual`，但在后代类中写 `override` 而不是`virtual`。

## 没有用到的 C++ 特性。 {#mei-you-yong-dao-de-c-te-xing}

**1.** 不使用虚拟继承。

**2.** 不使用 C++03 中的异常标准。

## 平台 {#ping-tai}

**1.** 我们为特定平台编写代码。

但在其他条件相同的情况下，首选跨平台或可移植代码。

**2.** 语言： C++20.

**3.** 编译器： `clang`。 此时（2021年03月），代码使用11版编译。（它也可以使用`gcc` 编译 but it is not suitable for production）

使用标准库 (`libc++`)。

**4.** 操作系统：Linux Ubuntu，不比 Precise 早。

**5.** 代码是为x86_64 CPU架构编写的。

CPU指令集是我们服务器中支持的最小集合。 目前，它是SSE 4.2。

**6.** 使用 `-Wall -Wextra -Werror` 编译参数。

**7.** 对所有库使用静态链接，除了那些难以静态连接的库（参见 `ldd` 命令的输出）.

**8.** 使用发布的设置来开发和调试代码。

## 工具 {#gong-ju}

**1.** KDevelop 是一个好的 IDE.

**2.** 调试可以使用 `gdb`， `valgrind` (`memcheck`)， `strace`， `-fsanitize=...`， 或 `tcmalloc_minimal_debug`.

**3.** 对于性能分析，使用 `Linux Perf`， `valgrind` (`callgrind`)，或者 `strace -cf`。

**4.** 源代码用 Git 作版本控制。

**5.** 使用 `CMake` 构建。

**6.** 程序的发布使用 `deb` 安装包。

**7.** 提交到 master 分支的代码不能破坏编译。

虽然只有选定的修订被认为是可行的。

**8.** 尽可能经常地进行提交，即使代码只是部分准备好了。

为了这种目的可以创建分支。

如果您的代码在 `master` 分支中尚不可构建，在 `push` 之前需要将其从构建中排除。您需要在几天内完成或删除它。

**9.** 对于非一般的更改，请使用分支并在服务器上发布它们。

**10.** 未使用的代码将从 repo 中删除。

## 库 {#ku}

**1.** The C++20 standard library is used (experimental extensions are allowed), as well as `boost` and `Poco` frameworks.

**2.** It is not allowed to use libraries from OS packages. It is also not allowed to use pre-installed libraries. All libraries should be placed in form of source code in `contrib` directory and built with ClickHouse.

**3.** Preference is always given to libraries that are already in use.

## 一般建议 {#yi-ban-jian-yi-1}

**1.** 尽可能精简代码。

**2.** 尝试用最简单的方式实现。

**3.** 在你知道代码是如何工作以及内部循环如何运作之前，不要编写代码。

**4.** 在最简单的情况下，使用 `using` 而不是类或结构。

**5.** 如果可能，不要编写复制构造函数，赋值运算符，析构函数（虚拟函数除外，如果类包含至少一个虚函数），移动构造函数或移动赋值运算符。 换句话说，编译器生成的函数必须正常工作。 您可以使用 `default`。

**6.** 鼓励简化代码。 尽可能减小代码的大小。

## 其他建议 {#qi-ta-jian-yi}

**1.** 从 `stddef.h` 明确指定 `std ::` 的类型。

不推荐。 换句话说，我们建议写 `size_t` 而不是 `std::size_t`，因为它更短。

也接受添加 `std::`。

**2.** 为标准C库中的函数明确指定 `std::`

不推荐。换句话说，写 `memcpy` 而不是`std::memcpy`。

原因是有类似的非标准功能，例如 `memmem`。我们偶尔会使用这些功能。`namespace std`中不存在这些函数。

如果你到处都写 `std::memcpy` 而不是 `memcpy`，那么没有 `std::` 的 `memmem` 会显得很奇怪。

不过，如果您愿意，仍然可以使用 `std::`。

**3.** 当标准C++库中提供相同的函数时，使用C中的函数。

如果它更高效，这是可以接受的。

例如，使用`memcpy`而不是`std::copy`来复制大块内存。

**4.** 函数的多行参数。

允许以下任何包装样式：

``` cpp
function(
  T1 x1,
  T2 x2)
```

``` cpp
function(
  size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

``` cpp
function(size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

``` cpp
function(size_t left, size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

``` cpp
function(
      size_t left,
      size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```
