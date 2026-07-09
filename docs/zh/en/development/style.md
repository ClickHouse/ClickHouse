---
description: 'ClickHouse C++ 开发的编码风格指南'
sidebar_label: 'C++ 风格指南'
sidebar_position: 70
slug: /development/style
title: 'C++ 风格指南'
doc_type: 'guide'
---

<div id="general-recommendations">
  ## 一般建议
</div>

以下内容仅为建议，并非硬性要求。
如果你在编辑代码，遵循现有代码的格式是合理的。
代码风格对于保持一致性很有必要。一致性能让代码更易于阅读，也更便于搜索。
许多规则并没有明确的逻辑依据；它们只是约定俗成的做法。

<div id="formatting">
  ## 格式化
</div>

**1.** 大多数格式化工作由 `clang-format` 自动完成。

**2.** 缩进为 4 个空格。请配置开发环境，将 Tab 键设置为插入四个空格。

**3.** 左右花括号必须各占一行。

```cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** 如果整个函数体只有一条 `statement`，可以将其写在同一行。在花括号两侧添加空格 (行尾空格除外) 。

```cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** 对于函数，括号两侧不要加空格。

```cpp
void reinsert(const Value & x)
```

```cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** 在 `if`、`for`、`while` 及其他表达式中，左括号前需加一个空格 (与函数调用的写法相反) 。

```cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** 在二元运算符 (`+`、`-`、`*`、`/`、`%` 等) 和三元运算符 `?:` 的两侧添加空格。

```cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** 如果输入了换行符，请将运算符放到新行，并增加其前面的缩进。

```cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** 如有需要，可以在行内使用空格来对齐内容。

```cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** 不要在运算符 `.`、`->` 两侧使用空格。

如有必要，operator 可以折行至下一行。此时，其前面的缩进量会相应增加。

**11.** 不要在一元运算符 (`--`、`++`、`*`、`&` 等) 与参数之间使用空格。

**12.** 逗号后加空格，逗号前不加空格。`for` 表达式中的分号同理。

**13.** 不要在 `[]` 运算符两侧添加空格。

**14.** 在 `template <...>` 表达式中，`template` 与 `<` 之间需加空格；`<` 之后和 `>` 之前不加空格。

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** 在类和结构体中，`public`、`private` 和 `protected` 与 `class/struct` 保持同级缩进，其余代码向内缩进。

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

**16.** 如果整个文件使用相同的 `namespace`，且没有其他重要内容，则 `namespace` 内部无需缩进。

**17.** 如果 `if`、`for`、`while` 或其他表达式的代码块只包含单个 `statement`，则花括号是可选的。此时应将该 `statement` 单独置于一行。此规则同样适用于嵌套的 `if`、`for`、`while` ……

但如果内部 `statement` 包含花括号或 `else`，则外层块应使用花括号编写。

```cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** 行尾不应有任何空格。

**19.** 源文件采用 UTF-8 编码。

**20.** 字符串字面量中可以使用非 ASCII 字符。

```cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** 不要在同一行中编写多个表达式。

**22.** 将函数内的代码分组，各组之间最多只留一行空行。

**23.** 函数、类等之间用一到两行空行分隔。

**24.** `A const` (与值相关) 必须写在类型名称之前。

```cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** 声明指针或引用时，`*` 和 `&` 符号两侧都应留空格。

```cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** 使用模板类型时，请使用 `using` 关键字为其定义别名 (最简单的情况除外) 。

换句话说，模板参数只需在 `using` 中指定一次，无需在代码中重复。

`using` 可以在局部作用域中声明，例如在函数内部。

```cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** 不要在同一条语句中声明多个不同类型的变量。

```cpp
//incorrect
int x, *y;
```

**28.** 不要使用 C 风格的强制类型转换。

```cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** 在类和结构体中，应在每个可见性作用域内将成员和函数分别分组。

**30.** 对于较小的类和结构体，没有必要将方法声明与实现分离。

任何类或结构体中的较小方法也是如此。

对于模板类和结构体，不要将方法声明与实现分离 (否则它们就必须定义在同一个翻译单元中) 。

**31.** 行宽可以放宽到 140 个字符，而不必限制为 80 个。

**32.** 如果不需要后缀形式，始终使用前缀自增/自减运算符。

```cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

<div id="comments">
  ## 注释
</div>

**1.** 请务必为代码中所有不那么显而易见的部分添加注释。

这一点非常重要。编写注释的过程可能会帮助你意识到，这段代码其实并不必要，或者它的设计有问题。

```cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** 注释可以根据需要写得尽可能详细。

**3.** 将注释放在其所说明的代码之前。少数情况下，注释也可以与代码写在同一行，放在代码之后。

```cpp
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

**4.** 注释只能用英文书写。

**5.** 如果你在编写库，请在主头文件中添加详细的说明性注释。

**6.** 不要添加不能提供额外信息的注释。尤其不要留下像下面这样的空注释：

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

该示例引自 http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/。

**7.** 不要在每个文件开头写无用的注释 (如 author、创建日期等) 。

**8.** 单行注释以三个斜杠开头：`///`；多行注释以 `/**` 开头。这些注释被视为“文档”注释。

注意：你可以使用 Doxygen 根据这些注释生成文档。但通常并不使用 Doxygen，因为在 IDE 中浏览代码更方便。

**9.** 多行注释的开头和结尾不能有空行 (结束多行注释的那一行除外) 。

**10.** 如果要注释掉代码，请使用普通注释，不要使用“文档”注释。

**11.** 在提交前，删除已注释掉的代码片段。

**12.** 不要在注释或代码中使用脏话。

**13.** 不要使用大写字母。不要过度使用标点符号。

```cpp
/// WHAT THE FAIL???
```

**14.** 不要用注释作分隔符。

```cpp
///******************************************************
```

**15.** 不要在注释中展开讨论。

```cpp
/// Why did you do this stuff?
```

**16.** 没必要在代码块末尾再写注释来说明它的用途。

```cpp
/// for
```

<div id="names">
  ## 名称
</div>

**1.** 变量和类成员的名称应使用以下划线分隔的小写字母。

```cpp
size_t max_block_size;
```

**2.** 函数 (方法) 名称应使用首字母小写的驼峰命名法。

```cpp
std::string getName() const override { return "Memory"; }
```

**3.** 类 (结构体) 的名称应使用首字母大写的 CamelCase。接口除 I 外不使用其他前缀。

```cpp
class StorageMemory : public IStorage
```

**4.** `using` 的命名规则与类相同。

**5.** 模板类型参数的命名：在简单情况下，使用 `T`；`T`、`U`；`T1`、`T2`。

对于更复杂的情况，可以遵循类名规则，或添加前缀 `T`。

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** 模板常量参数名称：要么遵循变量命名规则，要么在简单情况下使用 `N`。

```cpp
template <bool without_www>
struct ExtractDomain
```

**7.** 对于抽象类 (接口) ，可添加 `I` 前缀。

```cpp
class IProcessor
```

**8.** 如果变量只在局部使用，可以使用短名称。

其他情况下，使用能体现其含义的名称。

```cpp
bool info_successfully_loaded = false;
```

**9.** `define` 和全局常量的名称应使用以下划线分隔的全大写形式。

```cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** 文件名应与其内容保持相同的风格。

如果文件只包含一个类，文件名应与类名采用相同的命名方式 (CamelCase) 。

如果文件只包含一个函数，文件名应与函数名采用相同的命名方式 (camelCase) 。

**11.** 如果名称中包含缩写，则：

* 对于变量名，缩写应使用小写字母，如 `mysql_connection` (不要写成 `mySQL_connection`) 。
* 对于类名和函数名，应保留缩写中的大写字母，如 `MySQLConnection` (不要写成 `MySqlConnection`) 。

**12.** 仅用于初始化类成员的构造函数参数，其命名应与类成员相同，但末尾要加上下划线。

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

如果该参数未在构造函数体中使用，则可省略下划线后缀。

**13.** 局部变量和类成员的名称没有区别 (无需添加前缀) 。

```cpp
timer (not m_timer)
```

**14.** 对于 `enum` 中的常量，使用首字母大写的 CamelCase。ALL&#95;CAPS 也可以接受。如果 `enum` 不是局部的，请使用 `enum class`。

```cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** 所有名称都必须使用英文。不允许将希伯来语单词音译。

不要使用 T&#95;PAAMAYIM&#95;NEKUDOTAYIM

**16.** 如果缩写广为人知，则可以接受 (也就是你可以在 Wikipedia 或搜索引擎中轻松查到它的含义) 。

`AST`、`SQL`。

不要用 `NVDH` (一些随意拼凑的字母)

如果单词的简写形式已被普遍使用，也可以接受不完整的单词。

如果在注释中同时写出了全称，也可以使用缩写。

**17.** C++ 源代码的文件名必须使用 `.cpp` 扩展名。头文件必须使用 `.h` 扩展名。

<div id="how-to-write-code">
  ## 如何编写代码
</div>

**1.** 内存管理。

手动释放内存 (`delete`) 只能用于库代码。

在库代码中，`delete` 运算符只能在析构函数中使用。

在应用代码中，内存必须由其所属对象释放。

示例：

* 最简单的方式是将对象放在栈上，或作为另一个类的成员。
* 对于大量小对象，请使用容器。
* 对于少量分配在堆上的对象，如需自动释放，请使用 `shared_ptr/unique_ptr`。

**2.** 资源管理。

使用 `RAII`，并参见上文。

**3.** 错误处理。

使用异常。在大多数情况下，你只需要抛出异常，不需要捕获它 (因为有 `RAII`) 。

在离线数据处理应用中，通常可以不捕获异常。

在处理用户请求的服务器中，通常只需在 connection handler 的顶层捕获异常即可。

在线程函数中，你应该捕获并保存所有异常，以便在 `join` 之后在主线程中重新抛出。

```cpp
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

绝不要隐藏未处理的异常。也绝不要不加区分地把所有异常都记入日志。

```cpp
//Not correct
catch (...) {}
```

如果需要忽略某些异常，只应忽略特定的异常，其余异常应继续抛出。

```cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

使用带有返回码或 `errno` 的函数时，务必检查结果，并在出错时抛出异常。

```cpp
if (0 != close(fd))
    throw ErrnoException(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file {}", file_name);
```

你可以使用 assert 来检查代码中的不变量。

**4.** 异常类型。

在应用代码中，没有必要使用复杂的异常层次结构。异常文本应当能让系统管理员看懂。

**5.** 从析构函数中抛出异常。

不建议这样做，但这是允许的。

可采用以下做法：

* 创建一个函数 (`done()` 或 `finalize()`) ，提前完成所有可能导致异常的工作。如果该函数已经调用，之后析构函数中就不应再出现异常。
* 过于复杂的任务 (例如通过网络发送消息) 可以放在单独的方法中，由类的使用者在销毁前调用。
* 如果析构函数中出现异常，与其将其掩盖，不如把它记录到日志中 (如果日志记录器可用) 。
* 在简单应用中，可以接受依赖 `std::terminate` (用于处理 C++11 中默认 `noexcept` 的情况) 来处理异常。

**6.** 匿名代码块。

你可以在单个函数内创建独立的代码块，以便将某些变量限制为局部变量，这样在退出该块时就会调用析构函数。

```cpp
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

* 尽量先把单个 CPU 核心上的性能做到最好。然后再根据需要对代码进行并行化。

在服务器应用中：

* 使用线程池处理请求。到目前为止，我们还没有遇到需要 userspace 上下文切换的任务。

不要使用 fork 来做并行化。

**8.** 线程同步。

很多情况下，可以让不同线程使用不同的内存单元 (更理想的是不同的缓存行) ，并且完全不做线程同步 (`joinAll` 除外) 。

如果确实需要同步，大多数情况下使用 `lock_guard` 配合 mutex 就足够了。

其他情况下，请使用系统同步原语。不要使用忙等。

原子操作只应在最简单的场景下使用。

除非无锁数据结构正是你的主攻方向，否则不要尝试实现它。

**9.** 指针与引用。

大多数情况下，优先使用引用。

**10.** `const`。

使用常量引用、指向常量的指针、`const_iterator` 和 `const` 方法。

把 `const` 视为默认选择，只有在必要时才使用非 `const`。

按值传递变量时，使用 `const` 通常没有意义。

**11.** unsigned。

必要时使用 `unsigned`。

**12.** 数值类型。

使用类型 `UInt8`、`UInt16`、`UInt32`、`UInt64`、`Int8`、`Int16`、`Int32` 和 `Int64`，以及 `size_t`、`ssize_t` 和 `ptrdiff_t`。

不要用这些类型来表示数字：`signed/unsigned long`、`long long`、`short`、`signed/unsigned char`、`char`。

**13.** 传递参数。

如果复杂值后续会被移动，就按值传递并使用 `std::move`；如果需要在循环中更新某个值，就按引用传递。

如果函数会接管堆上创建对象的所有权，请将参数类型设为 `shared_ptr` 或 `unique_ptr`。

**14.** 返回值。

在大多数情况下，直接使用 `return` 即可。不要写 `return std::move(res)`。

如果函数在堆上分配对象并返回它，请使用 `shared_ptr` 或 `unique_ptr`。

在少数情况下 (例如在循环中更新某个值) ，你可能需要通过参数返回该值。此时，该参数应为引用。

```cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Allows creating an aggregate function by its name.
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** `namespace`。

应用程序代码不必专门使用单独的 `namespace`。

小型库同样不需要这么做。

对于中大型库，应将所有内容都放在一个 `namespace` 中。

在库的 `.h` 文件中，可以使用 `namespace detail` 来隐藏应用程序代码不需要的实现细节。

在 `.cpp` 文件中，可以使用 `static` 或匿名 `namespace` 来隐藏符号。

此外，也可以将 `namespace` 与 `enum` 配合使用，避免相应的名称落入外部 `namespace` (不过更好的做法是使用 `enum class`) 。

**16.** 延迟初始化。

如果初始化需要参数，通常就不应该编写默认构造函数。

如果之后需要延迟初始化，可以添加一个默认构造函数来创建无效对象。或者，当对象数量较少时，也可以使用 `shared_ptr/unique_ptr`。

```cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** 虚函数。

如果某个类不打算用于多态，就不需要把函数声明为 virtual。析构函数也是如此。

**18.** 编码。

统一使用 UTF-8。使用 `std::string` 和 `char *`。不要使用 `std::wstring` 和 `wchar_t`。

**19.** 日志。

请参考代码中的各处示例。

提交前，删除所有无意义的日志、调试日志以及其他任何调试输出。

应避免在循环中记录日志，即使是在 Trace 级别也是如此。

无论在哪个日志级别下，日志都必须可读。

在大多数情况下，日志只应在应用代码中使用。

日志消息必须使用英文编写。

日志最好能让系统管理员看懂。

不要在日志中使用脏话。

日志中使用 UTF-8 编码。在极少数情况下，可以在日志中使用非 ASCII 字符。

**20.** 输入输出。

不要在对应用性能至关重要的内部循环中使用 `iostreams` (更不要使用 `stringstream`) 。

请改用 `DB/IO` 库。

**21.** 日期和时间。

请参见 `DateLUT` 库。

**22.** include。

始终使用 `#pragma once`，不要使用头文件保护宏。

**23.** using。

不要使用 `using namespace`。可以对特定对象使用 `using`，但应将其局部限定在类或函数内部。

**24.** 除非必要，不要对函数使用 `trailing return type`。

```cpp
auto f() -> void
```

**25.** 变量声明与初始化。

```cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** 对于虚函数，在基类中使用 `virtual`，但在派生类中应写 `override`，而不是 `virtual`。

<div id="unused-features-of-c">
  ## C++ 中未使用的特性
</div>

**1.** 不使用虚继承。

**2.** 例如，现代 C++ 中那些有便捷语法糖的构造。

```cpp
// Traditional way without syntactic sugar
template <typename G, typename = std::enable_if_t<std::is_same<G, F>::value, void>> // SFINAE via std::enable_if, usage of ::value
std::pair<int, int> func(const E<G> & e) // explicitly specified return type
{
    if (elements.count(e)) // .count() membership test
    {
        // ...
    }

    elements.erase(
        std::remove_if(
            elements.begin(), elements.end(),
            [&](const auto x){
                return x == 1;
            }),
        elements.end()); // remove-erase idiom

    return std::make_pair(1, 2); // create pair via make_pair()
}

// With syntactic sugar (C++14/17/20)
template <typename G>
requires std::same_v<G, F> // SFINAE via C++20 concept, usage of C++14 template alias
auto func(const E<G> & e) // auto return type (C++14)
{
    if (elements.contains(e)) // C++20 .contains membership test
    {
        // ...
    }

    elements.erase_if(
        elements,
        [&](const auto x){
            return x == 1;
        }); // C++20 std::erase_if

    return {1, 2}; // or: return std::pair(1, 2); // create pair via initialization list or value initialization (C++17)
}
```

<div id="platform">
  ## 平台
</div>

**1.** 我们为特定平台编写代码。

但在其他条件相同的情况下，优先选择跨平台或可移植的代码。

**2.** 语言：C++20 (参见可用的 [C++20 特性列表](https://en.cppreference.com/w/cpp/compiler_support#C.2B.2B20_features)) 。

**3.** 编译器：`clang`。在撰写本文时 (2025 年 3 月) ，代码使用 &gt;= 19 版本的 clang 编译。

使用标准库 (`libc++`) 。

**4.** 操作系统：Linux Ubuntu，版本不低于 Precise。

**5.** 代码面向 x86&#95;64 CPU 架构编写。

CPU 指令集采用我们服务器所支持的最低集合。目前为 SSE 4.2。

**6.** 使用 `-Wall -Wextra -Werror -Weverything` 编译选项，只有少数例外。

**7.** 除了那些难以静态链接的库外，所有库都使用静态链接 (参见 `ldd` 命令的输出) 。

**8.** 代码在发布配置下进行开发和调试。

<div id="tools">
  ## 工具
</div>

**1.** KDevelop 是一款不错的 IDE。

**2.** 如需调试，请使用 `gdb`、`valgrind` (`memcheck`) 、`strace`、`-fsanitize=...` 或 `tcmalloc_minimal_debug`。

**3.** 如需进行性能分析，请使用 `Linux Perf`、`valgrind` (`callgrind`) 或 `strace -cf`。

**4.** 源代码托管在 Git 中。

**5.** 构建使用 `CMake`。

**6.** 程序以 `deb` 软件包形式发布。

**7.** 提交到 master 的更改不得破坏构建。

不过，只有部分选定的修订版本会被视为可正常使用。

**8.** 应尽可能频繁地提交，即使代码还只是部分完成。

为此请使用分支。

如果你在 `master` 分支中的代码还无法构建，请在推送前将其从构建中排除。你需要在几天内将其完成或删除。

**9.** 对于较复杂的更改，请使用分支并将其推送到服务器。

**10.** 未使用的代码会从仓库中移除。

<div id="libraries">
  ## 库
</div>

**1.** 使用 C++20 标准库 (允许使用 Experimental 扩展) ，以及 `boost` 和 `Poco` 框架。

**2.** 不允许使用操作系统软件包中的库，也不允许使用预装库。所有库都应以源代码形式放在 `contrib` 目录中，并与 ClickHouse 一起构建。详情请参阅[添加新的 third-party libraries 指南](/zh/development/contrib#adding-and-maintaining-third-party-libraries)。

**3.** 始终优先选择已在使用的库。

<div id="general-recommendations">
  ## 一般建议
</div>

**1.** 尽量少写代码。

**2.** 先尝试最简单的解决方案。

**3.** 在弄清楚代码将如何工作，以及内部循环将如何运行之前，不要动手写代码。

**4.** 在最简单的情况下，优先使用 `using`，而不是类或结构体。

**5.** 如果可能，不要编写拷贝构造函数、赋值运算符、析构函数 (如果类中至少包含一个虚函数，则虚析构函数除外) 、移动构造函数或移动赋值运算符。换句话说，要让编译器自动生成的函数能够正常工作。你可以使用 `default`。

**6.** 鼓励简化代码。在可能的情况下，尽量缩减代码体量。

<div id="additional-recommendations">
  ## 其他建议
</div>

**1.** 为来自 `stddef.h` 的类型显式加上 `std::`

不推荐这样做。换句话说，我们建议写 `size_t`，而不是 `std::size_t`，因为前者更短。

当然，加上 `std::` 也是可以接受的。

**2.** 为标准 C 库中的函数显式加上 `std::`

不推荐这样做。换句话说，写 `memcpy`，而不是 `std::memcpy`。

原因是还有一些类似的非标准函数，例如 `memmem`。我们确实偶尔会用到这些函数。而这些函数并不存在于 `namespace std` 中。

如果你到处都写 `std::memcpy` 而不是 `memcpy`，那么不带 `std::` 的 `memmem` 看起来就会很奇怪。

不过，如果你更喜欢，也还是可以使用 `std::`。

**3.** 当标准 C++ 库中提供了相同功能时，使用 C 中的函数。

如果这样效率更高，这是可以接受的。

例如，复制大块内存时，使用 `memcpy` 而不是 `std::copy`。

**4.** 多行函数参数。

以下任一种换行风格都可以：

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