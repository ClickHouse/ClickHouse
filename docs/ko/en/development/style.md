---
description: 'ClickHouse C++ 개발을 위한 코딩 스타일 가이드'
sidebar_label: 'C++ 스타일 가이드'
sidebar_position: 70
slug: /development/style
title: 'C++ 스타일 가이드'
doc_type: 'guide'
---

<div id="general-recommendations">
  ## 일반 권장 사항
</div>

다음은 필수 사항이 아니라 권장 사항입니다.
코드를 편집하는 경우 기존 코드의 포맷을 따르는 것이 좋습니다.
일관성을 유지하려면 코드 스타일이 필요합니다. 일관성이 있으면 코드를 읽기 쉬워지고, 코드를 검색하기도 쉬워집니다.
많은 규칙에는 논리적인 이유가 없으며, 오랜 관행에 따라 정해진 것입니다.

<div id="formatting">
  ## 포맷팅
</div>

**1.** 대부분의 포매팅은 `clang-format`이 자동으로 처리합니다.

**2.** 들여쓰기는 공백 4칸입니다. 탭 키 입력 시 공백 4개가 삽입되도록 개발 환경을 구성하십시오.

**3.** 여는 중괄호와 닫는 중괄호는 각각 별도의 줄에 작성해야 합니다.

```cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** 함수 본문(body) 전체가 단일 `statement`인 경우, 한 줄로 작성할 수 있습니다. 중괄호 양쪽에 공백을 넣으십시오(줄 끝 공백은 제외).

```cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** 함수의 경우, 괄호 앞뒤에 공백을 넣지 마십시오.

```cpp
void reinsert(const Value & x)
```

```cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** `if`, `for`, `while` 및 기타 표현식에서는 여는 괄호 앞에 공백을 삽입합니다(함수 호출과는 달리).

```cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** 이항 연산자(`+`, `-`, `*`, `/`, `%`, ...) 및 삼항 연산자 `?:` 앞뒤에 공백을 추가하십시오.

```cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** 줄 바꿈이 입력된 경우, 연산자를 새 줄에 배치하고 그 앞의 들여쓰기를 늘리십시오.

```cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** 필요한 경우 줄 안에서 정렬을 맞추기 위해 공백을 사용할 수 있습니다.

```cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** `.`, `->` 연산자 앞뒤에 공백을 사용하지 마십시오.

필요한 경우 연산자를 다음 줄로 내릴 수 있습니다. 이 경우 앞의 들여쓰기 수준이 증가합니다.

**11.** 단항 연산자(`--`, `++`, `*`, `&`, ...)와 인수 사이에 공백을 사용하지 마십시오.

**12.** 쉼표 뒤에는 공백을 넣되, 앞에는 넣지 마십시오. `for` 표현식 내부의 세미콜론에도 동일한 규칙이 적용됩니다.

**13.** `[]` 연산자 앞뒤에 공백을 사용하지 마십시오.

**14.** `template <...>` 표현식에서 `template`과 `<` 사이에는 공백을 넣고, `<` 뒤와 `>` 앞에는 공백을 넣지 마십시오.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** 클래스와 구조체에서 `public`, `private`, `protected`는 `class/struct`와 같은 수준으로 작성하고, 나머지 코드는 들여쓰기하십시오.

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

**16.** 파일 전체에 동일한 `namespace`가 사용되고 그 외 특별히 중요한 내용이 없다면, `namespace` 내부에 들여쓰기(offset)를 할 필요가 없습니다.

**17.** `if`, `for`, `while` 또는 다른 표현식의 블록이 단일 `statement`로 구성된 경우, 중괄호는 생략할 수 있습니다. 대신 `statement`를 별도의 줄에 작성하십시오. 이 규칙은 중첩된 `if`, `for`, `while`, ...에도 동일하게 적용됩니다.

단, 내부 `statement`에 중괄호나 `else`가 포함된 경우 외부 블록도 중괄호로 작성해야 합니다.

```cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** 줄 끝에 공백이 있으면 안 됩니다.

**19.** 소스 파일은 UTF-8로 인코딩되어 있습니다.

**20.** 문자열 리터럴(literal)에는 비ASCII 문자를 사용할 수 있습니다.

```cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** 한 줄에 여러 표현식을 작성하지 마십시오.

**22.** 함수 내부의 코드 구간은 묶고, 그 사이에는 빈 줄을 최대 1줄만 두십시오.

**23.** 함수, 클래스 등은 1개 또는 2개의 빈 줄로 구분하십시오.

**24.** 값과 관련된 `A const`는 타입 이름 앞에 작성해야 합니다.

```cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** 포인터 또는 참조를 선언할 때는 `*` 및 `&` 기호의 양옆을 공백으로 띄워야 합니다.

```cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** Template 타입을 사용할 때는 `using` 키워드를 사용해 별칭을 지정합니다(가장 단순한 경우는 제외).

즉, Template 매개변수는 `using`에서만 지정하고 코드에서는 반복하지 않습니다.

`using`은 함수 내부처럼 로컬 범위에서 선언할 수 있습니다.

```cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** 서로 다른 타입의 변수 여러 개를 하나의 문에서 선언하지 마십시오.

```cpp
//incorrect
int x, *y;
```

**28.** C 스타일 형변환을 사용하지 마십시오.

```cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** 클래스와 struct에서는 각 접근 범위 내에서 멤버와 함수를 따로 묶습니다.

**30.** 작은 클래스와 struct에서는 메서드 선언과 구현을 분리할 필요가 없습니다.

클래스나 struct의 작은 메서드에도 동일하게 적용됩니다.

Template 클래스와 struct에서는 메서드 선언과 구현을 분리하지 마십시오(그렇지 않으면 같은 번역 단위에서 정의해야 하기 때문입니다).

**31.** 줄바꿈은 80자 대신 140자에서 해도 됩니다.

**32.** 후위 증가/감소 연산자가 꼭 필요하지 않다면 항상 전위 증가/감소 연산자를 사용합니다.

```cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

<div id="comments">
  ## Comments
</div>

**1.** 자명하지 않은 모든 코드 부분에는 반드시 Comments을 추가하십시오.

이는 매우 중요합니다. Comments을 작성하는 과정에서 해당 코드가 वास्तव로 필요하지 않거나 설계가 잘못되었다는 점을 깨닫게 될 수도 있습니다.

```cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** Comments은 필요에 따라 얼마든지 자세하게 작성할 수 있습니다.

**3.** Comments은 설명할 코드 앞에 작성하십시오. 드물게는 같은 줄에서 코드 뒤에 Comments을 작성할 수도 있습니다.

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

**4.** Comments은 영어로만 작성하십시오.

**5.** 라이브러리를 작성하는 경우, 메인 헤더 파일에 라이브러리에 대한 자세한 설명 Comments을 포함하십시오.

**6.** 추가 정보를 제공하지 않는 Comments은 달지 마십시오. 특히 다음과 같은 빈 Comments은 남기지 마십시오:

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

이 예시는 http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/의 내용을 가져온 것입니다.

**7.** 각 파일의 시작 부분에 쓸모없는 Comments(author, creation date ..)을 작성하지 마십시오.

**8.** 한 줄 Comments은 슬래시 3개인 `///`로 시작하고, 여러 줄 Comments은 `/**`로 시작합니다. 이러한 Comments은 &quot;문서화&quot; Comments으로 간주됩니다.

참고: 이러한 Comments에서 문서를 생성하기 위해 Doxygen을 사용할 수 있습니다. 하지만 IDE에서 코드를 탐색하는 편이 더 편리하므로, 일반적으로는 Doxygen을 사용하지 않습니다.

**9.** 여러 줄 Comments의 시작과 끝에는 빈 줄이 있으면 안 됩니다(여러 줄 Comments을 닫는 줄은 제외).

**10.** 코드를 Comments 처리할 때는 &quot;문서화&quot; Comments이 아니라 일반 Comments을 사용하십시오.

**11.** 커밋하기 전에 Comments 처리된 코드 부분은 삭제하십시오.

**12.** Comments이나 코드에 비속어를 사용하지 마십시오.

**13.** 대문자를 사용하지 마십시오. 문장 부호를 과도하게 사용하지 마십시오.

```cpp
/// WHAT THE FAIL???
```

**14.** 구분 기호를 만들 때 Comments을 사용하지 마십시오.

```cpp
///******************************************************
```

**15.** Comments에서 토론을 벌이지 마십시오.

```cpp
/// Why did you do this stuff?
```

**16.** 블록 끝에 해당 내용이 무엇인지 설명하는 Comments을 달 필요는 없습니다.

```cpp
/// for
```

<div id="names">
  ## 이름
</div>

**1.** 변수와 클래스 멤버의 이름은 밑줄 문자(&#95;)를 사용한 소문자로 작성합니다.

```cpp
size_t max_block_size;
```

**2.** 함수(메서드) 이름은 소문자로 시작하는 camelCase를 사용하십시오.

```cpp
std::string getName() const override { return "Memory"; }
```

**3.** 클래스(struct) 이름은 첫 글자를 대문자로 하는 CamelCase를 사용합니다. 인터페이스에는 I를 제외한 접두사를 사용하지 않습니다.

```cpp
class StorageMemory : public IStorage
```

**4.** `using`의 이름은 클래스와 같은 방식으로 지정합니다.

**5.** Template 타입 인수의 이름: 단순한 경우에는 `T`, `T`, `U`, `T1`, `T2`를 사용합니다.

더 복잡한 경우에는 클래스 이름 규칙을 따르거나 접두사 `T`를 추가합니다.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Template 상수 인수의 이름은 변수 이름 규칙을 따르거나, 간단한 경우에는 `N`을 사용합니다.

```cpp
template <bool without_www>
struct ExtractDomain
```

**7.** 추상 클래스(인터페이스)에는 `I` 접두사를 붙일 수 있습니다.

```cpp
class IProcessor
```

**8.** 변수를 로컬 범위에서만 사용하는 경우에는 짧은 이름을 사용할 수 있습니다.

그 외의 모든 경우에는 의미가 드러나는 이름을 사용하십시오.

```cpp
bool info_successfully_loaded = false;
```

**9.** `define`와 전역 상수의 이름은 언더스코어가 포함된 ALL&#95;CAPS를 사용합니다.

```cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** 파일 이름은 파일 내용과 동일한 스타일을 따라야 합니다.

파일에 클래스가 하나만 있으면 파일 이름도 클래스 이름과 동일하게 지정합니다(CamelCase).

파일에 함수가 하나만 있으면 파일 이름도 함수 이름과 동일하게 지정합니다(camelCase).

**11.** 이름에 약어가 포함된 경우에는 다음 규칙을 따릅니다.

* 변수 이름에서는 약어를 소문자로 표기해야 합니다. `mysql_connection` (`mySQL_connection` 아님)
* 클래스 및 함수 이름에서는 약어의 대문자를 유지합니다. `MySQLConnection` (`MySqlConnection` 아님)

**12.** 클래스 멤버를 초기화하는 데만 사용하는 생성자 인수는 클래스 멤버와 같은 이름을 사용하되, 끝에 밑줄을 붙여야 합니다.

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

생성자 본문에서 인수를 사용하지 않는 경우 밑줄(&#95;) 접미사는 생략할 수 있습니다.

**13.** 로컬 변수와 클래스 멤버의 이름은 구분하지 않습니다(프리픽스가 필요하지 않습니다).

```cpp
timer (not m_timer)
```

**14.** `enum`의 상수는 첫 글자를 대문자로 하는 CamelCase를 사용합니다. ALL&#95;CAPS도 허용됩니다. `enum`이 지역 범위 밖에 선언된 경우 `enum class`를 사용합니다.

```cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** 모든 이름은 영어로 작성해야 합니다. 히브리어 단어를 음역하는 것은 허용되지 않습니다.

not T&#95;PAAMAYIM&#95;NEKUDOTAYIM

**16.** 약어는 널리 알려져 있다면 허용됩니다(약어의 의미를 Wikipedia나 검색 엔진에서 쉽게 찾을 수 있는 경우).

`AST`, `SQL`.

`NVDH`는 허용되지 않습니다(무작위 글자 몇 개)

축약형이 일반적으로 사용된다면 완전한 단어가 아니어도 허용됩니다.

주석에 전체 이름이 함께 포함되어 있다면 약어를 사용할 수도 있습니다.

**17.** C++ 소스 코드 파일 이름은 `.cpp` 확장자를 사용해야 합니다. 헤더 파일은 `.h` 확장자를 사용해야 합니다.

<div id="how-to-write-code">
  ## 코드 작성 방법
</div>

**1.** 메모리 관리.

수동 메모리 해제(`delete`)는 라이브러리 코드에서만 사용할 수 있습니다.

라이브러리 코드에서는 `delete` 연산자를 소멸자에서만 사용할 수 있습니다.

애플리케이션 코드에서는 해당 메모리를 소유한 객체가 메모리를 해제해야 합니다.

예시:

* 가장 쉬운 방법은 객체를 스택에 두거나 다른 클래스의 멤버로 두는 것입니다.
* 작은 객체가 많이 필요한 경우에는 컨테이너를 사용하십시오.
* 힙에 있는 소수의 객체를 자동으로 해제하려면 `shared_ptr/unique_ptr`를 사용하십시오.

**2.** 리소스 관리.

`RAII`를 사용하고, 위 내용을 참고하십시오.

**3.** 오류 처리.

예외를 사용하십시오. 대부분의 경우 예외를 발생시키기만 하면 되며, 이를 catch할 필요는 없습니다(`RAII` 때문입니다).

오프라인 데이터 처리 애플리케이션에서는 예외를 catch하지 않아도 되는 경우가 많습니다.

사용자 요청을 처리하는 서버에서는 일반적으로 connection handler의 최상위 수준에서 예외를 catch하는 것만으로도 충분합니다.

스레드 함수에서는 모든 예외를 catch해 보관한 뒤, `join` 후 메인 스레드에서 다시 발생시켜야 합니다.

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

처리하지 않은 예외를 절대 숨기지 마십시오. 예외를 무작정 모두 로그에만 남기지 마십시오.

```cpp
//Not correct
catch (...) {}
```

일부 예외를 무시해야 한다면 특정 예외만 무시하고, 나머지는 다시 throw하십시오.

```cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

응답 코드나 `errno`를 사용하는 함수를 사용할 때는 항상 결과를 확인하고, 오류가 발생하면 예외를 발생시키십시오.

```cpp
if (0 != close(fd))
    throw ErrnoException(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file {}", file_name);
```

코드에서 assert를 사용해 불변식을 확인할 수 있습니다.

**4.** 예외 유형.

애플리케이션 코드에서 복잡한 예외 계층 구조를 사용할 필요는 없습니다. 예외 메시지는 시스템 관리자가 이해할 수 있어야 합니다.

**5.** 소멸자에서 예외 발생시키기.

권장되지는 않지만 허용됩니다.

다음 옵션을 사용하십시오:

* 예외로 이어질 수 있는 작업을 미리 모두 수행하는 함수(`done()` 또는 `finalize()`)를 만드십시오. 해당 함수가 호출되었다면, 이후 소멸자에서는 예외가 발생하지 않아야 합니다.
* 지나치게 복잡한 작업(예: 네트워크를 통해 메시지 전송)은 클래스 사용자가 소멸 전에 호출해야 하는 별도의 메서드로 분리할 수 있습니다.
* 소멸자에서 예외가 발생하면, 이를 숨기기보다 기록하는 편이 낫습니다(로거를 사용할 수 있는 경우).
* 단순한 애플리케이션에서는 예외 처리를 위해 `std::terminate`(`C++11`에서 기본적으로 `noexcept`인 경우)에 의존하는 것도 괜찮습니다.

**6.** 익명 코드 블록.

특정 변수를 지역 변수로 제한하고 블록을 벗어날 때 소멸자가 호출되도록, 하나의 함수 안에 별도의 코드 블록을 만들 수 있습니다.

```cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** 멀티스레딩.

오프라인 데이터 처리 프로그램에서는 다음을 따르십시오.

* 단일 CPU 코어에서 가능한 한 최대 성능을 내도록 하십시오. 그러면 필요할 경우 코드를 병렬화할 수 있습니다.

서버 애플리케이션에서는 다음을 따르십시오.

* 요청 처리에는 스레드 풀을 사용하십시오. 지금까지는 사용자 공간 Context 전환이 필요한 작업은 없었습니다.

Fork는 병렬화에 사용하지 않습니다.

**8.** 스레드 동기화.

대부분의 경우 서로 다른 스레드가 서로 다른 메모리 셀(더 나아가 서로 다른 캐시 라인)을 사용하게 하고, 스레드 동기화를 전혀 사용하지 않는 것(`joinAll` 제외)이 가능합니다.

동기화가 필요하다면 대부분의 경우 `lock_guard` 아래에서 뮤텍스를 사용하는 것으로 충분합니다.

그 밖의 경우에는 시스템 동기화 기본 요소를 사용하십시오. busy wait는 사용하지 마십시오.

원자적 연산은 가장 단순한 경우에만 사용해야 합니다.

잠금 없는 데이터 구조를 구현하려고 하지 마십시오. 해당 분야가 주된 전문 분야인 경우는 예외입니다.

**9.** 포인터와 참조.

대부분의 경우 참조를 우선 사용하십시오.

**10.** `const`.

상수 참조, 상수를 가리키는 포인터, `const_iterator`, `const` 메서드를 사용하십시오.

`const`를 기본값으로 간주하고, 필요할 때만 non-`const`를 사용하십시오.

변수를 값으로 전달할 때는 보통 `const`를 사용하는 것이 의미가 없습니다.

**11.** unsigned.

필요한 경우 `unsigned`를 사용하십시오.

**12.** 숫자 타입.

`UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64` 타입과 `size_t`, `ssize_t`, `ptrdiff_t`를 사용하십시오.

숫자에는 다음 타입을 사용하지 마십시오: `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** 인수 전달.

이동할 예정인 복잡한 값은 값으로 전달하고 `std::move`를 사용하십시오. 루프에서 값을 갱신해야 한다면 참조로 전달하십시오.

함수가 힙에 생성된 객체의 소유권을 넘겨받는 경우, 인수 타입은 `shared_ptr` 또는 `unique_ptr`로 하십시오.

**14.** 반환 값.

대부분의 경우 `return`만 사용하십시오. `return std::move(res)`를 작성하지 마십시오.

함수가 힙에 객체를 할당해 반환하는 경우, `shared_ptr` 또는 `unique_ptr`를 사용하십시오.

드문 경우(루프에서 값을 갱신하는 경우)에는 인수를 통해 값을 반환해야 할 수 있습니다. 이 경우 해당 인수는 참조여야 합니다.

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

**15.** `namespace`.

애플리케이션 코드에 별도의 `namespace`를 사용할 필요는 없습니다.

작은 라이브러리도 마찬가지로 그럴 필요가 없습니다.

중대형 라이브러리라면 모든 것을 `namespace` 안에 넣으십시오.

라이브러리의 `.h` 파일에서는 애플리케이션 코드에 필요하지 않은 구현 세부 사항을 숨기기 위해 `namespace detail`을 사용할 수 있습니다.

`.cpp` 파일에서는 `static` 또는 익명 `namespace`를 사용해 심볼을 숨길 수 있습니다.

또한 `enum`에 `namespace`를 사용하면 해당 이름이 외부 `namespace`로 새어 나가는 것을 막을 수 있습니다(하지만 `enum class`를 사용하는 편이 더 낫습니다).

**16.** 지연 초기화.

초기화에 인수가 필요하다면 일반적으로 기본 생성자를 작성하지 않아야 합니다.

나중에 초기화를 미뤄야 한다면 유효하지 않은 객체를 만드는 기본 생성자를 추가할 수 있습니다. 또는 객체 수가 적다면 `shared_ptr/unique_ptr`를 사용할 수 있습니다.

```cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** 가상 함수.

클래스를 다형적으로 사용할 의도가 없다면 함수를 가상 함수로 만들 필요가 없습니다. 이는 소멸자에도 적용됩니다.

**18.** 인코딩.

어디서나 UTF-8을 사용하십시오. `std::string` 및 `char *`를 사용하십시오. `std::wstring` 및 `wchar_t`는 사용하지 마십시오.

**19.** 로깅.

코드 곳곳의 예시를 참조하십시오.

커밋하기 전에 의미 없는 로깅, 디버그 로깅, 그 밖의 모든 디버그 출력을 삭제하십시오.

성능에 중요한 루프에서는 Trace 수준이라도 로깅을 피해야 합니다.

어떤 로깅 수준에서도 로그는 읽기 쉬워야 합니다.

로깅은 대부분 애플리케이션 코드에서만 사용해야 합니다.

로그 메시지는 영어로 작성해야 합니다.

가능하면 로그는 시스템 관리자도 이해할 수 있어야 합니다.

로그에 비속어를 사용하지 마십시오.

로그에는 UTF-8 인코딩을 사용하십시오. 드문 경우에는 로그에 비ASCII 문자를 사용할 수 있습니다.

**20.** 입출력.

애플리케이션 성능에 중요한 내부 루프에서는 `iostreams`를 사용하지 마십시오(`stringstream`은 절대 사용하지 마십시오).

대신 `DB/IO` 라이브러리를 사용하십시오.

**21.** 날짜 및 시간.

`DateLUT` 라이브러리를 참조하십시오.

**22.** include.

include 가드 대신 항상 `#pragma once`를 사용하십시오.

**23.** using.

`using namespace`는 사용하지 않습니다. 특정 대상에 한해서는 `using`을 사용할 수 있습니다. 다만 클래스나 함수 내부의 지역 범위에서만 사용하십시오.

**24.** 필요하지 않다면 함수에 `trailing return type`을 사용하지 마십시오.

```cpp
auto f() -> void
```

**25.** 변수 선언 및 초기화.

```cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** 가상 함수는 기반 클래스에 `virtual`을 쓰고, 파생 클래스에는 `virtual` 대신 `override`를 씁니다.

<div id="unused-features-of-c">
  ## C++에서 사용하지 않는 기능
</div>

**1.** 가상 상속은 사용하지 않습니다.

**2.** 최신 C++에서 간편한 문법 설탕을 제공하는 구문들(예:

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
  ## 플랫폼
</div>

**1.** 특정 플랫폼용으로 코드를 작성합니다.

다만 다른 조건이 같다면 크로스플랫폼 또는 이식 가능한 코드를 선호합니다.

**2.** 언어: C++20(사용 가능한 [C++20 기능](https://en.cppreference.com/w/cpp/compiler_support#C.2B.2B20_features) 목록 참조).

**3.** 컴파일러: `clang`. 이 문서를 작성하는 시점(2025년 3월) 기준으로 코드는 clang 버전 19 이상으로 컴파일합니다.

표준 라이브러리(`libc++`)를 사용합니다.

**4.** OS: Linux Ubuntu, Precise보다 이전 버전은 지원하지 않습니다.

**5.** 코드는 x86&#95;64 CPU 아키텍처용으로 작성됩니다.

CPU 명령어 집합은 서버에서 지원되는 최소 집합을 기준으로 합니다. 현재는 SSE 4.2입니다.

**6.** 몇 가지 예외를 제외하고 `-Wall -Wextra -Werror -Weverything` 컴파일 플래그를 사용합니다.

**7.** 정적으로 연결하기 어려운 라이브러리를 제외한 모든 라이브러리에 정적 링크를 사용합니다(`ldd` 명령의 출력 참조).

**8.** 코드는 릴리스 설정으로 개발하고 디버그합니다.

<div id="tools">
  ## 도구
</div>

**1.** KDevelop은 좋은 IDE입니다.

**2.** 디버깅에는 `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...`, 또는 `tcmalloc_minimal_debug`를 사용합니다.

**3.** 프로파일링에는 `Linux Perf`, `valgrind` (`callgrind`), 또는 `strace -cf`를 사용합니다.

**4.** 소스 코드는 Git으로 관리됩니다.

**5.** 빌드에는 `CMake`를 사용합니다.

**6.** 프로그램은 `deb` 패키지로 배포됩니다.

**7.** master에 대한 커밋으로 빌드가 깨져서는 안 됩니다.

다만 일부 revision만 정상적으로 동작하는 것으로 간주됩니다.

**8.** 코드가 아직 일부만 준비된 상태이더라도 가능한 한 자주 커밋하십시오.

이때는 브랜치를 사용하십시오.

`master` 브랜치의 코드가 아직 빌드되지 않는다면 `push` 전에 빌드 대상에서 제외하십시오. 며칠 안에 이를 마무리하거나 제거해야 합니다.

**9.** 간단하지 않은 변경의 경우 브랜치를 사용하고 서버에 공개하십시오.

**10.** 사용되지 않는 코드는 리포지토리에서 제거됩니다.

<div id="libraries">
  ## 라이브러리
</div>

**1.** C++20 표준 라이브러리를 사용하며(실험적 확장은 허용됨), `boost` 및 `Poco` 프레임워크도 사용합니다.

**2.** OS 패키지의 라이브러리는 사용할 수 없습니다. 사전 설치된 라이브러리도 사용할 수 없습니다. 모든 라이브러리는 `contrib` 디렉터리에 소스 코드 형태로 포함되어야 하며 ClickHouse와 함께 빌드해야 합니다. 자세한 내용은 [새 타사 라이브러리 추가 가이드라인](/ko/development/contrib#adding-and-maintaining-third-party-libraries)을 참조하십시오.

**3.** 항상 이미 사용 중인 라이브러리를 우선적으로 선택합니다.

<div id="general-recommendations">
  ## 일반 권장 사항
</div>

**1.** 코드는 가능한 한 적게 작성하십시오.

**2.** 가장 단순한 해결 방법을 먼저 시도하십시오.

**3.** 코드가 어떻게 동작할지, 그리고 내부 루프가 어떻게 작동할지 알기 전에는 코드를 작성하지 마십시오.

**4.** 가장 단순한 경우에는 클래스나 구조체 대신 `using`을 사용하십시오.

**5.** 가능하다면 복사 생성자, 대입 연산자, 소멸자(클래스에 `virtual` 함수가 하나 이상 있는 경우의 `virtual` 소멸자는 제외), 이동 생성자, 이동 대입 연산자를 직접 작성하지 마십시오. 즉, 컴파일러가 생성한 함수가 올바르게 동작해야 합니다. `default`를 사용할 수 있습니다.

**6.** 코드를 단순하게 유지하는 것이 좋습니다. 가능하면 코드 크기를 줄이십시오.

<div id="additional-recommendations">
  ## 추가 권장 사항
</div>

**1.** `stddef.h`의 타입에 `std::`를 명시적으로 붙이는 것

은 권장하지 않습니다. 즉, 더 짧으므로 `std::size_t` 대신 `size_t`를 쓰는 것을 권장합니다.

`std::`를 추가해도 괜찮습니다.

**2.** 표준 C 라이브러리 함수에 `std::`를 명시적으로 붙이는 것

은 권장하지 않습니다. 즉, `std::memcpy` 대신 `memcpy`를 쓰십시오.

이유는 `memmem`처럼 비표준이지만 비슷한 함수가 있기 때문입니다. 이러한 함수도 경우에 따라 사용합니다. 이런 함수는 `namespace std`에 없습니다.

어디에서나 `memcpy` 대신 `std::memcpy`를 쓰면, `std::`가 없는 `memmem`이 어색해 보일 수 있습니다.

그래도 원한다면 `std::`를 계속 사용할 수 있습니다.

**3.** 표준 C++ 라이브러리에 같은 함수가 있는데도 C 함수를 사용하는 것.

더 효율적이라면 허용됩니다.

예를 들어, 큰 메모리 청크를 복사할 때는 `std::copy` 대신 `memcpy`를 사용하십시오.

**4.** 여러 줄 함수 인수.

다음 줄바꿈 스타일은 모두 허용됩니다.

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