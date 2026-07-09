---
description: 'ClickHouse の C++ 開発におけるコーディングスタイルガイドライン'
sidebar_label: 'C++ スタイルガイド'
sidebar_position: 70
slug: /development/style
title: 'C++ スタイルガイド'
doc_type: 'guide'
---

<div id="general-recommendations">
  ## 一般的な推奨事項
</div>

以下は必須要件ではなく、推奨事項です。
コードを編集する場合は、既存のコードのフォーマットに合わせるのが適切です。
一貫性を保つには、コードスタイルが必要です。一貫性があるとコードを読みやすくなり、検索もしやすくなります。
規則の多くに論理的な理由はなく、確立された慣行に基づいています。

<div id="formatting">
  ## フォーマット
</div>

**1.** フォーマットのほとんどは、`clang-format` によって自動的に処理されます。

**2.** インデントは4スペースです。タブキーを押すと4スペース分が挿入されるよう、開発環境を設定してください。

**3.** 開き波括弧と閉じ波括弧は、それぞれ独立した行に記述してください。

```cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** 関数のボディ全体が1つの`ステートメント`である場合、1行に記述できます。波括弧の周囲にスペースを置いてください (行末のスペースを除く) 。

```cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** 関数について。括弧の前後にスペースを入れないこと。

```cpp
void reinsert(const Value & x)
```

```cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** `if`、`for`、`while` などの式では、開き括弧の前にスペースを入れます (関数呼び出しとは異なります) 。

```cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** 二項演算子 (`+`、`-`、`*`、`/`、`%` など) および三項演算子 `?:` の前後にスペースを入れてください。

```cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** 改行文字が入力された場合は、演算子を新しい行に配置し、その前のインデントを深めます。

```cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** 必要に応じて、行内の整列にスペースを使用できます。

```cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** 演算子 `.`、`->` の前後にスペースを使用しないでください。

必要に応じて、operatorを次の行に折り返すことができます。この場合、その前のインデントが増えます。

**11.** 単項演算子 (`--`、`++`、`*`、`&` など) と引数の間にスペースを入れないでください。

**12.** カンマの後にはスペースを入れ、前には入れないこと。同じルールが `for` 式内のセミコロンにも適用される。

**13.** `[]` 演算子の前後にスペースを入れないでください。

**14.** `template <...>` 式では、`template` と `<` の間にスペースを入れること。`<` の後や `>` の前にはスペースを入れない。

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** クラスや構造体では、`public`、`private`、`protected` を `class/struct` と同じレベルに記述し、それ以外のコードはインデントしてください。

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

**16.** ファイル全体で同じ `namespace` が使用されており、他に特筆すべき内容がない場合、`namespace` 内でインデントは不要です。

**17.** `if`、`for`、`while`、またはその他の式のブロックが単一の`ステートメント`で構成される場合、波括弧は省略できます。その場合は、`ステートメント`を別の行に記述してください。このルールは、ネストされた`if`、`for`、`while`、... にも適用されます。

ただし、内側の`ステートメント`に波括弧または`else`が含まれる場合、外側のブロックは波括弧で記述する必要があります。

```cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** 行末にスペースを入れないでください。

**19.** ソースファイルは UTF-8 でエンコードされています。

**20.** 非ASCII文字は文字列リテラル内で使用できます。

```cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** 1 行に複数の式を書かないでください。

**22.** 関数内のコードのまとまりごとに区切り、空行は 1 行までにしてください。

**23.** 関数、クラスなどの間は、1 行または 2 行の空行で区切ってください。

**24.** `A const` (値に関連するもの) は、型名の前に記述する必要があります。

```cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** ポインタまたは参照を宣言する際は、`*` と `&` の前後にスペースを入れる必要があります。

```cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** Template 型を使用する場合は、`using` キーワードで別名を付けます (ごく単純な場合を除く) 。

つまり、テンプレートパラメータは `using` でのみ指定し、コード内で繰り返し記述しません。

`using` は、関数内などでローカルに宣言できます。

```cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** 異なる型の複数の変数を1つのステートメントで宣言しないでください。

```cpp
//incorrect
int x, *y;
```

**28.** Cスタイルのキャストは使用しないでください。

```cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** クラスや構造体では、各アクセス指定子のスコープ内で、メンバーと関数をそれぞれ分けてまとめてください。

**30.** 小さなクラスや構造体では、メソッドの宣言と実装を分ける必要はありません。

これは、どのクラスや構造体でも、小さなメソッドについて同様です。

Template クラスや構造体では、メソッドの宣言と実装を分けないでください (そうしないと、それらを同じ翻訳単位内で定義しなければならないためです) 。

**31.** 行は、80文字ではなく140文字で折り返してかまいません。

**32.** 後置が必要でない場合は、常に前置のインクリメント/デクリメント演算子を使用してください。

```cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

<div id="comments">
  ## コメント
</div>

**1.** 自明でないコードには、必ずコメントを付けてください。

これは非常に重要です。実際にコメントを書いてみると、そのコードは不要だと気づいたり、設計に問題があるとわかったりすることがあります。

```cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** コメントは必要に応じて、いくらでも詳しくできます。

**3.** コメントは、説明対象のコードの前に置きます。まれに、同じ行でコードの後ろにコメントを付けることもできます。

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

**4.** コメントは英語でのみ記述してください。

**5.** ライブラリを作成する場合は、メインのヘッダーファイルに、その内容を説明する詳細なコメントを記載してください。

**6.** 補足情報を提供しないコメントは追加しないでください。特に、次のような中身のないコメントを残さないでください。

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

この例は、http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/ の内容を参考にしています。

**7.** 各ファイルの先頭に、不要なコメント (著者、作成日など) を書かないでください。

**8.** 1 行コメントはスラッシュ 3 つ `///` で始め、複数行コメントは `/**` で始めます。これらのコメントは &quot;ドキュメンテーション&quot; と見なされます。

注: これらのコメントから Doxygen を使ってドキュメントを生成できます。ただし、IDE 上でコードをたどるほうが便利なため、Doxygen は一般にはあまり使われません。

**9.** 複数行コメントの先頭と末尾に空行を入れてはいけません (複数行コメントを閉じる行は除く) 。

**10.** コードをコメントアウトする場合は、&quot;ドキュメンテーション&quot; コメントではなく、通常のコメントを使ってください。

**11.** コミットする前に、コメントアウトしたコードは削除してください。

**12.** コメントやコードで暴言を使わないでください。

**13.** 大文字は使わないでください。句読点を過剰に使わないでください。

```cpp
/// WHAT THE FAIL???
```

**14.** 区切り文字としてコメントを使用しないでください。

```cpp
///******************************************************
```

**15.** コメントで議論を始めないでください。

```cpp
/// Why did you do this stuff?
```

**16.** ブロックの内容を説明するコメントを末尾に書く必要はありません。

```cpp
/// for
```

<div id="names">
  ## 名前
</div>

**1.** 変数名およびクラスメンバー名には、小文字とアンダースコアを使用してください。

```cpp
size_t max_block_size;
```

**2.** 関数 (メソッド) の名前は、先頭を小文字にしたcamelCaseを使用します。

```cpp
std::string getName() const override { return "Memory"; }
```

**3.** クラス (構造体) の名前には、先頭を大文字にしたCamelCaseを使用します。インターフェイスには、I 以外のプレフィックスは使用しません。

```cpp
class StorageMemory : public IStorage
```

**4.** `using` の命名は、クラスと同じルールに従います。

**5.** Template型引数の名前: 単純な場合は、`T`、`T`, `U`、`T1`, `T2` を使用します。

より複雑な場合は、クラス名のルールに従うか、プレフィックス `T` を付けます。

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Template定数引数の名前は、変数名の規則に従うか、単純な場合は `N` を使用します。

```cpp
template <bool without_www>
struct ExtractDomain
```

**7.** 抽象クラス (インターフェイス) には、先頭に `I` プレフィックスを付けることができます。

```cpp
class IProcessor
```

**8.** ローカルで使用する変数には、短い名前を使えます。

それ以外の場合は、意味がわかる名前を使用してください。

```cpp
bool info_successfully_loaded = false;
```

**9.** `define` の名前とグローバル定数名には、アンダースコア区切りの ALL&#95;CAPS を使用します。

```cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** ファイル名は、その内容と同じ命名スタイルにする必要があります。

ファイルにクラスが 1 つだけ含まれている場合は、ファイル名もクラス名と同じ形式 (CamelCase) にします。

ファイルに関数が 1 つだけ含まれている場合は、ファイル名も関数名と同じ形式 (camelCase) にします。

**11.** 名前に略語が含まれる場合は、次のルールに従います。

* 変数名では、略語は小文字を使用します `mysql_connection` (`mySQL_connection` ではなく) 。
* クラス名と関数名では、略語の大文字表記を維持します `MySQLConnection` (`MySqlConnection` ではなく) 。

**12.** クラスメンバーの初期化にのみ使用するコンストラクター引数は、クラスメンバーと同じ名前にし、末尾にアンダースコアを付けます。

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

引数をコンストラクタ内で使用しない場合は、末尾のアンダースコアを省略できます。

**13.** ローカル変数名とクラスメンバー名は区別しません (プレフィックスは不要です) 。

```cpp
timer (not m_timer)
```

**14.** `enum` 内の定数には、先頭が大文字の CamelCase を使用します。ALL&#95;CAPS も使用できます。`enum` がローカルでない場合は、`enum class` を使用します。

```cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** すべての名前は英語でなければなりません。ヘブライ語の単語を翻字して使うことはできません。

T&#95;PAAMAYIM&#95;NEKUDOTAYIM は不可

**16.** 略語は、十分によく知られている場合に限り使用できます (Wikipedia や検索エンジンでその意味を簡単に確認できる場合) 。

`AST`、`SQL`。

`NVDH` は不可 (適当な文字の羅列)

短縮形が一般的に使われている場合は、省略された語形も使用できます。

コメント内に正式名称が併記されている場合も、略語を使用できます。

**17.** C++ のソースコードのファイル名には `.cpp` 拡張子を付けなければなりません。ヘッダーファイルには `.h` 拡張子を付けなければなりません。

<div id="how-to-write-code">
  ## コードの書き方
</div>

**1.** メモリ管理。

手動でのメモリ解放 (`delete`) は、ライブラリコードでのみ使用できます。

ライブラリコードでは、`delete` 演算子はデストラクタ内でのみ使用できます。

アプリケーションコードでは、メモリはそれを所有するオブジェクトが解放しなければなりません。

例:

* 最も簡単な方法は、オブジェクトをスタック上に配置するか、別のクラスのメンバーにすることです。
* 小さなオブジェクトが大量にある場合は、コンテナを使用します。
* ヒープ上にある少数のオブジェクトを自動的に解放するには、`shared_ptr/unique_ptr` を使用します。

**2.** リソース管理。

`RAII` を使用し、上記に従ってください。

**3.** エラー処理。

例外を使用します。ほとんどの場合、必要なのは例外を送出することだけで、キャッチする必要はありません (`RAII` のためです) 。

オフラインのデータ処理アプリケーションでは、例外をキャッチしないことが許容される場合がよくあります。

ユーザーのリクエストを処理するサーバーでは、通常、接続ハンドラーの最上位レベルで例外をキャッチすれば十分です。

スレッド関数では、すべての例外をキャッチして保持し、`join` の後でメインスレッドから再送出するようにしてください。

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

例外は、処理せずに握りつぶしてはいけません。すべての例外を何も考えずにログへ出力するだけにしてもいけません。

```cpp
//Not correct
catch (...) {}
```

一部の例外を無視する必要がある場合は、特定の例外に対してのみそうし、それ以外は再スローしてください。

```cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

戻り値コードまたは `errno` を伴う関数を使用する場合は、必ず結果を確認し、エラーが発生した場合は例外を送出してください。

```cpp
if (0 != close(fd))
    throw ErrnoException(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file {}", file_name);
```

コード内の不変条件のチェックには assert を使用できます。

**4.** 例外の種類。

アプリケーションコードで複雑な例外階層を使う必要はありません。例外メッセージは、システム管理者が理解できるものであるべきです。

**5.** デストラクタからの例外送出。

これは推奨されませんが、禁止はされていません。

次の選択肢があります。

* 例外につながる可能性のある処理を事前にすべて済ませる関数 (`done()` または `finalize()`) を作成します。その関数が呼び出されていれば、後でデストラクタで例外が発生してはなりません。
* 複雑すぎるタスク (ネットワーク経由でのメッセージ送信など) は、オブジェクトの破棄前にクラスの利用者が呼び出す別個のメソッドに分けることができます。
* デストラクタで例外が発生した場合、隠すよりもログに記録するほうが適切です (ロガーが利用可能であれば) 。
* 単純なアプリケーションでは、例外処理を `std::terminate` (C++11 では `noexcept` がデフォルトであるため) に任せてもかまいません。

**6.** 無名コードブロック。

特定の変数をローカルにし、ブロックを抜けるときにデストラクタが呼び出されるようにするため、1 つの関数内に別のコードブロックを作成できます。

```cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** マルチスレッド。

オフラインデータ処理プログラムでは:

* まず単一の CPU コアで可能な限り高い性能を引き出すようにしてください。そのうえで、必要に応じてコードを並列化します。

サーバーアプリケーションでは:

* リクエストの処理にはスレッドプールを使用してください。現時点では、ユーザー空間のコンテキストスイッチを必要とするタスクはありませんでした。

並列化に fork は使用しません。

**8.** スレッドの同期。

多くの場合、異なるスレッドが異なるメモリセル (さらに望ましいのは異なる cache line) を使うようにし、スレッド同期をまったく使わないことが可能です (`joinAll` を除く) 。

同期が必要な場合でも、ほとんどのケースでは `lock_guard` と mutex を使えば十分です。

それ以外の場合は、システムの同期プリミティブを使用してください。ビジーウェイトは使用しないでください。

atomic 操作は、最も単純なケースでのみ使用してください。

それが自身の主たる専門分野でない限り、ロックフリーなデータ構造を実装しようとしないでください。

**9.** ポインタと参照。

ほとんどの場合、参照を優先してください。

**10.** `const`。

定数参照、定数へのポインタ、`const_iterator`、および `const` メソッドを使用してください。

`const` をデフォルトと考え、必要な場合にのみ非 `const` を使用してください。

変数を値渡しする場合、通常は `const` を付けても意味はありません。

**11.** unsigned。

必要な場合に `unsigned` を使用してください。

**12.** 数値型。

`UInt8`、`UInt16`、`UInt32`、`UInt64`、`Int8`、`Int16`、`Int32`、`Int64`、および `size_t`、`ssize_t`、`ptrdiff_t` 型を使用します。

数値には次の型を使用しないでください: `signed/unsigned long`、`long long`、`short`、`signed/unsigned char`、`char`。

**13.** 引数の受け渡し。

複雑な値は、ムーブするのであれば値渡しにし、std::move を使用します。ループ内で値を更新したい場合は、参照渡しにします。

関数がヒープ上に作成されたオブジェクトの所有権を受け取る場合、引数の型は `shared_ptr` または `unique_ptr` にします。

**14.** 戻り値。

ほとんどの場合、単に `return` を使います。`return std::move(res)` と書いてはいけません。

関数がヒープ上にオブジェクトを確保してそれを返す場合は、`shared_ptr` または `unique_ptr` を使用します。

まれに (ループ内で値を更新する場合など) 、引数経由で値を返す必要があることがあります。この場合、その引数は参照であるべきです。

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

アプリケーションコードのために、別の `namespace` を使う必要はありません。

小規模なライブラリでも同様に不要です。

中規模から大規模のライブラリでは、すべてを `namespace` に入れてください。

ライブラリの `.h` ファイルでは、アプリケーションコードに不要な実装詳細を隠すために `namespace detail` を使えます。

`.cpp` ファイルでは、`static` または無名 `namespace` を使ってシンボルを隠せます。

また、対応する名前が外側の `namespace` に出てしまうのを防ぐために、`enum` に対して `namespace` を使うこともできます (ただし、`enum class` を使うほうが望ましいです) 。

**16.** 遅延初期化。

初期化に引数が必要なら、通常はデフォルトコンストラクタを書くべきではありません。

後で初期化を遅らせる必要が生じるなら、無効なオブジェクトを作るデフォルトコンストラクタを追加できます。あるいは、オブジェクト数が少ないなら、`shared_ptr/unique_ptr` を使うこともできます。

```cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** virtual関数。

クラスが多態的な用途を想定していない場合、関数をvirtualにする必要はありません。これはデストラクタにも当てはまります。

**18.** エンコーディング。

どこでもUTF-8を使用してください。`std::string` と `char *` を使用してください。`std::wstring` と `wchar_t` は使用しないでください。

**19.** ロギング。

code全体にある例を参照してください。

commitする前に、無意味なログ、Debug用のログ、およびその他あらゆるDebug出力を削除してください。

cycle内でのログは、Traceレベルであっても避けるべきです。

ログはどのログレベルでも読みやすくなければなりません。

ログは、基本的にはapplication codeでのみ使用すべきです。

ログメッセージは英語で記述しなければなりません。

ログは、できればシステム管理者にも理解できる内容にしてください。

ログで下品な表現を使わないでください。

ログではUTF-8エンコーディングを使用してください。まれに、ログで非ASCII文字を使用してもかまいません。

**20.** 入出力。

アプリケーションの性能上重要な内部cycleでは `iostreams` を使用しないでください (`stringstream` は決して使用しないでください) 。

代わりに `DB/IO` ライブラリを使用してください。

**21.** 日付と時刻。

`DateLUT` ライブラリを参照してください。

**22.** include。

include guardの代わりに、常に `#pragma once` を使用してください。

**23.** using。

`using namespace` は使用しません。特定のものに対して `using` を使うことはできます。ただし、classまたは関数の中だけでローカルに使用してください。

**24.** 必要な場合を除き、関数で `trailing return type` を使用しないでください。

```cpp
auto f() -> void
```

**25.** 変数の宣言と初期化。

```cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** 仮想関数では、基底クラスには `virtual` を記述し、派生クラスでは `virtual` ではなく `override` を記述してください.‬

<div id="unused-features-of-c">
  ## C++で使用していない機能
</div>

**1.** 仮想継承は使用していません。

**2.** モダンC++で便利なシンタックスシュガーが用意されている構文。例:

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
  ## プラットフォーム
</div>

**1.** 特定のプラットフォーム向けにコードを書きます。

ただし、他の条件が同じであれば、クロスプラットフォームでポータブルなコードが望まれます。

**2.** 言語: C++20 (利用可能な [C++20 features](https://en.cppreference.com/w/cpp/compiler_support#C.2B.2B20_features) の一覧を参照してください) 。

**3.** コンパイラ: `clang`。本稿執筆時点 (2025年3月) では、コードは clang バージョン 19 以上でコンパイルされています。

標準ライブラリ (`libc++`) を使用します。

**4.** OS: Linux Ubuntu、Precise 以降。

**5.** コードは x86&#95;64 CPU アーキテクチャ向けに書かれています。

CPU 命令セットは、当社のサーバー群でサポートされている最小のセットです。現在は SSE 4.2 です。

**6.** いくつかの例外を除き、`-Wall -Wextra -Werror -Weverything` コンパイルフラグを使用します。

**7.** 静的リンクが難しいライブラリを除き、すべてのライブラリで静的リンクを使用します (`ldd` コマンドの出力を参照してください) 。

**8.** コードはリリース設定で開発およびデバッグされます。

<div id="tools">
  ## ツール
</div>

**1.** KDevelop は優れた IDE です。

**2.** デバッグには、`gdb`、`valgrind` (`memcheck`) 、`strace`、`-fsanitize=...`、または `tcmalloc_minimal_debug` を使用します。

**3.** プロファイリングには、`Linux Perf`、`valgrind` (`callgrind`) 、または `strace -cf` を使用します。

**4.** ソースコードは Git で管理されています。

**5.** ビルドには `CMake` を使用します。

**6.** プログラムは `deb` パッケージで配布されます。

**7.** master へのコミットでビルドを壊してはいけません。

ただし、正常に動作すると見なされるのは一部のリビジョンだけです。

**8.** コードがまだ一部しかできていなくても、できるだけ頻繁にコミットしてください。

そのためにブランチを使用してください。

`master` ブランチ内のコードがまだビルドできない場合は、`push` の前にビルド対象から外してください。数日以内に完成させるか削除する必要があります。

**9.** ある程度大きな変更には、ブランチを使用し、サーバー上に公開してください。

**10.** 使われていないコードはリポジトリから削除されます。

<div id="libraries">
  ## ライブラリ
</div>

**1.** C++20標準ライブラリを使用します (実験的な拡張は使用可) 。また、`boost` および `Poco` フレームワークも使用します。

**2.** OSパッケージのライブラリは使用できません。プリインストールされているライブラリも使用できません。すべてのライブラリはソースコードの形で `contrib` ディレクトリに配置し、ClickHouse とともにビルドする必要があります。詳細については、[新しいサードパーティライブラリの追加に関するガイドライン](/ja/development/contrib#adding-and-maintaining-third-party-libraries)を参照してください。

**3.** すでに使用されているライブラリを常に優先します。

<div id="general-recommendations">
  ## 一般的な推奨事項
</div>

**1.** コードはできるだけ少なくしてください。

**2.** まずは最も単純な解決策を試してください。

**3.** どう動作するのか、そして内部ループがどう機能するのかを理解するまでは、コードを書かないでください。

**4.** 最も単純なケースでは、クラスや構造体ではなく `using` を使ってください。

**5.** 可能であれば、コピーコンストラクタ、代入演算子、デストラクタ (クラスに `virtual` 関数が少なくとも 1 つある場合の virtual なものを除く) 、ムーブコンストラクタ、ムーブ代入演算子は書かないでください。言い換えれば、コンパイラが生成する関数が正しく動作するようにすべきです。`default` を使用できます。

**6.** コードの簡素化を心がけてください。可能な限りコード量を減らしてください。

<div id="additional-recommendations">
  ## Additional recommendations
</div>

**1.** `stddef.h` の型に対して `std::` を明示的に付けること

は推奨されません。つまり、`std::size_t` ではなく `size_t` と書くことを推奨します。短いからです。

`std::` を付けること自体は許容されます。

**2.** 標準 C ライブラリの関数に対して `std::` を明示的に付けること

は推奨されません。つまり、`std::memcpy` ではなく `memcpy` と書いてください。

その理由は、`memmem` のような類似の非標準関数があるためです。こうした関数も実際に使うことがあります。これらの関数は `namespace std` には存在しません。

どこでも `memcpy` ではなく `std::memcpy` と書いていると、`std::` の付いていない `memmem` が不自然に見えてしまいます。

それでも、好みであれば `std::` を使ってかまいません。

**3.** 同じものが標準 C++ ライブラリでも利用できる場合に C の関数を使うこと。

より効率的であれば、これは許容されます。

たとえば、大きなメモリ領域をコピーする場合は、`std::copy` ではなく `memcpy` を使ってください。

**4.** 複数行の関数引数。

以下の折り返しスタイルはどれも使用できます。

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