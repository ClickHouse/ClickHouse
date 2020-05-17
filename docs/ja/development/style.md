---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 68
toc_title: "C++\u30B3\u30FC\u30C9\u306E\u66F8\u304D\u65B9"
---

# C++コードの書き方 {#how-to-write-c-code}

## 一般的な推奨事項 {#general-recommendations}

**1.** 以下は推奨事項であり、要件ではありません。

**2.** コードを編集する場合は、既存のコードの書式に従うことが理にかなっています。

**3.** 一貫性のためにコードスタイルが必要です。 一貫性により、コードを読みやすくなり、コードの検索も簡単になります。

**4.** ルールの多くは論理的な理由を持っていない;彼らは確立された慣行によって決定されます。

## 書式設定 {#formatting}

**1.** 多くのフォーマットは自動的に実行されるのでよ `clang-format`.

**2.** インデントは4スペースです。 タブにスペースが追加されるように開発環境を構成します。

**3.** 中括弧の開始と終了は別の行にする必要があります。

``` cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** 関数本体全体が単一の場合 `statement`、それは単一行に置くことができます。 中括弧の周りにスペースを配置します（行末のスペースのほかに）。

``` cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** 機能のため。 をかけないスットに固定して使用します。

``` cpp
void reinsert(const Value & x)
```

``` cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** で `if`, `for`, `while` 他の式では、（関数呼び出しとは対照的に）開始括弧の前にスペースが挿入されます。

``` cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** 二項演算子の周りにスペースを追加 (`+`, `-`, `*`, `/`, `%`, …) and the ternary operator `?:`.

``` cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** 改行が入力されている場合は、演算子を新しい行に置き、その前にインデントを増やします。

``` cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** 必要に応じて、行内の整列にスペースを使用できます。

``` cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** 演算子の周りにスペースを使用しない `.`, `->`.

必要に応じて、演算子を次の行に折り返すことができます。 この場合、その前のオフセットが増加する。

**11.** 単項演算子の区切りにスペースを使用しないでください (`--`, `++`, `*`, `&`, …) from the argument.

**12.** 入れの後に空白、コンマといえるものです。 同じルールは、内部のセミコロンのために行く `for` 式。

**13.** 区切りにスペースを使用しないで下さい `[]` オペレーター

**14.** で `template <...>` 式は、間にスペースを使用します `template` と `<`;後にスペースなし `<` または前に `>`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** クラスと構造体では、 `public`, `private`,and `protected` と同じレベルで `class/struct` コードの残りの部分をインデントします。

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

**16.** 同じ場合 `namespace` ファイル全体に使用され、他に重要なものはありません。 `namespace`.

**17.** のブロックが `if`, `for`, `while`、または他の式は、単一の `statement`、中括弧は省略可能です。 を置く `statement` 別の行で、代わりに。 この規則は、入れ子に対しても有効です `if`, `for`, `while`, …

しかし、内側の場合 `statement` 中括弧または `else`、外部ブロックは中括弧で記述する必要があります。

``` cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** 行の終わりにスペースがあってはなりません。

**19.** ソースファイルはUTF-8エンコードです。

**20.** ASCII以外の文字は、文字列リテラルで使用できます。

``` cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** 単一行に複数の式を記述しないでください。

**22.** 関数内のコードのセクションをグループ化し、空行を複数以下で区切ります。

**23.** 関数、クラスなどを空行で区切ります。

**24.** `A const` (値に関連する)型名の前に記述する必要があります。

``` cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** ポインタまたは参照を宣言するとき、 `*` と `&` シンボルは、両側のスペースで区切る必要があります。

``` cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** テンプレートタイプを使用するときは、それらを `using` キーワード(最も単純な場合を除く)。

つまり、テンプレートパラメーターは、 `using` コードでは繰り返されません。

`using` 関数内など、ローカルで宣言できます。

``` cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** 一つの文で異なる型の変数を複数宣言しないでください。

``` cpp
//incorrect
int x, *y;
```

**28.** Cスタイルのキャストは使用しないでください。

``` cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** クラスと構造体では、各可視スコープ内でメンバーと関数を個別にグループ化します。

**30.** 小さなクラスと構造体の場合、メソッド宣言を実装から分離する必要はありません。

同じことが、クラスや構造体の小さなメソッドにも当てはまります。

テンプレート化されたクラスと構造体の場合、メソッド宣言を実装から分離しないでください（そうでない場合は、同じ翻訳単位で定義する必要があ

**31.** 行は140文字で、80文字ではなく折り返すことができます。

**32.** Postfixが必要ない場合は、常に接頭辞の増分/減分演算子を使用します。

``` cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

## コメント {#comments}

**1.** コードのすべての非自明な部分にコメントを追加してください。

これは非常に重要です。 書面でのコメントだけを更新したいのですが--このコードに必要な、又は間違っています。

``` cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** コメントは必要に応じて詳細に設定できます。

**3.** 記述するコードの前にコメントを配置します。 まれに、コメントが同じ行のコードの後に来ることがあります。

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

**4.** コメントは英語のみで書く必要があります。

**5.** を書いていて図書館を含む詳細なコメントで説明を主なヘッダファイルです。

**6.** 追加情報を提供しないコメントは追加しないでください。 特に放置しないでください空のコメントこのような:

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

この例はリソースから借用されていますhttp://home.tamk.fi/~jaalto/course/coding-style/doc/unmainainable-code/.

**7.** ごみのコメントを書かないでください（作成者、作成日。.)各ファイルの先頭にある。

**8.** 単一行のコメントはスラッシュで始まります: `///` 複数行のコメントは `/**`. これらのコメントは、 “documentation”.

注:Doxygenを使用すると、これらのコメントからドキュメントを生成できます。 しかし、Ideでコードをナビゲートする方が便利なので、Doxygenは一般的には使用されません。

**9.** 複数行コメントの先頭と末尾に空行を含めることはできません(複数行コメントを閉じる行を除きます)。

**10.** コメント行コードは、基本的なコメントは、 “documenting” コメント。

**11.** 削除のコメントアウトされていパーツのコード深い.

**12.** コメントやコードで冒涜を使用しないでください。

**13.** 大文字は使用しないでください。 過度の句読点を使用しないでください。

``` cpp
/// WHAT THE FAIL???
```

**14.** 使用しないコメントをdelimeters.

``` cpp
///******************************************************
```

**15.** コメントで議論を開始しないでください。

``` cpp
/// Why did you do this stuff?
```

**16.** それが何であったかを説明するブロックの最後にコメントを書く必要はありません。

``` cpp
/// for
```

## 名前 {#names}

**1.** 変数とクラスメンバーの名前にアンダースコア付きの小文字を使用します。

``` cpp
size_t max_block_size;
```

**2.** 関数（メソッド）の名前には、小文字で始まるcamelCaseを使用します。

``` cpp
std::string getName() const override { return "Memory"; }
```

**3.** クラス(構造体)の名前には、大文字で始まるCamelCaseを使用します。 I以外の接頭辞はインターフェイスには使用されません。

``` cpp
class StorageMemory : public IStorage
```

**4.** `using` クラスと同じように、または `_t` 最後に。

**5.** テンプレート型引数の名前:単純な場合は、次のようにします `T`; `T`, `U`; `T1`, `T2`.

より複雑な場合は、クラス名の規則に従うか、プレフィックスを追加します `T`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** テンプレート定数引数の名前:変数名の規則に従うか、または `N` 簡単なケースでは。

``` cpp
template <bool without_www>
struct ExtractDomain
```

**7.** 抽象クラス(インターフェイス)については、 `I` プレフィックス

``` cpp
class IBlockInputStream
```

**8.** 変数をローカルで使用する場合は、短い名前を使用できます。

それ以外の場合は、意味を説明する名前を使用します。

``` cpp
bool info_successfully_loaded = false;
```

**9.** の名前 `define`sおよびグローバル定数を使用ALL\_CAPSをアンダースコア(\_).

``` cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** ファイル名は内容と同じスタイルを使用する必要があります。

ファイルに単一のクラスが含まれている場合は、クラス(CamelCase)と同じ方法でファイルに名前を付けます。

ファイルに単一の関数が含まれている場合は、関数(camelCase)と同じ方法でファイルに名前を付けます。

**11.** 名前に略語が含まれている場合は、:

-   変数名の場合、省略形は小文字を使用する必要があります `mysql_connection` （ない `mySQL_connection`).
-   クラスおよび関数の名前については、省略形に大文字を使用します`MySQLConnection` （ない `MySqlConnection`).

**12.** クラスメンバーを初期化するためだけに使用されるコンストラクター引数には、クラスメンバーと同じ名前を付ける必要がありますが、最後にアンダースコ

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

引数がコンストラクタ本体で使用されていない場合は、アンダースコアの接尾辞を省略できます。

**13.** ローカル変数とクラスメンバーの名前に違いはありません（接頭辞は必要ありません）。

``` cpp
timer (not m_timer)
```

**14.** の定数に対して `enum`、大文字でキャメルケースを使用します。 ALL\_CAPSも許容されます。 もし `enum` は非ローカルである。 `enum class`.

``` cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** すべての名前は英語である必要があります。 ロシア語の音訳は許可されていません。

    not Stroka

**16.** 略語は、よく知られている場合（Wikipediaや検索エンジンで略語の意味を簡単に見つけることができる場合）に許容されます。

    `AST`, `SQL`.

    Not `NVDH` (some random letters)

短縮版が一般的に使用されている場合、不完全な単語は許容されます。

コメントの横にフルネームが含まれている場合は、略語を使用することもできます。

**17.** C++のソースコードを持つファイル名は、 `.cpp` 延長。 ヘッダファイルには `.h` 延長。

## コードの書き方 {#how-to-write-code}

**1.** メモリ管理。

手動メモリ解放 (`delete`）ライブラリコードでのみ使用できます。

ライブラリコードでは、 `delete` operatorはデストラクタでのみ使用できます。

アプリケーショ

例:

-   最も簡単な方法は、スタック上にオブジェクトを配置するか、別のクラスのメンバーにすることです。
-   多数の小さなオブジェクトの場合は、コンテナを使用します。
-   ヒープ内に存在する少数のオブジェクトの自動割り当て解除には、以下を使用します `shared_ptr/unique_ptr`.

**2.** リソース管理。

使用 `RAII` 上記を参照してください。

**3.** エラー処理。

例外を使用します。 ほとんどの場合、例外をスローするだけで、それをキャッチする必要はありません（ `RAII`).

オフライ

ユーザー要求を処理するサーバーでは、通常、接続ハンドラの最上位レベルで例外をキャッチするだけで十分です。

スレッド機能、とくすべての例外rethrowのメインスレッド後 `join`.

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

ない非表示の例外なります。 すべての例外を盲目的に記録するだけではありません。

``` cpp
//Not correct
catch (...) {}
```

いくつかの例外を無視する必要がある場合は、特定の例外に対してのみ実行し、残りを再スローします。

``` cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

応答コードまたは関数を使用する場合 `errno`、常に結果をチェックし、エラーの場合は例外をスローします。

``` cpp
if (0 != close(fd))
    throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);
```

`Do not use assert`.

**4.** 例外タイプ。

アプリケーションコードで複雑な例外階層を使用する必要はありません。 例外テキストは、システム管理者が理解できるはずです。

**5.** 投げから例外がスローされる場合destructors.

これは推奨されませんが、許可されています。

次のオプションを使用しま:

-   関数の作成 (`done()` または `finalize()`）それは例外につながる可能性のあるすべての作業を事前に行います。 その関数が呼び出された場合、後でデストラクタに例外はないはずです。
-   タスクも複雑で(メッセージを送信するなどのネットワーク)を置くことができます別の方法は、クラスのユーザーを呼び出す前に破壊.
-   デストラクタに例外がある場合は、それを非表示にするよりもログに記録する方が良いでしょう（ロガーが利用可能な場合）。
-   簡単な適用では、頼ることは受諾可能です `std::terminate` （以下の場合 `noexcept` デフォルトではc++11）例外を処理する。

**6.** 匿名コードブロック。

特定の変数をローカルにするために、単一の関数内に別のコードブロックを作成して、ブロックを終了するときにデストラクターが呼び出されるようにす

``` cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** マルチスレッド

オフライ:

-   単一のCPUコアで可能な限り最高のパフォーマンスを得ようとします。 必要に応じて、コードを並列化できます。

サーバーアプリ:

-   スレッドプールを使用して要求を処理します。 この時点で、まだたタスクを必要とした管理コスイッチング時の値です。※

Forkは並列化には使用されません。

**8.** 同期スレッド。

くすることが可能で別のスレッドが別のメモリー細胞により異なるキャッシュ回線)を使用していないスレッドが同期を除く `joinAll`).

同期が必要な場合は、ほとんどの場合、以下の条件でmutexを使用すれば十分です `lock_guard`.

他の場合は、システム同期プリミティブを使用します。 Busy waitは使用しないでください。

原子演算は、最も単純な場合にのみ使用する必要があります。

主な専門分野でない限り、ロックフリーのデータ構造を実装しようとしないでください。

**9.** ポインタ対参照。

ほとんどの場合、参照を好む。

**10.** const.

定数参照、定数へのポインタを使用する, `const_iterator`、およびconstメソッド。

考慮する `const` デフォルトで非を使用するには-`const` 必要なときだけ。

変数を値で渡すときは、 `const` 通常は意味がありません。

**11.** 無署名

使用 `unsigned` 必要に応じて。

**12.** 数値型。

タイプを使用する `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`,and `Int64` だけでなく、 `size_t`, `ssize_t`,and `ptrdiff_t`.

これらの型を数値に使用しないでください: `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** 引数を渡す。

参照による複素数値の渡し(含む `std::string`).

関数がヒープ内に作成されたオブジェクトの所有権を取得する場合は、引数の型を作成します `shared_ptr` または `unique_ptr`.

**14.** 戻り値。

ほとんどの場合、 `return`. 書かない `[return std::move(res)]{.strike}`.

関数がオブジェクトをヒープに割り当てて返す場合は、次のようにします `shared_ptr` または `unique_ptr`.

まれに、引数を使用して値を返す必要がある場合があります。 この場合、引数は参照でなければなりません。

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

**15.** 名前空間。

別のものを使用する必要はありません `namespace` 適用コードのため。

小さな図書館でもこれは必要ありません。

中規模から大規模のライブラリの場合は、すべてを `namespace`.

図書館の `.h` ファイル、使用できます `namespace detail` アプリケーションコードに必要ない実装の詳細を非表示にする。

で `.cpp` を使用することができます `static` またはシンボルを非表示にする匿名の名前空間。

また、 `namespace` に使用することができます `enum` 対応する名前が外部に落ちないようにするには `namespace` （しかし、それを使用する方が良いです `enum class`).

**16.** 遅延初期化。

初期化に引数が必要な場合は、通常は既定のコンストラクタを記述しないでください。

後で初期化を遅らせる必要がある場合は、無効なオブジェクトを作成する既定のコンストラクターを追加できます。 または、少数のオブジェクトの場合は、以下を使用できます `shared_ptr/unique_ptr`.

``` cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** 仮想関数。

クラスが多態的な使用を意図していない場合、関数を仮想にする必要はありません。 これはデストラクタにも当てはまります。

**18.** エンコーディング

どこでもUTF-8を使用します。 使用 `std::string`と`char *`. 使用しない `std::wstring`と`wchar_t`.

**19.** ロギング

コードのどこでも例を参照してください。

コミットする前に、無意味なログやデバッグログ、その他のデバッグ出力をすべて削除します。

トレースレベルでも、サイクルのログインは避けるべきです。

ログには必読でログインです。

ログインできるアプリケーションコードにすることができます。

ログメッセージは英語で書く必要があります。

ログは、システム管理者が理解できるようにしてください。

ログに冒涜を使用しないでください。

ログでUTF-8エンコーディングを使用します。 まれに、ログに非ASCII文字を使用できます。

**20.** 入出力。

使用しない `iostreams` 内部のサイクルにおいて不可欠な存在であのためのアプリケーション性能(い利用 `stringstream`).

使用する `DB/IO` 代わりに図書館。

**21.** 日付と時刻。

を参照。 `DateLUT` 図書館

**22.** 含める。

常に使用 `#pragma once` 代わりに警備員を含めます。

**23.** を使用して。

`using namespace` は使用されません。 以下を使用できます `using` 特定の何かと。 しかし、クラスや関数内でローカルにします。

**24.** 使用しない `trailing return type` 必要がない限り機能のため。

``` cpp
[auto f() -&gt; void;]{.strike}
```

**25.** 変数の宣言と初期化。

``` cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** のための仮想関数を書く `virtual` 基本クラスでは、 `override` 代わりに `virtual` 子孫クラスで。

## C++の未使用の機能 {#unused-features-of-c}

**1.** 仮想継承は使用されません。

**2.** C++03の例外指定子は使用されません。

## プラット {#platform}

**1.** を書いていますコードの特定の。

それが同じ場合には、クロス-プラットフォームまたは携帯コードが好ましい。

**2.** 言語:C++17.

**3.** コンパイラ: `gcc`. 2017年現在、コードはバージョン7.2を使用してコンパイルされている。 （以下を使ってコンパイルできます `clang 4`.)

標準ライブラリが使用されます (`libstdc++` または `libc++`).

**4.**OS：LinuxのUbuntuの、正確よりも古いではありません。

**5.**コードはx86\_64CPUアーキテクチャ用に書かれている。

CPU命令セットは、サーバー間でサポートされる最小のセットです。 現在、SSE4.2です。

**6.** 使用 `-Wall -Wextra -Werror` コンパイルフラグ。

**7.** 静的に接続することが困難なライブラリを除くすべてのライブラリとの静的リンクを使用します。 `ldd` コマンド）。

**8.** コードはリリース設定で開発およびデバッグされます。

## ツール {#tools}

**1.** KDevelopは良いIDEです。

**2.** デバッグの使用 `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...`,または `tcmalloc_minimal_debug`.

**3.** のためのプロファイリングを使用 `Linux Perf`, `valgrind` (`callgrind`）、または `strace -cf`.

**4.** ソースはGitにあります。

**5.** アセンブリの使用 `CMake`.

**6.** プログラムは `deb` パッケージ。

**7.** ることを約束し、マスターが破ってはいけないの。

選択したリビジョンのみが実行可能とみなされます。

**8.** コードが部分的にしか準備できていなくても、できるだけ頻繁にコミットを行います。

この目的のために分岐を使用します。

あなたのコードが `master` branchはまだビルド可能ではありません。 `push`. あなたはそれを終了するか、数日以内にそれを削除する必要があります。

**9.** 些細な変更ではない場合は、ブランチを使用してサーバーに公開します。

**10.** 未使用のコードがリポジトリから削除されます。

## 図書館 {#libraries}

**1.** C++14標準ライブラリが使用されています（実験的な拡張が許可されています）。 `boost` と `Poco` フレームワーク

**2.** 必要に応じて、OSパッケージで利用可能な既知のライブラリを使用できます。

すでに利用可能な良い解決策がある場合は、別のライブラリをインストールする必要がある場合でも、それを使用してください。

(が準備をしておいてくださ去の悪い図書館からのコードです。)

**3.** パッケージに必要なものがない場合や、古いバージョンや間違った種類のコンパイルがある場合は、パッケージにないライブラリをインストールできます。

**4.** ライブラリが小さく、独自の複雑なビルドシステムがない場合は、ソースファイルを `contrib` フォルダ。

**5.** すでに使用されているライブラリが優先されます。

## 一般的な推奨事項 {#general-recommendations-1}

**1.** できるだけ少ないコードを書く。

**2.** う最も単純な解決策です。

**3.** どのように動作し、内部ループがどのように機能するかを知るまで、コードを記述しないでください。

**4.** 最も単純なケースでは、 `using` クラスや構造体の代わりに。

**5.** 可能であれば、コピーコンストラクター、代入演算子、デストラクター(クラスに少なくとも一つの仮想関数が含まれている場合は仮想関数を除く)、コンストラク つまり、コンパイラで生成された関数は正しく動作する必要があります。 以下を使用できます `default`.

**6.** コードの簡素化が推奨されます。 可能であれば、コードのサイズを小さくします。

## その他の推奨事項 {#additional-recommendations}

**1.** 明示的に指定する `std::` からのタイプのため `stddef.h`

推奨されません。 つまり、書くことをお勧めします `size_t` 代わりに `std::size_t` それは短いので。

追加することは許容されます `std::`.

**2.** 明示的に指定する `std::` 標準Cライブラリの関数の場合

推奨されません。 言い換えれば、 `memcpy` 代わりに `std::memcpy`.

その理由は、次のような非標準的な機能があるからです `memmem`. 私たちは機会にこれらの機能を使用します。 これらの関数は、 `namespace std`.

あなたが書く場合 `std::memcpy` 代わりに `memcpy` どこでも、その後、 `memmem` なし `std::` 奇妙に見えます。

それにもかかわらず、 `std::` あなたがそれを好むなら。

**3.** 標準C++ライブラリで同じ関数が使用できる場合、Cからの関数を使用します。

これは、より効率的であれば許容されます。

たとえば、次を使用します `memcpy` 代わりに `std::copy` メモリの大きな塊をコピーするため。

**4.** 複数行の関数の引数。

の他の包装のスタイルを許可:

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

[元の記事](https://clickhouse.tech/docs/en/development/style/) <!--hide-->
