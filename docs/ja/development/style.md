---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 68
toc_title: "C++\u30B3\u30FC\u30C9\u306E\u66F8\u304D\u65B9"
---

# C++コードの書き方 {#how-to-write-c-code}

## 一般的な推奨事項 {#general-recommendations}

**1.** 以下は要件ではなく推奨事項です。

**2.** コードを編集している場合は、既存のコードの書式設定に従うことが理にかなっています。

**3.** 一貫性のためにコードスタイルが必要です。 一貫性により、コードを読みやすくなり、コードの検索も容易になります。

**4.** ルールの多くは論理的な理由を持っていない;彼らは確立された慣行によって決定されます。

## 書式設定 {#formatting}

**1.** 書式設定のほとんどは自動的に行われます `clang-format`.

**2.** インデントは4スペースです。 タブが四つのスペースを追加するように開発環境を構成します。

**3.** 中括弧を開くと閉じるには、別の行にする必要があります。

``` cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** 関数本体全体が単一の場合 `statement`、それは単一ラインに置くことができます。 中括弧の周りにスペースを配置します（行末のスペース以外）。

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

**6.** で `if`, `for`, `while` その他の式では、関数呼び出しではなく、開き括弧の前にスペースが挿入されます。

``` cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** 二項演算子の前後にスペースを追加 (`+`, `-`, `*`, `/`, `%`, …) and the ternary operator `?:`.

``` cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** ラインフィードが入力された場合は、オペレータを新しい行に置き、その前にインデントを増やします。

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

**10.** 演算子の周囲にスペースを使用しない `.`, `->`.

必要に応じて、オペレータは次の行にラップすることができます。 この場合、その前のオフセットが増加する。

**11.** 単項演算子を区切るためにスペースを使用しない (`--`, `++`, `*`, `&`, …) from the argument.

**12.** 入れの後に空白、コンマといえるものです。 同じルールはaの中のセミコロンのために行く `for` 式。

**13.** を区切るためにスペースを使用しない。 `[]` オペレーター

**14.** で `template <...>` 式の間にスペースを使用します `template` と `<`;後にスペースなし `<` または前に `>`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** クラスと構造では、 `public`, `private`、と `protected` 同じレベルで `class/struct` コードの残りの部分をインデントします。

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

**17.** のためのブロック `if`, `for`, `while`、または他の式は、単一の `statement` 中括弧は省略可能です。 場所は `statement` 別の行に、代わりに。 この規則は、ネストされた場合にも有効です `if`, `for`, `while`, …

しかし、内側の場合 `statement` 中括弧または `else`、外部ブロックは中括弧で記述する必要があります。

``` cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** ラインの端にスペースがあってはなりません。

**19.** ソースファイルはUTF-8エンコードです。

**20.** ASCII以外の文字は、文字列リテラルで使用できます。

``` cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** 単一行に複数の式を書き込まないでください。

**22.** 関数内のコードのセクションをグループ化し、複数の空行で区切ります。

**23.** 関数、クラスなどを一つまたは二つの空行で区切ります。

**24.** `A const` (値に関連する)型名の前に記述する必要があります。

``` cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** ポインタまたは参照を宣言するとき、 `*` と `&` 記号は両側のスペースで区切る必要があります。

``` cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** テンプレート-タイプを使用する場合は、それらを `using` キーワード(最も単純な場合を除く)。

つまり、テンプレートのパラメータは指定しみ `using` そして、コードで繰り返されていません。

`using` 関数の内部など、ローカルで宣言できます。

``` cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** なを宣言するのに複数の変数の異なる種類の一つです。

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

**29.** 授業や構造体、グループのメンバーは、機能別に各部の可視性です。

**30.** 小さなクラスや構造体の場合、メソッド宣言を実装から分離する必要はありません。

同じことが、クラスや構造体の小さなメソッドにも当てはまります。

テンプレート化されたクラスと構造体の場合、メソッド宣言を実装から分離しないでください（そうでない場合は、同じ翻訳単位で定義する必要があ

**31.** 行を140文字で折り返すことができます（80文字ではなく）。

**32.** Postfixが不要な場合は、必ずprefix increment/decrement演算子を使用してください。

``` cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

## コメント {#comments}

**1.** コードのすべての非自明な部分にコメントを追加してください。

これは非常に重要です。 書面でのコメントだけを更新したいのですが–このコードに必要な、又は間違っています。

``` cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** コメントは、必要に応じて詳細に記述できます。

**3.** コメントを記述するコードの前に配置します。 まれに、コメントは同じ行のコードの後に来ることがあります。

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

**4.** コメントは英語のみで記述する必要があります。

**5.** を書いていて図書館を含む詳細なコメントで説明を主なヘッダファイルです。

**6.** 追加情報を提供しないコメントを追加しないでください。 特に放置しないでください空のコメントこのような:

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

この例はリソースから借用されていますhttp://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/。

**7.** ガベージコメント（作成者、作成日）を書いてはいけません。.）各ファイルの先頭に。

**8.** シングルラインのコメントずつスラッシュ: `///` 複数行のコメントは次の形式で始まります `/**`. これらのコメントは、 “documentation”.

注:doxygenを使用して、これらのコメントからドキュメントを生成できます。 しかし、doxygenはideのコードをナビゲートする方が便利なので、一般的には使用されません。

**9.** 複数行のコメントは、先頭と末尾に空の行を含めることはできません(複数行のコメントを閉じる行を除く)。

**10.** コメント行コードは、基本的なコメントは、 “documenting” コメント.

**11.** コミットする前に、コードのコメント部分を削除します。

**12.** コメントやコードで冒涜を使用しないでください。

**13.** 大文字は使用しないでください。 過度の句読点を使用しないでください。

``` cpp
/// WHAT THE FAIL???
```

**14.** 区切り文字の作成にはコメントを使用しません。

``` cpp
///******************************************************
```

**15.** コメントで議論を開始しないでください。

``` cpp
/// Why did you do this stuff?
```

**16.** それが何であるかを記述するブロックの最後にコメントを書く必要はありません。

``` cpp
/// for
```

## 名前 {#names}

**1.** 変数とクラスメンバーの名前には、アンダースコア付きの小文字を使用します。

``` cpp
size_t max_block_size;
```

**2.** 関数(メソッド)の名前には、小文字で始まるcamelCaseを使用します。

``` cpp
std::string getName() const override { return "Memory"; }
```

**3.** クラス（構造体）の名前には、大文字で始まるCamelCaseを使用します。 I以外の接頭辞はインターフェイスには使用されません。

``` cpp
class StorageMemory : public IStorage
```

**4.** `using` クラスと同じように名前が付けられます。 `_t` 最後に。

**5.** テンプレート型引数の名前:単純なケースでは、 `T`; `T`, `U`; `T1`, `T2`.

より複雑なケースでは、クラス名の規則に従うか、プレフィックスを追加します `T`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** テンプレート定数引数の名前:変数名の規則に従うか、または `N` 単純なケースでは。

``` cpp
template <bool without_www>
struct ExtractDomain
```

**7.** 抽象クラス（インターフェイス）の場合は、 `I` 接頭辞。

``` cpp
class IBlockInputStream
```

**8.** ローカルで変数を使用する場合は、短い名前を使用できます。

それ以外の場合は、意味を説明する名前を使用します。

``` cpp
bool info_successfully_loaded = false;
```

**9.** の名前 `define`sおよびグローバル定数は、ALL\_CAPSとアンダースコアを使用します。

``` cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** ファイル名は内容と同じスタイルを使用する必要があります。

ファイルに単一のクラスが含まれている場合は、クラス（camelcase）と同じようにファイルに名前を付けます。

ファイルに単一の関数が含まれている場合は、そのファイルに関数(camelcase)と同じ名前を付けます。

**11.** 名前に略語が含まれている場合は、:

-   変数名の場合、省略形は小文字を使用する必要があります `mysql_connection` (ない `mySQL_connection`).
-   クラスと関数の名前については、大文字を省略形にしておきます`MySQLConnection` (ない `MySqlConnection`).

**12.** クラスメンバを初期化するためだけに使用されるコンストラクタ引数は、クラスメンバと同じように名前を付ける必要がありますが、最後にアン

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

のアンダースコアの接尾辞ければ省略することができ、引数を使用していないのコンストラクタ。

**13.** ローカル変数とクラスメンバの名前に違いはありません（接頭辞は必要ありません）。

``` cpp
timer (not m_timer)
```

**14.** の定数のために `enum`、大文字でキャメルケースを使用します。 ALL\_CAPSも許容されます。 この `enum` ローカルではなく、 `enum class`.

``` cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** すべての名前は英語である必要があります。 ロシア語の音訳は許可されていません。

    not Stroka

**16.** 略語は、よく知られている場合（ウィキペディアや検索エンジンで略語の意味を簡単に見つけることができる場合）には許容されます。

    `AST`, `SQL`.

    Not `NVDH` (some random letters)

短くされた版が共通の使用なら不完全な単語は受諾可能である。

コメントの横にフルネームが含まれている場合は、省略形を使用することもできます。

**17.** C++ソースコードを持つファイル名には、 `.cpp` 拡張子。 ヘッダーファイルには、 `.h` 拡張子。

## コードの書き方 {#how-to-write-code}

**1.** メモリ管理。

手動メモリ割り当て解除 (`delete`)ライブラリコードでのみ使用できます。

ライブラリコードでは、 `delete` 演算子はデストラクターでのみ使用できます。

アプリケーショ

例:

-   最も簡単な方法は、スタックにオブジェクトを配置するか、別のクラスのメンバーにすることです。
-   多数の小さなオブジェクトの場合は、コンテナを使用します。
-   ヒープに存在する少数のオブジェクトの自動割り当て解除の場合は、以下を使用します `shared_ptr/unique_ptr`.

**2.** リソース管理。

使用 `RAII` と上記参照。

**3.** エラー処理。

例外を使用します。 ほとんどの場合、例外をスローするだけで、例外をキャッチする必要はありません `RAII`).

オフラインのデータ処理アプリケーションでは、しばしば可能な漁例外をスローしました。

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

ない非表示の例外なります。 盲目的にすべての例外をログに記録することはありません。

``` cpp
//Not correct
catch (...) {}
```

いくつかの例外を無視する必要がある場合は、特定の例外に対してのみ行い、残りを再スローします。

``` cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

応答コード付きの関数を使用する場合、または `errno`、常に結果をチェックし、エラーの場合は例外をスローします。

``` cpp
if (0 != close(fd))
    throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);
```

`Do not use assert`.

**4.** 例外タイプ。

アプリケーションコードで複雑な例外階層を使用する必要はありません。 例外テキストは、システム管理者が理解できるはずです。

**5.** 投げから例外がスローされる場合destructors.

これは推奨されませんが、許可されています。

次のオプションを使用:

-   関数の作成 (`done()` または `finalize()`）それは例外につながる可能性のあるすべての作業を事前に行います。 その関数が呼び出された場合、後でデストラクタに例外はないはずです。
-   複雑すぎるタスク（ネットワーク経由でメッセージを送信するなど）は、クラスユーザーが破棄する前に呼び出す必要がある別のメソッドに入れることがで
-   デストラクタに例外がある場合は、それを隠すよりもログに記録する方が良いでしょう（ロガーが利用可能な場合）。
-   簡単な適用では、頼ることは受諾可能です `std::terminate` （以下の場合 `noexcept` デフォルトではC++11）例外を処理する。

**6.** 匿名のコードブロック。

特定の変数をローカルにするために、単一の関数内に別のコードブロックを作成して、ブロックを終了するときにデストラクタが呼び出されるように

``` cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** マルチスレッド。

オフラインのデータ処理プログラム:

-   単一のcpuコアで最高のパフォーマンスを得るようにしてください。 必要に応じてコードを並列化できます。

でサーバアプリケーション:

-   スレッドプールを使用して要求を処理します。 この時点で、まだたタスクを必要とした管理コスイッチング時の値です。※

Forkは並列化には使用されません。

**8.** スレッドの同期。

多くの場合、異なるスレッドに異なるメモリセルを使用させることができます（さらに良い：異なるキャッシュライン）。 `joinAll`).

同期が必要な場合は、ほとんどの場合、mutexを使用すれば十分です。 `lock_guard`.

他の例では、システム同期プリミティブを使用します。 使用中の待ち時間を使用しないで下さい。

原子操作は、最も単純な場合にのみ使用する必要があります。

主な専門分野でない限り、ロックフリーのデータ構造を実装しようとしないでください。

**9.** ポインタ対参照。

ほとんどの場合、参照を好む。

**10.** const

定数参照、定数へのポインタを使用する, `const_iterator`、およびconstメソッド。

考える `const` デフォルトにしてnonを使用するには-`const` 必要なときだけ。

値によって変数を渡すとき、 `const` 通常は意味をなさない。

**11.** 署名なし

使用 `unsigned` 必要であれば。

**12.** 数値型。

タイプの使用 `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`、と `Int64`、同様に `size_t`, `ssize_t`、と `ptrdiff_t`.

これらの型を数値に使用しないでください: `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** 引数を渡す。

参照によって複雑な値を渡す(以下を含む `std::string`).

関数がヒープで作成されたオブジェクトの所有権を取得する場合は、引数の型を作成します `shared_ptr` または `unique_ptr`.

**14.** 戻り値。

ほとんどの場合、単に `return`. 書き込まない `[return std::move(res)]{.strike}`.

関数がヒープ上にオブジェクトを割り当て、それを返す場合は、 `shared_ptr` または `unique_ptr`.

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

小さな図書館にもこれは必要ありません。

中規模から大規模のライブラリの場合は、すべてを `namespace`.

ライブラリの中で `.h` ファイル、使用できます `namespace detail` アプリケーションコードに必要のない実装の詳細を非表示にする。

で `.cpp` ファイル、あなたが使用できる `static` または匿名の名前空間は、記号を非表示にします。

また、 `namespace` に使用することができ `enum` 対応する名前が外部に落ちないようにするには `namespace` （しかし、それを使用する方が良いです `enum class`).

**16.** 遅延初期化。

初期化に引数が必要な場合は、通常はデフォルトのコンストラクタを記述すべきではありません。

後で初期化を遅らせる必要がある場合は、無効なオブジェクトを作成する既定のコンストラクターを追加できます。 または、少数のオブジェクトの場合は、次のものを使用できます `shared_ptr/unique_ptr`.

``` cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** 仮想関数。

クラスが多態的な使用を意図していない場合は、関数を仮想にする必要はありません。 これはデストラクタにも当てはまります。

**18.** エンコーディング。

どこでもutf-8を使用します。 使用 `std::string`と`char *`. 使用しない `std::wstring`と`wchar_t`.

**19.** ログ記録。

コードのどこにでも例を見てください。

コミットする前に、無意味なデバッグログとその他のデバッグ出力をすべて削除します。

トレースレベルでも、サイクルでのログ記録は避けるべきです。

ログには必読でログインです。

ログインできるアプリケーションコードにすることができます。

ログメッセージは英語で書く必要があります。

ログは、システム管理者が理解できることが望ましいです。

ログに冒涜を使用しないでください。

ログにutf-8エンコーディングを使用します。 まれに、ログに非ascii文字を使用できます。

**20.** 入出力。

使用しない `iostreams` アプリケーショ `stringstream`).

を使用 `DB/IO` 代わりに図書館。

**21.** 日付と時刻。

を見る `DateLUT` ライブラリ。

**22.** 含める。

常に使用 `#pragma once` 代わりに、警備員を含みます。

**23.** 使用。

`using namespace` は使用されません。 を使用することができ `using` 特定の何かと。 しかし、クラスや関数の中でローカルにします。

**24.** 使用しない `trailing return type` 必要な場合を除き、機能のため。

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

**26.** 仮想関数の場合は、以下を記述します `virtual` 基本クラスでは、次のように記述します `override` 代わりに `virtual` 子孫クラスで。

## C++の未使用機能 {#unused-features-of-c}

**1.** 仮想継承は使用されません。

**2.** C++03の例外指定子は使用されません。

## Platform {#platform}

**1.** を書いていますコードの特定の。

それが同じ場合には、クロス-プラットフォームまたは携帯コードが好ましい。

**2.** 言語：C++17。

**3.** コンパイラ: `gcc`. この時点で(December2017)、をコードはコンパイル使用してバージョン7.2。 （コンパイルすることもできます `clang 4`.)

標準ライブラリが使用されます (`libstdc++` または `libc++`).

**4.**OS：LinuxのUbuntuは、正確なよりも古いではありません。

**5.**コードはx86\_64cpuアーキテクチャ用に書かれている。

CPU命令セットは、当社のサーバー間でサポートされる最小セットです。 現在、SSE4.2です。

**6.** 使用 `-Wall -Wextra -Werror` コンパイルフラグ。

**7.** 静的に接続するのが難しいライブラリを除くすべてのライブラリとの静的リンクを使用します。 `ldd` コマンド）。

**8.** コードは開発され、リリース設定でデバッグされます。

## ツール {#tools}

**1.** KDevelopは良いIDEです。

**2.** デバッグのために、 `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...`、または `tcmalloc_minimal_debug`.

**3.** プロファイ `Linux Perf`, `valgrind` (`callgrind`)、または `strace -cf`.

**4.** ソースはGitにあります。

**5.** アセンブリ使用 `CMake`.

**6.** プログラムは `deb` パッケージ。

**7.** ることを約束し、マスターが破ってはいけないの。

選択したリビジョンのみが実行可能と見なされます。

**8.** コードが部分的にしか準備されていなくても、できるだけ頻繁にコミットを行います。

用の支店です。

あなたのコードが `master` ブランチはまだビルド可能ではない。 `push`. あなたはそれを終了するか、数日以内にそれを削除する必要があります。

**9.** 些細な変更の場合は、ブランチを使用してサーバーに公開します。

**10.** 未使用のコードはリポジトリから削除されます。

## ライブラリ {#libraries}

**1.** C++14標準ライブラリが使用されています（実験的な拡張が許可されています）。 `boost` と `Poco` フレームワーク

**2.** 必要に応じて、OSパッケージで利用可能な既知のライブラリを使用することができます。

すでに利用可能な良い解決策がある場合は、別のライブラリをインストールする必要があることを意味していても使用してください。

(が準備をしておいてくださ去の悪い図書館からのコードです。)

**3.** パッケージに必要なものがないか、古いバージョンや間違ったタイプのコンパイルがない場合は、パッケージに含まれていないライブラリをインストール

**4.** ライブラリが小さく、独自の複雑なビルドシステムを持たない場合は、ソースファイルを `contrib` フォルダ。

**5.** すでに使用されているライブラリが優先されます。

## 一般的な推奨事項 {#general-recommendations-1}

**1.** できるだけ少ないコードを書く。

**2.** う最も単純な解決策です。

**3.** それがどのように機能し、内部ループがどのように機能するかを知るまで、コードを書かないでください。

**4.** 最も単純な場合は、次のようにします `using` クラスや構造体の代わりに。

**5.** 可能であれば、コピーコンストラクター、代入演算子、デストラクター(仮想関数を除く、クラスに少なくとも一つの仮想関数が含まれている場合)、コンストラ つまり、コンパイラ生成機能しないでください。 を使用することができ `default`.

**6.** コードの単純化が推奨されます。 可能な場合は、コードのサイズを小さくします。

## その他の推奨事項 {#additional-recommendations}

**1.** 明示的に指定する `std::` からのタイプの場合 `stddef.h`

は推奨されません。 つまり、我々は書くことをお勧めします `size_t` 代わりに `std::size_t`、それは短いですので。

それは可能追加する `std::`.

**2.** 明示的に指定する `std::` 標準Cライブラリの関数の場合

は推奨されません。 言い換えれば、 `memcpy` 代わりに `std::memcpy`.

その理由は、次のような非標準的な機能があるからです `memmem`. 私達は機会にこれらの機能を使用します。 これらの関数は `namespace std`.

あなたが書く場合 `std::memcpy` 代わりに `memcpy` どこでも、その後 `memmem` なし `std::` 奇妙に見えます。

それでも、あなたはまだ `std::` あなたがそれを好むなら。

**3.** 同じものが標準C++ライブラリで利用可能な場合、Cの関数を使用する。

これは、より効率的であれば許容されます。

たとえば、以下を使用します `memcpy` 代わりに `std::copy` メモリの大きな塊をコピーするため。

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
