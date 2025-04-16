---
slug: /ja/development/style
sidebar_position: 71
sidebar_label: C++ ガイド
description: コーディングスタイル、命名規則、フォーマットなどに関する推奨事項の一覧
---

# C++ コードの書き方

## 一般的な推奨事項 {#general-recommendations}

**1.** 以下の内容は推奨事項であり、必須ではありません。

**2.** コードを編集する場合は、既存のコードのフォーマットに従うのが理にかなっています。

**3.** コードスタイルは一貫性のために必要です。一貫性があることでコードを読みやすくし、コード検索も容易になります。

**4.** 多くのルールには論理的な理由はありませんが、確立された慣行に従っています。

## フォーマット {#formatting}

**1.** `clang-format` により、ほとんどのフォーマットは自動的に行われます。

**2.** インデントは4スペースです。タブで4スペースが追加されるよう開発環境を設定してください。

**3.** 開閉中括弧は別行に置かれなければなりません。

``` cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** 関数の本体全体が単一の `statement` である場合、それを単一行に置くことができます。中括弧の周りにスペースを置いてください（行末のスペースを除いて）。

``` cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** 関数の場合、括弧の周りにスペースを入れないでください。

``` cpp
void reinsert(const Value & x)
```

``` cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** `if`、`for`、`while` などの式では、開き括弧の前にスペースが挿入されます（関数呼び出しとは対照的に）。

``` cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** 二項演算子（`+`、`-`、`*`、`/`、`%`、...）と三項演算子 `?:` の周りにスペースを追加してください。

``` cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** 改行が入る場合は、演算子を新しい行に置き、その前にインデントを増やします。

``` cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** 必要に応じて、行内の整列のためにスペースを使用することができます。

``` cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** 演算子 `.`、`->` の周りにスペースを使用しないでください。

必要であれば、演算子を次の行に包むことができます。この場合、前にインデントを増やします。

**11.** 単項演算子（`--`、`++`、`*`、`&`、...）を引数から分離するためにスペースを使用しないでください。

**12.** コンマの後にスペースを入れ、前には入れないでください。同じルールは `for` 式内のセミコロンにも当てはまります。

**13.** `[]` 演算子を分離するためのスペースを使用しないでください。

**14.** `template <...>` 式では、`template` と `<` の間にスペースを使用し、`<` の直後または `>` の直前にスペースを入れないでください。

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** クラスや構造体では、`public`、`private`、`protected` を `class/struct` と同じレベルに書き、それ以外のコードはインデントします。

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

**16.** ファイル全体で同じ `namespace` が使用され、他に重要なことがなければ、`namespace` 内でのインデントは不要です。

**17.** `if`、`for`、`while`、その他の式のブロックが単一の `statement` で構成されている場合、中括弧はオプションです。`statement` を別行に置いてください。このルールはネストされた `if`、`for`、`while` にも適用されます。

しかし、内部 `statement` に中括弧や `else` が含まれている場合、外部ブロックは中括弧で書かれるべきです。

``` cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** 行末にスペースを入れないでください。

**19.** ソースファイルは UTF-8 エンコードです。

**20.** 非ASCII文字は文字列リテラル内で使用できます。

``` cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** 単一行に複数の式を書かないでください。

**22.** 関数内のコードをセクションでグループ化し、最大で1つの空行で分離します。

**23.** 関数、クラスなどを1行または2行の空行で分離します。

**24.** `A const`（値に関連するもの）は型名の前に書かなければなりません。

``` cpp
//正しい書き方
const char * pos
const std::string & s
//誤った書き方
char const * pos
```

**25.** ポインタまたは参照を宣言するとき、`*` および `&` 記号は両側にスペースで分離されなければなりません。

``` cpp
//正しい書き方
const char * pos
//誤った書き方
const char* pos
const char *pos
```

**26.** テンプレート型を使用する場合、（最も簡単な場合を除いて）`using` キーワードでエイリアス化してください。

別の言い方をすると、テンプレートパラメータは `using` にのみ指定され、コード内で繰り返されません。

`using` は関数内などでローカルに宣言できます。

``` cpp
//正しい書き方
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//誤った書き方
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** 異なる型の複数の変数を一度に宣言しないでください。

``` cpp
//誤った書き方
int x, *y;
```

**28.** C スタイルのキャストを使用しないでください。

``` cpp
//誤った書き方
std::cerr << (int)c <<; std::endl;
//正しい書き方
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** クラスや構造体では、メンバーと関数をそれぞれの可視範囲内で別々にグループ化します。

**30.** 小さいクラスや構造体では、メソッドの宣言と実装を分ける必要はありません。

どのクラスや構造体でも小さなメソッドには同様です。

テンプレートクラスや構造体では、メソッドの宣言と実装を分けないでください（そうしないと、同じ翻訳単位に定義されなければなりません）。

**31.** 140文字で改行することができます、80文字ではなく。

**32.** 後置インクリメント/デクリメント演算子が必要ない場合は、常に前置インクリメント/デクリメント演算子を使用してください。

``` cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

## コメント {#comments}

**1.** 複雑な部分のコードには必ずコメントを追加してください。

これは非常に重要です。コメントを書くことで、そのコードが不要であるか、設計が間違っていることに気付く可能性があります。

``` cpp
/** 使用できるメモリの部分。
  * 例えば、internal_buffer が 1MB で、読み込みのためにファイルからバッファに 10 バイトしかロードされていない場合、
  * working_buffer は10バイトのサイズを持ちます。
  * （working_buffer.end() は読み取り可能な10バイトの直後の位置を指します）。
  */
```

**2.** コメントは必要に応じて詳細に書くことができます。

**3.** コメントは、その説明対象コードの前に置いてください。まれに、同じ行内のコードの後に置くこともできます。

``` cpp
/** クエリを解析し、実行します。
*/
void executeQuery(
    ReadBuffer & istr, /// クエリを（INSERTの場合はデータも）読み込む場所
    WriteBuffer & ostr, /// 結果を書き込む場所
    Context & context, /// DB、テーブル、データ型、エンジン、関数、集計関数...
    BlockInputStreamPtr & query_plan, /// クエリがどのように実行されたかの説明がここに書かれる
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete /// SELECT クエリをどの段階まで処理するか
    )
```

**4.** コメントは英語でのみ書かれなければなりません。

**5.** ライブラリを書いている場合は、メインヘッダーファイルに詳しい説明コメントを含めてください。

**6.** 追加情報を提供しないコメントを追加しないでください。特に、以下のような空コメントを残さないでください：

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

例はリソース http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/ から引用されています。

**7.** 各ファイルの冒頭には、作成者や作成日などのゴミコメントを書かないでください。

**8.** 単一行のコメントは三つのスラッシュ `///` で始まり、複数行のコメントは `/**` で始まります。これらのコメントは「ドキュメンテーション」と見なされます。

注意: Doxygen を使ってこれらのコメントからドキュメントを生成することができます。しかし、通常はIDEでコードをナビゲートする方が便利です。

**9.** 複数行のコメントは、冒頭と末尾に空行を置かないでください（複数行コメントを閉じる行を除く）。

**10.** コードをコメントアウトするためには、基本的なコメントを使用し、「ドキュメンテーション」コメントを使わないでください。

**11.** コミット前にはコメントアウトされたコード部分を削除してください。

**12.** コメントやコードに不適切な言葉を使わないでください。

**13.** 大文字を使用しないでください。過剰な句読点を使わないでください。

``` cpp
/// WHAT THE FAIL???
```

**14.** コメントで区切りを作らないでください。

``` cpp
///******************************************************
```

**15.** コメント内で議論を始めないでください。

``` cpp
/// Why did you do this stuff?
```

**16.** ブロックが何であったかを最後に説明するコメントは必要ありません。

``` cpp
/// for
```

## 名前 {#names}

**1.** 変数とクラスメンバーの名前には小文字とアンダースコアを使用してください。

``` cpp
size_t max_block_size;
```

**2.** 関数（メソッド）の名前には、小文字で始まるキャメルケースを使用してください。

``` cpp
std::string getName() const override { return "Memory"; }
```

**3.** クラス（構造体）の名前には、大文字で始まるキャメルケースを使用してください。インターフェイスには I 以外のプレフィックスは使用しません。

``` cpp
class StorageMemory : public IStorage
```

**4.** `using` はクラスと同じように名付けられます。

**5.** テンプレート型の引数の名前は、簡単な場合には `T`; `T`, `U`; `T1`, `T2`. を使用します。

より複雑な場合には、クラス名の規則に従うか、プレフィックス `T` を追加します。

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** テンプレート定数引数の名前は、変数名の規則に従うか、簡単な場合には `N` を使用します。

``` cpp
template <bool without_www>
struct ExtractDomain
```

**7.** 抽象クラス（インターフェース）の場合、`I` プレフィックスを追加できます。

``` cpp
class IProcessor
```

**8.** 変数をローカルに使用する場合、短い名前を使用できます。

他のすべての場合は意味を説明する名前を使用してください。

``` cpp
bool info_successfully_loaded = false;
```

**9.** `define` やグローバル定数の名前はアンダースコアを使用した ALL_CAPS を使用します。

``` cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** ファイル名はその内容と同じスタイルを使用します。

ファイルが単一のクラスを含む場合、クラスと同じようにファイルを名付けます（CamelCase）。

ファイルが単一の関数を含む場合、関数と同じようにファイルを名付けます（camelCase）。

**11.** 名前に略語が含まれている場合：

- 変数名の場合、略語は小文字のまま `mysql_connection`（`mySQL_connection` ではない）で使用します。
- クラスや関数の名前では、省略語の大文字を維持する `MySQLConnection`（`MySqlConnection` ではない）。

**12.** クラスメンバーを初期化するために使用されるコンストラクター引数は、クラスメンバーと同じように名付けますが、末尾にアンダースコアを付けて名付けます。

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

構造体のコンストラクター本体で使用されない場合は、末尾のアンダースコアを省くことができます。

**13.** ローカル変数とクラスメンバーには違いがありません（プレフィックスは不要です）。

``` cpp
timer（`m_timer` ではなく）
```

**14.** `enum` の定数には、大文字から始まる CamelCase を使用します。ALL_CAPS も受け入れられます。`enum` がローカルでない場合は、`enum class` を使用してください。

``` cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** すべての名前は英語でなければなりません。ヘブライ文字の翻字は許可されません。

    T_PAAMAYIM_NEKUDOTAYIMではありません

**16.** よく知られている略語が許容されます（略語の意味をウィキペディアや検索エンジンで簡単に見つけることができる場合）。

    `AST`、`SQL`。

    `NVDH`（ランダムな文字の列）ではない

一般的な使用で短縮された単語は許容されます。

略語の意味がコメントで併記されている場合も使用できます。

**17.** C++ ソースコードのファイル名には `.cpp` 拡張子を使わなければなりません。ヘッダーファイルには `.h` 拡張子を使わなければなりません。

## コードの書き方 {#how-to-write-code}

**1.** メモリ管理。

手動でのメモリの解放（`delete`）はライブラリコードでのみ使用できます。

ライブラリコードでは、`delete` 演算子はデストラクタでのみ使用できます。

アプリケーションコードでは、メモリはその所有者であるオブジェクトが解放しなければなりません。

例：

- 最も簡単なのは、オブジェクトをスタックに置くか、他のクラスのメンバーにすることです。
- 小さなオブジェクトの多くを扱うためには、コンテナを使用します。
- ヒープにある少数のオブジェクトの自動解放のためには、`shared_ptr/unique_ptr` を使用します。

**2.** リソース管理。

`RAII` を使用し、上記を参照してください。

**3.** エラーハンドリング。

例外を使ってください。ほとんどの場合、例外を投げるだけでキャッチする必要はありません（`RAII` のため）。

オフラインデータ処理アプリケーションでは、例外をキャッチしないことがしばしば許容されます。

ユーザーリクエストを処理するサーバーでは、通常、接続ハンドラの最上位レベルで例外をキャッチするだけで十分です。

スレッド関数内では、すべての例外をキャッチして保持し、`join` の後にメインスレッドで再スローする必要があります。

``` cpp
/// 計算がまだ行われていない場合、最初のブロックを同期的に計算します
if (!started)
{
    calculate();
    started = true;
}
else /// 計算がすでに進行中である場合、結果を待ちます
    pool.wait();

if (exception)
    exception->rethrow();
```

例外を処理せずに無視しないでください。ただ例外をログに出力するだけの行為はしないでください。

``` cpp
//正しくありません
catch (...) {}
```

一部の例外を無視する必要がある場合は、特定のものに限定し、残りは再スローしてください。

``` cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

応答コードや `errno` を持つ関数を使用する場合、常に結果を確認し、エラーの場合は例外を投げます。

``` cpp
if (0 != close(fd))
    throw ErrnoException(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file {}", file_name);
```

コードの不変条件を確認するには、アサートを使用できます。

**4.** 例外の種類。

アプリケーションコードでの複雑な例外階層を使用する必要はありません。例外のテキストはシステム管理者に理解できるものであるべきです。

**5.** デストラクタからの例外の投げ。

これは推奨されませんが、許可されています。

次のオプションを使用してください：

- 例外を引き起こす可能性のあるすべての作業を事前に行う `done()` または `finalize()` という関数を作成します。その関数が呼ばれた場合、後のデストラクタに例外がないはずです。
- 余りに複雑なタスク（例えば、ネットワーク越しのメッセージの送信）は、クラスユーザーが破壊前に呼び出す必要がある別のメソッドに置くことができます。
- デストラクタで例外が発生した場合、（ロガーが利用可能であれば）隠すよりもログを残す方が良いです。
- 単純なアプリケーションでは、例外の処理に `std::terminate` に依存する（C++11のデフォルトの `noexcept` の場合）ことが許容されます。

**6.** 匿名コードブロック。

単一の関数内に別のコードブロックを作成して、特定の変数をローカルにし、ブロックを抜ける際にデストラクタを呼び出すことができます。

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

オフラインデータ処理プログラムでは：

- 単一の CPU コアで最高のパフォーマンスを得ることを目指してください。必要に応じてコードを並列化できます。

サーバーアプリケーションでは：

- リクエストを処理するためにスレッドプールを使用します。ユーザースペースのコンテキストスイッチを必要とするタスクはまだありません。

並列化のためにフォークは使用されていません。

**8.** スレッドの同期。

多くの場合、異なるスレッドが異なるメモリセルを使用するようにすることができ（さらに良いのは異なるキャッシュライン）、スレッドの同期を使用しないようにします（`joinAll` を除く）。

同期が必要な場合、ほとんどの場合、`lock_guard` でミューテックスを使用するだけで十分です。

他の場合は、システム同期プリミティブを使用します。ビジーウェイトを使用しないでください。

アトミック操作は最も簡単な場合にのみ使用してください。

専門的な領域でない限り、ロックフリーのデータ構造を実装しようとしないでください。

**9.** ポインタと参照。

ほとんどの場合、参照を使用してください。

**10.** `const`。

定数参照、定数へのポインタ、`const_iterator`、`const` メソッドを使用してください。

`const` をデフォルトと考え、必要な場合のみ非 `const` を使用します。

値渡しで変数を渡す場合、通常 `const` を使用する意味はありません。

**11.** unsigned。

必要であれば `unsigned` を使用してください。

**12.** 数値型。

型 `UInt8`、`UInt16`、`UInt32`、`UInt64`、`Int8`、`Int16`、`Int32`、`Int64` を使用してください。また `size_t`、`ssize_t`、`ptrdiff_t` も使用してください。

これらの型を数字には使用しないでください：`signed/unsigned long`、`long long`、`short`、`signed/unsigned char`、`char`。

**13.** 引数の渡し方。

値が複雑である場合、移動する場合は値渡しをし、`std::move` を使用します。値を更新したい場合は参照渡しをします。

ヒープに作成されたオブジェクトの所有権を関数が取得する場合、引数の型を `shared_ptr` または `unique_ptr` にします。

**14.** 戻り値。

ほとんどの場合、ただ `return` を使用してください。`return std::move(res)` と書かないでください。

関数がヒープにオブジェクトを割り当ててそれを返す場合、`shared_ptr` または `unique_ptr` を使用してください。

まれに（ループ中に値を更新する場合）、引数を通じて値を返す必要があるかもしれません。この場合、引数は参照であるべきです。

``` cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** 名前に基づいて集計関数を作成できるようにします。
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** `namespace`。

アプリケーションコードには個別の `namespace` を使用する必要はありません。

小さなライブラリにも必要ありません。

中大規模のライブラリはすべてを `namespace` に置きます。

ライブラリの `.h` ファイルでは、実装の詳細を隠すために `namespace detail` を使用することができます。

`.cpp` ファイルでは、シンボルを隠すために `static` または匿名の `namespace` を使用できます。

別途外部 `namespace` に出さないため `enum` に `namespace` を使用することもできます（ただし `enum class` を使用する方が良いです）。

**16.** 遅延初期化。

初期化に引数が必要な場合、通常デフォルトコンストラクタを書くべきではありません。

後で初期化を遅らせる必要がある場合、無効なオブジェクトを作成するデフォルトコンストラクタを追加できます。または、少数のオブジェクトについては `shared_ptr/unique_ptr` を使用することができます。

``` cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// 遅延初期化のために
Loader() {}
```

**17.** 仮想関数。

クラスがポリモーフィックな使用を意図していなければ、関数を仮想化する必要はありません。これはデストラクタにもあてはまります。

**18.** エンコーディング。

どこでも UTF-8 を使用してください。`std::string` と `char *` を使用してください。`std::wstring` と `wchar_t` は使用しないでください。

**19.** ロギング。

コードの至るところにある例を参照してください。

コミットする前に、意味のないログ、デバッグログ、その他の種類のデバッグ出力を削除してください。

サイクル中のロギングは、トレースレベルでも避けるべきです。

ログはどのログレベルでも読みやすくなければなりません。

基本的には、アプリケーションコードでのみロギングを行います。

ログメッセージは英語で書かれなければなりません。

ログはシステム管理者が理解できるものであるべきです。

ログに不適切な言葉を使わないでください。

ログには UTF-8 エンコードを使用します。まれに、ログに非ASCII文字を使用することが許容されます。

**20.** 入出力。

アプリケーションの性能にとって重要な内部サイクルでは `iostreams` を使用しないでください（そして `stringstream` は一切使用しないでください）。

代わりに `DB/IO` ライブラリを使用してください。

**21.** 日付と時刻。

`DateLUT` ライブラリを参照してください。

**22.** include。

インクルードガードの代わりに常に `#pragma once` を使用してください。

**23.** using。

`using namespace` は使用されません。特定のものについては `using` を使用できます。でもクラスや関数内でローカルにしてください。

**24.** 必要でない限り、関数に `trailing return type` を使用しないでください。

``` cpp
auto f() -> void
```

**25.** 変数の宣言と初期化。

``` cpp
//正しい方法
std::string s = "Hello";
std::string s{"Hello"};

//誤った方法
auto s = std::string{"Hello"};
```

**26.** 仮想関数の場合、基底クラスで `virtual` を、派生クラスでは `virtual` の代わりに `override` を書いてください。

## 未使用の C++ 機能 {#unused-features-of-c}

**1.** 仮想継承は使用されていません。

**2.** 最新の C++ で便利なシンタックスシュガーを持つ構造体、例えば：

```
// シンタックスシュガーのない従来の方法
template <typename G, typename = std::enable_if_t<std::is_same<G, F>::value, void>> // SFINAE を std::enable_if で使用、 ::value の使用

std::pair<int, int> func(const E<G> & e) // 返り値型を明示指定
{
    if (elements.count(e)) // .count() メンバーシップテスト
    {
        // ...
    }

    elements.erase(
        std::remove_if(
            elements.begin(), elements.end(),
            [&](const auto x){
                return x == 1;
            }),
        elements.end()); // remove-erase イディオム

    return std::make_pair(1, 2); // make_pair() でペアを作成
}

// シンタックスシュガーを使用した方法 (C++14/17/20)
template <typename G>
requires std::same_v<G, F> // SFINAE を C++20 のコンセプトで、C++14 テンプレートエイリアスの使用
auto func(const E<G> & e) // auto 返り値型 (C++14)
{
    if (elements.contains(e)) // C++20 .contains メンバーシップテスト
    {
        // ...
    }

    elements.erase_if(
        elements,
        [&](const auto x){
            return x == 1;
        }); // C++20 std::erase_if

    return {1, 2}; // または： return std::pair(1, 2); // 初期化リストや値初期化 (C++17) でペアを作成
}
```

## プラットフォーム {#platform}

**1.** 特定のプラットフォーム用にコードを書いています。

しかし、他の条件が同じであれば、クロスプラットフォームまたは移植可能なコードが好まれます。

**2.** 言語: C++20（使用可能な [C++20 機能](https://en.cppreference.com/w/cpp/compiler_support#C.2B.2B20_features) のリストを参照してください）。

**3.** コンパイラ: `clang`。執筆時（2022年7月現在）、コードはクランバージョン >= 12 を使用してコンパイルされています。（`gcc` でもコンパイルできますが、テストされておらず、本番使用には適していません）。

標準ライブラリは「`libc++`」を使用します。

**4.**OS: Linux Ubuntu、Precise以降。

**5.** コードは x86_64 CPU アーキテクチャ用に書かれています。

CPU 命令セットはサーバーの中でサポートされている最小のセットです。現在は SSE 4.2 です。

**6.** 許可されているいくつかの例外を除いて `-Wall -Wextra -Werror -Weverything` コンパイルフラグを使用します。

**7.** 静的リンクを、静的に接続するのが難しいライブラリを除いて、すべてのライブラリで使用します（`ldd` コマンドの出力を参照してください）。

**8.** コードはリリース設定で開発およびデバッグされています。

## ツール {#tools}

**1.** KDevelop は良い IDE です。

**2.** デバッグには `gdb`、`valgrind`（`memcheck`）、`strace`、`-fsanitize=...`、`tcmalloc_minimal_debug` を使用します。

**3.** プロファイリングには `Linux Perf`、`valgrind`（`callgrind`）、`strace -cf` を使用します。

**4.** ソースは Git にあります。

**5.** 組み立てには `CMake` を使用します。

**6.** プログラムは `deb` パッケージを用いてリリースされます。

**7.** マスタへのコミットはビルドを壊してはいけません。

ただし、選択されたリビジョンのみが動作可能と見なされます。

**8.** コードは完全に準備が整っていなくても、できるだけ頻繁にコミットしてください。

この目的のためにブランチを使用します。

`master` ブランチでのコードがまだビルド可能でない場合、`push` の前にそれをビルドから除外してください。数日以内に完成させるか、削除する必要があります。

**9.** 非自明な変更にはブランチを使用し、サーバーに公開してください。

**10.** 使用されていないコードはリポジトリから削除します。

## ライブラリ {#libraries}

**1.** C++20 標準ライブラリ（エクスペリメンタルな拡張を含む）、`boost`、`Poco` フレームワークが使用されています。

**2.** OS パッケージからのライブラリの使用は許可されていません。また、事前にインストールされたライブラリの使用も許可されていません。すべてのライブラリは `contrib` ディレクトリにソースコードの形で配置され、ClickHouse と共にビルドされなければなりません。詳細については [新しいサードパーティライブラリの追加に関するガイドライン](contrib.md#adding-third-party-libraries) を参照してください。

**3.** 常に既に使用されているライブラリを優先します。

## 一般的な推奨事項 {#general-recommendations-1}

**1.** 可能な限り少ないコードを書くようにします。

**2.** 最も簡単な解決策を試みます。

**3.** 動作と内部ループの動作がわかるまでコードを書かないでください。

**4.** 単純なケースでは `using` を使用してクラスや構造体の代わりにします。

**5.** コピコンストラクタ、割り当て演算子、デストラクタ（クラスが少なくとも1つの仮想関数を含む場合の仮想デストラクタを除いて）、ムーブコンストラクタまたはムーブ割り当て演算子を可能な限り書かないでください。言い換えれば、コンパイラが生成する関数が正しく動作するはずです。`default` を使用できます。

**6.** コードの簡素化が奨励されます。可能な場合、コードのサイズを縮小します。

## 追加の推奨事項 {#additional-recommendations}

**1.** `stddef.h` からの型に `std::` を明示的に指定することは推奨されません。言い換えれば、`std::size_t` ではなく、短い方の `size_t` と書くことをお勧めします。

`std::` を追加することは許容されます。

**2.** 標準Cライブラリの関数に `std::` を明示的に指定することは推奨されません。言い換えれば、`memcpy` と書きます（`std::memcpy` ではない）。

理由は、標準ではない類似関数、例えば `memmem` が存在するためです。これらの関数は `namespace std` に存在しません。

どこにでも `memcpy` の代わりに `std::memcpy` と記述するなら、`memmem` を `std::` なしで記述するのは奇妙に見えるでしょう。

それにもかかわらず、`std::` が好まされれば、それを使用できます。

**3.** 標準C++ライブラリに同じものがある場合に C の関数を使用すること。

これはより効率的である場合には受け入れられます。

例えば、大きなメモリチャンクのコピーには `memcpy` を使用します（`std::copy` より）。

**4.** 複数行の関数引数。

次のような折り返しスタイルが許容されます：

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

