---
slug: /ja/sql-reference/functions/nlp-functions
sidebar_position: 130
sidebar_label: NLP (エクスペリメンタル)
---

# 自然言語処理 (NLP) 関数

:::warning
これは現在開発中のエクスペリメンタルな機能であり、一般的な利用にはまだ準備が整っていません。将来のリリースでは予測不可能な互換性のない変更が行われることがあります。有効にするには `allow_experimental_nlp_functions = 1` を設定してください。
:::

## detectCharset

`detectCharset` 関数は非UTF8エンコードの入力文字列の文字セットを検出します。

*構文*

``` sql
detectCharset('text_to_be_analyzed')
```

*引数*

- `text_to_be_analyzed` — 分析する文字列のコレクション（または文）。[String](../data-types/string.md#string).

*返される値*

- 検出された文字セットのコードを含む `String`

*例*

クエリ:

```sql
SELECT detectCharset('Ich bleibe für ein paar Tage.');
```

結果:

```response
┌─detectCharset('Ich bleibe für ein paar Tage.')─┐
│ WINDOWS-1252                                   │
└────────────────────────────────────────────────┘
```

## detectLanguage

UTF8エンコードされた入力文字列の言語を検出します。関数は検出に[CLD2ライブラリ](https://github.com/CLD2Owners/cld2)を使用し、2文字のISO言語コードを返します。

`detectLanguage` 関数は、入力文字列内に200文字以上を提供すると最適に動作します。

*構文*

``` sql
detectLanguage('text_to_be_analyzed')
```

*引数*

- `text_to_be_analyzed` — 分析する文字列のコレクション（または文）。[String](../data-types/string.md#string).

*返される値*

- 検出された言語の2文字のISOコード

その他の可能な結果:

- `un` = 不明、言語を検出できない。
- `other` = 検出された言語に2文字のコードがない。

*例*

クエリ:

```sql
SELECT detectLanguage('Je pense que je ne parviendrai jamais à parler français comme un natif. Where there’s a will, there’s a way.');
```

結果:

```response
fr
```

## detectLanguageMixed

`detectLanguage` 関数に似ていますが、`detectLanguageMixed` は特定の言語の割合がテキスト内でどれだけ存在するかを示す2文字の言語コードの `Map` を返します。

*構文*

``` sql
detectLanguageMixed('text_to_be_analyzed')
```

*引数*

- `text_to_be_analyzed` — 分析する文字列のコレクション（または文）。[String](../data-types/string.md#string).

*返される値*

- `Map(String, Float32)`: キーが2文字のISOコードで、値がその言語のテキスト内の割合

*例*

クエリ:

```sql
SELECT detectLanguageMixed('二兎を追う者は一兎をも得ず二兎を追う者は一兎をも得ず A vaincre sans peril, on triomphe sans gloire.');
```

結果:

```response
┌─detectLanguageMixed()─┐
│ {'ja':0.62,'fr':0.36  │
└───────────────────────┘
```

## detectProgrammingLanguage

ソースコードからプログラミング言語を判別します。ソースコード内のコマンドのユニグラムとバイグラムをすべて計算します。その後、さまざまなプログラミング言語のコマンドのユニグラムとバイグラムの重みを持つマークアップされたDictionaryを使用して、最も重みのあるプログラミング言語を見つけて返します。

*構文*

``` sql
detectProgrammingLanguage('source_code')
```

*引数*

- `source_code` — 分析するソースコードの文字列表現。[String](../data-types/string.md#string).

*返される値*

- プログラミング言語。[String](../data-types/string.md).

*例*

クエリ:

```sql
SELECT detectProgrammingLanguage('#include <iostream>');
```

結果:

```response
┌─detectProgrammingLanguage('#include <iostream>')─┐
│ C++                                              │
└──────────────────────────────────────────────────┘
```

## detectLanguageUnknown

`detectLanguage` 関数に似ていますが、`detectLanguageUnknown` 関数は非UTF8エンコードの文字列で動作します。文字セットがUTF-16またはUTF-32の場合、このバージョンを使用してください。

*構文*

``` sql
detectLanguageUnknown('text_to_be_analyzed')
```

*引数*

- `text_to_be_analyzed` — 分析する文字列のコレクション（または文）。[String](../data-types/string.md#string).

*返される値*

- 検出された言語の2文字のISOコード

その他の可能な結果:

- `un` = 不明、言語を検出できない。
- `other` = 検出された言語に2文字のコードがない。

*例*

クエリ:

```sql
SELECT detectLanguageUnknown('Ich bleibe für ein paar Tage.');
```

結果:

```response
┌─detectLanguageUnknown('Ich bleibe für ein paar Tage.')─┐
│ de                                                     │
└────────────────────────────────────────────────────────┘
```

## detectTonality

テキストデータの感情を判定します。マークアップされた感情Dictionaryを使用し、各単語には `-12` から `6` までのトナリティがあります。各テキストについて、その単語の平均感情値を計算し、範囲 `[-1,1]` でそれを返します。

:::note
この関数は現段階では制限されています。現在は `/contrib/nlp-data/tonality_ru.zst` に埋め込まれた感情Dictionaryを使用し、ロシア語のみに対応しています。
:::

*構文*

``` sql
detectTonality(text)
```

*引数*

- `text` — 分析するテキスト。[String](../data-types/string.md#string).

*返される値*

- `text` 内の単語の平均感情値。[Float32](../data-types/float.md).

*例*

クエリ:

```sql
SELECT detectTonality('Шарик - хороший пёс'), -- シャリクは良い犬です
       detectTonality('Шарик - пёс'), -- シャリクは犬です
       detectTonality('Шарик - плохой пёс'); -- シャリクは悪い犬です
```

結果:

```response
┌─detectTonality('Шарик - хороший пёс')─┬─detectTonality('Шарик - пёс')─┬─detectTonality('Шарик - плохой пёс')─┐
│                               0.44445 │                             0 │                                 -0.3 │
└───────────────────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
```
## lemmatize

与えられた単語のレンマタイゼーションを行います。動作にはDictionariesが必要であり、[こちら](https://github.com/vpodpecan/lemmagen3/tree/master/src/lemmagen3/models)から入手できます。

*構文*

``` sql
lemmatize('language', word)
```

*引数*

- `language` — 適用するルールの言語。[String](../data-types/string.md#string).
- `word` — レンマ化する必要のある単語。小文字である必要があります。[String](../data-types/string.md#string).

*例*

クエリ:

``` sql
SELECT lemmatize('en', 'wolves');
```

結果:

``` text
┌─lemmatize("wolves")─┐
│              "wolf" │
└─────────────────────┘
```

*設定*

この設定は、英語（`en`）の単語のレンマタイゼーションに `en.bin` のDictionaryを使用することを指定します。 `.bin` ファイルは[こちら](https://github.com/vpodpecan/lemmagen3/tree/master/src/lemmagen3/models)からダウンロードできます。

``` xml
<lemmatizers>
    <lemmatizer>
        <!-- highlight-start -->
        <lang>en</lang>
        <path>en.bin</path>
        <!-- highlight-end -->
    </lemmatizer>
</lemmatizers>
```

## stem

与えられた単語のステミングを行います。

*構文*

``` sql
stem('language', word)
```

*引数*

- `language` — 適用するルールの言語。2文字の[ISO 639-1コード](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes)を使用します。
- `word` — ステミングする必要のある単語。小文字である必要があります。[String](../data-types/string.md#string).

*例*

クエリ:

``` sql
SELECT arrayMap(x -> stem('en', x), ['I', 'think', 'it', 'is', 'a', 'blessing', 'in', 'disguise']) as res;
```

結果:

``` text
┌─res────────────────────────────────────────────────┐
│ ['I','think','it','is','a','bless','in','disguis'] │
└────────────────────────────────────────────────────┘
```
*stem()でサポートされている言語*

:::note
stem() 関数は [Snowballステミング](https://snowballstem.org/) ライブラリを使用します。最新の言語情報などはSnowballのウェブサイトを参照してください。
:::

- アラビア語
- アルメニア語
- バスク語
- カタルーニャ語
- デンマーク語
- オランダ語
- 英語
- フィンランド語
- フランス語
- ドイツ語
- ギリシャ語
- ヒンディー語
- ハンガリー語
- インドネシア語
- アイルランド語
- イタリア語
- リトアニア語
- ネパール語
- ノルウェー語
- ポーター
- ポルトガル語
- ルーマニア語
- ロシア語
- セルビア語
- スペイン語
- スウェーデン語
- タミル語
- トルコ語
- イディッシュ語

## synonyms

指定された単語の同義語を見つけます。`plain` と `wordnet` の2種類の同義語拡張があります。

`plain` 拡張タイプでは、シンプルなテキストファイルへのパスを提供する必要があります。このファイルの各行は特定の同義語セットに対応し、その行内の単語はスペースまたはタブ文字で区切られている必要があります。

`wordnet` 拡張タイプでは、WordNetシソーラスが含まれているディレクトリへのパスを提供する必要があります。シソーラスにはWordNetのセンスインデックスが含まれている必要があります。

*構文*

``` sql
synonyms('extension_name', word)
```

*引数*

- `extension_name` — 検索が実行される拡張子の名前。[String](../data-types/string.md#string).
- `word` — 拡張子内で検索される単語。[String](../data-types/string.md#string).

*例*

クエリ:

``` sql
SELECT synonyms('list', 'important');
```

結果:

``` text
┌─synonyms('list', 'important')────────────┐
│ ['important','big','critical','crucial'] │
└──────────────────────────────────────────┘
```

*設定*
``` xml
<synonyms_extensions>
    <extension>
        <name>en</name>
        <type>plain</type>
        <path>en.txt</path>
    </extension>
    <extension>
        <name>en</name>
        <type>wordnet</type>
        <path>en/</path>
    </extension>
</synonyms_extensions>
```
