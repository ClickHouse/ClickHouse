---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 36
toc_title: "\u6BD4\u8F03"
---

# 比較関数 {#comparison-functions}

比較関数は常に0または1(uint8)を返します。

次のタイプを比較できます:

-   数字
-   文字列と固定文字列
-   日付
-   時間のある日付

各グループ内ではなく、異なるグループ間。

たとえば、日付と文字列を比較することはできません。 文字列を日付に変換するには関数を使用する必要があります。

文字列はバイトで比較されます。 短い文字列は、それで始まり、少なくとも一つ以上の文字を含むすべての文字列よりも小さくなります。

## 等号、a=bおよびa===b演算子 {#function-equals}

## notEquals,a! 演算子=bとa\<\>b {#function-notequals}

## 演算子 {#function-less}

## より大きい、\>演算子 {#function-greater}

## リース、\<=演算子 {#function-lessorequals}

## greaterOrEquals,\>=演算子 {#function-greaterorequals}

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/comparison_functions/) <!--hide-->
