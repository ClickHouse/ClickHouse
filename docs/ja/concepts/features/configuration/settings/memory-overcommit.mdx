---
description: 'クエリに対して、より柔軟なメモリ制限を設定できるようにすることを目的とした実験的な手法です。'
slug: /operations/settings/memory-overcommit
title: 'メモリオーバーコミット'
doc_type: 'reference'
---

メモリオーバーコミットは、クエリに対してより柔軟なメモリ制限を設定できるようにすることを目的とした実験的な手法です。

この手法の考え方は、クエリが使用できる保証済みのメモリ量を表す設定を導入することです。
メモリオーバーコミットが有効で、メモリ制限に達すると、ClickHouse は最もオーバーコミットしているクエリを選択し、そのクエリを強制終了してメモリを解放しようとします。

メモリ制限に達すると、どのクエリも新たなメモリを割り当てようとする際に、一定時間待機します。
タイムアウトまでにメモリが解放されれば、そのクエリは実行を継続します。
そうでない場合は例外がスローされ、クエリは強制終了されます。

停止または強制終了するクエリの選択は、どのメモリ制限に達したかに応じて、グローバルオーバーコミットトラッカーまたはユーザーオーバーコミットトラッカーによって行われます。
オーバーコミットトラッカーが停止するクエリを選択できない場合は、MEMORY&#95;LIMIT&#95;EXCEEDED 例外がスローされます。

<div id="user-overcommit-tracker">
  ## ユーザー オーバーコミットトラッカー
</div>

ユーザー オーバーコミットトラッカー は、ユーザーのクエリ一覧から overcommit ratio が最も大きいクエリを見つけます。
クエリの overcommit ratio は、割り当てられたバイト数を `memory_overcommit_ratio_denominator_for_user` 設定の値で割って計算されます。

クエリの `memory_overcommit_ratio_denominator_for_user` が 0 の場合、オーバーコミットトラッカー はそのクエリを選択しません。

Waiting timeout は `memory_usage_overcommit_max_wait_microseconds` 設定で指定します。

**例**

```sql
SELECT number FROM numbers(1000) GROUP BY number SETTINGS memory_overcommit_ratio_denominator_for_user=4000, memory_usage_overcommit_max_wait_microseconds=500
```

<div id="global-overcommit-tracker">
  ## グローバル overcommit トラッカー
</div>

グローバル overcommit トラッカーは、すべてのクエリの一覧から、overcommit ratio が最も大きいクエリを見つけます。
この場合、overcommit ratio は、割り当てられたバイト数を `memory_overcommit_ratio_denominator` 設定の値で割って計算されます。

クエリの `memory_overcommit_ratio_denominator` が 0 の場合、overcommit トラッカーはそのクエリを選択しません。

Waiting timeout は、設定ファイル内の `memory_usage_overcommit_max_wait_microseconds` パラメータで設定します。