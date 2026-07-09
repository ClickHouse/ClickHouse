---
description: 'ClickHouse server より先に Linux の OOM キラーの対象となる身代わりの子プロセスで、サーバーが負荷を軽減して生き残るための猶予を与えます。'
sidebar_label: 'OOM canary'
sidebar_position: 60
slug: /operations/settings/oom-canary
title: 'OOM canary'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge />

:::note
OOM canary は実験的機能であり、デフォルトでは無効になっています。本番環境での検証が完了するまで、ClickHouse のバージョンによって動作が変更される可能性があります。
:::

<div id="overview">
  ## 概要
</div>

ホストまたはメモリ cgroup のメモリが不足すると、Linux の OOM (out-of-memory)
キラーが `SIGKILL` でプロセスを強制終了します。通常は最も多くメモリを消費しているプロセスが対象となり、
専用ホストでは多くの場合 `clickhouse-server` 自体です。すると、サーバーは
復旧する間もなく、プロセス全体が失われます。

OOM canary は、先に終了される対象を変えます。小さな *犠牲用* の子プロセスを実行し、
そのプロセス自身を OOM の標的として最も選ばれやすい状態にすることで、
カーネルがサーバーではなくその子プロセスを終了させるようにします。するとサーバーはその終了を検知し、
それが OOM イベントだったことを確認したうえで、メモリ負荷を軽減し、自身を存続させることができます。

canary はメモリ制限を引き上げるものではなく、適切な制限設定の代わりにもなりません
([メモリオーバーコミット](/ja/operations/settings/memory-overcommit) および
`max_server_memory_usage` を参照) 。これは最後の防御線であり、少量の固定メモリと引き換えに、
一時的なメモリ急増を乗り切れる可能性を得るための仕組みです。

<div id="how-it-works">
  ## 仕組み
</div>

canary は独立した `clickhouse oom-canary` プロセスです。自身の
`oom_score_adj` を最大値 (`1000`) に設定してカーネルが最初にこれを対象にするようにしたうえで、
`oom_canary_size` バイト (デフォルトは 100 MB) を割り当て、実際にアクセスし、`mlock` して、
resident set が実体を持つようにします。サーバーが終了すると自動的に kill されます。

サーバーでは、監視スレッドが canary を (`pidfd` 経由で) 監視し、
canary が停止すると次のように動作します。

* cgroup OOM の証拠**あり**で `SIGKILL` により kill された場合 → OOM 応答を実行し、その後
  新しい canary を再起動します。
* OOM の証拠**なし**で kill された場合 (たとえば手動の `kill -9`) 、または
  一時的な障害で終了した場合 → 応答は実行せず、再起動のみ行います。
* 永続的なセットアップ失敗、またはサーバーのシャットダウン時 → canary は自身を無効化します。

OOM の証拠として使われるのは、cgroup v2 の `memory.events.local` の `oom_kill`
カウンターだけです。これは意図的に cgroup ローカルに限定されています。階層的なカウンターやホスト全体のカウンターは
無関係なプロセスによって増えることがあり、誤った応答をトリガーしかねないためです。

OOM が確認されると、応答では次の独立した手順が実行されます。`FATAL`
メッセージの記録、アロケータ (jemalloc) アリーナの purge、実行中のすべての
クエリのベストエフォートでのキャンセル、すべてのマージとミューテーションのキャンセル、そして
[`system.crash_log`](/ja/operations/system-tables/crash_log) へのイベントのキューイングです。システムログは
同期的には flush されません。メモリ逼迫下で I/O を強制すると、状況が悪化するおそれがあるためです。

<div id="requirements">
  ## 要件
</div>

* **Linux ≥ 5.3.** モニタは `pidfd_open` を介して canary を管理します。これより古いカーネルでは、
  canary は起動時に自動的に無効化されます。非 Linux プラットフォームでは何もしません。
* **OOM 対応には `memory.events.local` を備えた cgroup v2 が必要です。** これがない場合でも
  canary は `SIGKILL` の後に再起動しますが、OOM を確認できないため、
  対応処理は実行されません (起動時に警告が記録されます) 。
* **`mlock` capability (任意) 。** canary のメモリをロックするには
  `CAP_IPC_LOCK` または十分な `RLIMIT_MEMLOCK` が必要です。これに失敗すると、canary は
  警告を記録し、そのメモリがスワップアウトされる可能性があるため、
  OOM 対象としての有効性が低下します。

:::warning memory.oom.group
サーバーの cgroup で cgroup v2 の `memory.oom.group` が有効になっている場合、カーネルは
OOM 時に cgroup 全体を 1 つの単位として強制終了します。つまり、サーバーは
canary とともに停止し、対応処理は実行されません。このモードでは canary はサーバーを保護できません。
起動時に警告が記録されます。
:::

<div id="configuration">
  ## 設定
</div>

canary は、サーバー構成の最上位要素として設定され、再起動後に適用される[サーバー設定](/ja/operations/server-configuration-parameters/settings)によって制御されます。

| 設定                                   | デフォルト                | 説明                                                                                                                |
| ------------------------------------ | -------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `oom_canary_enable`                  | `false`              | OOM canary を有効にします。                                                                                               |
| `oom_canary_size`                    | `104857600` (100 MB) | canary が確保してアクセスするバイト数です。値を大きくすると、OOM 時に終了対象として選ばれやすくなります。                                                        |
| `oom_canary_relaunch`                | `true`               | canary の終了後に再起動します (恒久的なセットアップ失敗またはシャットダウンの場合を除く) 。再起動には以下の制限が適用されます。                                             |
| `oom_canary_max_rapid_relaunches`    | `10`                 | 自動再起動が無効になるまでの、短時間での連続再起動の最大回数です。過度な再起動ループを防ぐためのものです。canary が `oom_canary_max_backoff_seconds` を超えて稼働するとリセットされます。 |
| `oom_canary_initial_backoff_seconds` | `1`                  | 再起動間の初期遅延です。最大値に達するまで、再起動のたびに 2 倍になります。                                                                           |
| `oom_canary_max_backoff_seconds`     | `60`                 | 再起動間の最大遅延です。                                                                                                      |

```xml
<clickhouse>
    <oom_canary_enable>1</oom_canary_enable>
    <oom_canary_size>104857600</oom_canary_size>
</clickhouse>
```

<div id="observability">
  ## オブザーバビリティ
</div>

確認された OOM が発生すると、
[`system.crash_log`](/ja/operations/system-tables/crash_log) に `signal = 9` の
行が記録され、`signal_description` には `OOM Canary` への言及が含まれます:

```sql
SELECT event_time, signal, signal_description
FROM system.crash_log
WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'
ORDER BY event_time DESC;
```

canaryのライフサイクルと、OOM発生時の各対応手順もサーバーログに記録されます。