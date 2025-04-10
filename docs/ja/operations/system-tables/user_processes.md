---
slug: /ja/operations/system-tables/user_processes
---
# user_processes

このシステムテーブルは、ユーザーのメモリ使用量とProfileEventsの概要を取得するために利用できます。

カラム:

- `user` ([String](../../sql-reference/data-types/string.md)) — ユーザー名。
- `memory_usage` ([Int64](../../sql-reference/data-types/int-uint#int-ranges)) – ユーザーの全プロセスによって使用されているRAMの合計。一部の専用メモリのタイプを含まない場合があります。[max_memory_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage) 設定を参照してください。
- `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint#int-ranges)) — ユーザーのメモリ使用量のピーク。この値は、ユーザーのクエリが実行されない状態でリセットされることがあります。
- `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/map)) – ユーザーの異なるメトリクスを計測するProfileEventsの概要。それらの説明は、テーブル[system.events](../../operations/system-tables/events.md#system_tables-events)で確認できます。

```sql
SELECT * FROM system.user_processes LIMIT 10 FORMAT Vertical;
```

```response
Row 1:
──────
user:              default
memory_usage:      9832
peak_memory_usage: 9832
ProfileEvents:     {'Query':5,'SelectQuery':5,'QueriesWithSubqueries':38,'SelectQueriesWithSubqueries':38,'QueryTimeMicroseconds':842048,'SelectQueryTimeMicroseconds':842048,'ReadBufferFromFileDescriptorRead':6,'ReadBufferFromFileDescriptorReadBytes':234,'IOBufferAllocs':3,'IOBufferAllocBytes':98493,'ArenaAllocChunks':283,'ArenaAllocBytes':1482752,'FunctionExecute':670,'TableFunctionExecute':16,'DiskReadElapsedMicroseconds':19,'NetworkSendElapsedMicroseconds':684,'NetworkSendBytes':139498,'SelectedRows':6076,'SelectedBytes':685802,'ContextLock':1140,'RWLockAcquiredReadLocks':193,'RWLockReadersWaitMilliseconds':4,'RealTimeMicroseconds':1585163,'UserTimeMicroseconds':889767,'SystemTimeMicroseconds':13630,'SoftPageFaults':1947,'OSCPUWaitMicroseconds':6,'OSCPUVirtualTimeMicroseconds':903251,'OSReadChars':28631,'OSWriteChars':28888,'QueryProfilerRuns':3,'LogTrace':79,'LogDebug':24}

1 row in set. Elapsed: 0.010 sec.
```
