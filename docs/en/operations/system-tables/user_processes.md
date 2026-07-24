---
description: 'System table containing information useful for an overview of memory
  usage and ProfileEvents of users.'
keywords: ['system table', 'user_processes']
slug: /operations/system-tables/user_processes
title: 'system.user_processes'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.user_processes

<SystemTableCloud/>

This system table can be used to get overview of memory usage and ProfileEvents of users.

Columns:

- `user` ([String](../../sql-reference/data-types/string.md)) — User name.
- `memory_usage` ([Int64](/sql-reference/data-types/int-uint#integer-ranges)) – Sum of RAM used by all processes of the user. It might not include some types of dedicated memory. See the [max_memory_usage](/operations/settings/settings#max_memory_usage) setting.
- `peak_memory_usage` ([Int64](/sql-reference/data-types/int-uint#integer-ranges)) — The peak of memory usage of the user. It can be reset when no queries are run for the user.
- `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/map)) – Summary of ProfileEvents that measure different metrics for the user. The description of them could be found in the table [system.events](/operations/system-tables/events)

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
