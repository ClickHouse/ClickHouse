---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'direct dictionary layout'
sidebar_label: 'direct'
sidebar_position: 9
description: 'A dictionary layout that queries the source directly without caching.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## direct {#direct}

The dictionary is not stored in memory and directly goes to the source during the processing of a request.

The dictionary key has the [UInt64](/sql-reference/data-types/int-uint.md) type.

All types of [sources](../sources/#dictionary-sources), except local files, are supported.

Configuration example:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
LAYOUT(DIRECT())
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<layout>
  <direct />
</layout>
```

</TabItem>
</Tabs>
<br/>

## complex_key_direct {#complex_key_direct}

This type of storage is for use with composite [keys](../attributes.md#composite-key). Similar to `direct`.
