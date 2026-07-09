---
description: 'Documentação do CREATE NAMED COLLECTION'
sidebar_label: 'NAMED COLLECTION'
slug: /sql-reference/statements/create/named-collection
title: 'CREATE NAMED COLLECTION'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="create-named-collection">
  # CREATE NAMED COLLECTION
</div>

Cria uma nova coleção nomeada.

**Sintaxe**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

**Exemplo**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**Instruções relacionadas**

* [CREATE NAMED COLLECTION](/pt-BR/sql-reference/statements/alter/named-collection)
* [DROP NAMED COLLECTION](/pt-BR/sql-reference/statements/drop#drop-function)

**Veja também**

* [Guia de coleções nomeadas](/pt-BR/operations/named-collections.md)