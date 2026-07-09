---
description: 'Documentação da instrução EXISTS'
sidebar_label: 'EXISTS'
sidebar_position: 45
slug: /sql-reference/statements/exists
title: 'Instrução EXISTS'
doc_type: 'reference'
---

```sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Retorna uma única coluna do tipo `UInt8`, que contém o valor `0` se a tabela ou o banco de dados não existir, ou `1` se a tabela existir no banco de dados especificado.