---
description: 'Documentação sobre a instrução USE'
sidebar_label: 'USE'
sidebar_position: 53
slug: /sql-reference/statements/use
title: 'Instrução USE'
doc_type: 'reference'
---

```sql
USE [DATABASE] db
```

Permite definir o banco de dados atual da sessão.

O banco de dados atual é usado para localizar tabelas quando o banco de dados não é definido explicitamente na consulta com um ponto antes do nome da tabela.

Essa consulta não pode ser feita ao usar o protocolo HTTP, pois não existe o conceito de sessão.