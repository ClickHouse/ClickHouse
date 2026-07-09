---
description: 'Documentação sobre Arquivos'
sidebar_label: 'Arquivos'
slug: /sql-reference/functions/files
title: 'Arquivos'
doc_type: 'reference'
---

<div id="file">
  ## file
</div>

Lê um arquivo como String e carrega os dados na coluna especificada. O conteúdo do arquivo não é interpretado.

Veja também a função de tabela [file](../table-functions/file.md).

**Sintaxe**

```sql
file(path[, default])
```

**Argumentos**

* `path` — O caminho do arquivo relativo a [user&#95;files&#95;path](../../operations/server-configuration-parameters/settings.md#user_files_path). Suporta curingas `*`, `**`, `?`, `{abc,def}` e `{N..M}`, em que `N` e `M` são números e `'abc'` e `'def'` são strings.
* `default` — O valor retornado se o arquivo não existir ou não puder ser acessado. Tipos de dados suportados: [String](../data-types/string.md) e [NULL](/pt-BR/operations/settings/formats#input_format_null_as_default).

**Exemplo**

Inserção de dados dos arquivos a.txt e b.txt em uma tabela como Strings:

```sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```