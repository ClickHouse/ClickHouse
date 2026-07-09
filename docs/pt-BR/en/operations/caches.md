---
description: 'Ao executar consultas, o ClickHouse usa diferentes caches.'
sidebar_label: 'Caches'
sidebar_position: 65
slug: /operations/caches
title: 'Tipos de cache'
keywords: ['cache']
doc_type: 'reference'
---

Ao executar consultas, o ClickHouse usa diferentes caches para acelerá-las
e reduzir a necessidade de ler do disco ou gravar nele.

Os principais tipos de cache são:

* `mark_cache` — Cache de [marcas](/pt-BR/development/architecture#merge-tree) usado pelos motores de tabela da família [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* `uncompressed_cache` — Cache de dados não comprimidos usado pelos motores de tabela da família [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* Cache de páginas do sistema operacional (usado indiretamente para arquivos com dados reais).

Também há diversos tipos adicionais de cache:

* Cache de DNS.
* Cache de [Regexp](/pt-BR/interfaces/formats/Regexp).
* Cache de expressões compiladas.
* Cache de [índice de similaridade vetorial](../engines/table-engines/mergetree-family/annindexes.md).
* Cache de [índice de texto](../engines/table-engines/mergetree-family/textindexes.md#caching).
* Cache de esquemas do [formato Avro](/pt-BR/interfaces/formats/Avro).
* Cache de dados de [Dicionários](../sql-reference/statements/create/dictionary/overview.md).
* Cache de inferência de esquema.
* [Cache do sistema de arquivos](storing-data.md) em S3, Azure, Local e outros disks.
* [Cache de páginas em espaço de usuário](/pt-BR/operations/userspace-page-cache)
* [Cache de consultas](query-cache.md).
* [Cache de condições de consulta](query-condition-cache.md).
* Cache de esquema de formato.

Se quiser limpar um dos caches, por motivos de ajuste de desempenho, solução de problemas ou consistência de dados,
você pode usar a instrução [`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md).