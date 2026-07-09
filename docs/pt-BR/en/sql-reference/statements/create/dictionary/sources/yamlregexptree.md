---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'Fonte de dicionário YAMLRegExpTree'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: 'Configure um arquivo YAML como fonte para dicionários em árvore de expressões regulares.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

A fonte `YAMLRegExpTree` carrega uma árvore de expressões regulares de um arquivo YAML no sistema de arquivos local.
Ela foi projetada exclusivamente para uso com o [`regexp_tree`](../layouts/regexp-tree.md) layout de dicionário
e fornece mapeamentos hierárquicos de regex para atributos para lookups baseados em padrões, como a análise de user agent.

:::note
A fonte `YAMLRegExpTree` está disponível apenas no ClickHouse Open Source.
No ClickHouse Cloud, exporte o dicionário para CSV e carregue-o por meio de uma [fonte de tabela do ClickHouse](./clickhouse.md).
Consulte [Como usar dicionários regexp&#95;tree no ClickHouse Cloud](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud) para mais detalhes.
:::

<div id="configuration">
  ## Configuração
</div>

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

Campos de definição:

| Configuração | Descrição                                                                                                                                                  |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PATH`       | O caminho absoluto para o arquivo YAML que contém a árvore de expressões regulares. Quando criado via DDL, o arquivo deve estar no diretório `user_files`. |

<div id="yaml-file-structure">
  ## Estrutura do arquivo YAML
</div>

O arquivo YAML contém uma lista de nós da árvore de expressões regulares. Cada nó pode ter atributos e nós filhos, formando uma hierarquia:

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

Cada nó tem a seguinte estrutura:

* **`regexp`**: A expressão regular deste nó.
* **atributos**: Atributos de dicionário definidos pelo usuário (por exemplo, `name`, `version`). Os valores dos atributos podem conter **retroreferências** a grupos de captura na expressão regular, escritas como `\1` ou `$1` (números de 1 a 9). Elas são substituídas pelo grupo de captura correspondente no momento da consulta.
* **nós filhos**: Uma lista de filhos, cada um com seus próprios atributos e, opcionalmente, mais filhos. O nome da lista de filhos é arbitrário (por exemplo, `versions` acima). A correspondência de strings segue uma busca em profundidade: se uma string corresponder a um nó, seus filhos também serão verificados. Os atributos do nó correspondente mais profundo têm precedência e substituem os atributos do nó pai com o mesmo nome.

<div id="related-pages">
  ## Páginas relacionadas
</div>

* [layout de dicionário regexp&#95;tree](../layouts/regexp-tree.md) — configuração do layout, exemplos de consulta e modos de correspondência
* [dictGet](/pt-BR/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/pt-BR/sql-reference/functions/ext-dict-functions#dictGetAll) — funções para consultar dicionários do tipo regexp tree