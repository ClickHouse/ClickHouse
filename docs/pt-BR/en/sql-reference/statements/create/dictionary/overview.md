---
description: 'Documentação para criar e configurar dicionários'
sidebar_label: 'Visão geral'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import CloudSupportedBadge from '@theme/badges/CloudSupportedBadge';

<div id="create-dictionary">
  # CREATE DICTIONARY
</div>

Um dicionário é um mapeamento (`key -> attributes`) útil para vários tipos de listas de referência.
O ClickHouse oferece suporte a funções especiais para trabalhar com dicionários, que podem ser usadas em consultas. Usar dicionários com funções é mais fácil e mais eficiente do que usar um `JOIN` com tabelas de referência.

Os dicionários podem ser criados de duas formas:

* [Com uma consulta DDL](#creating-a-dictionary-with-a-ddl-query) (recomendado)
* [Com um arquivo de configuração](#creating-a-dictionary-with-a-configuration-file)

<div id="creating-a-dictionary-with-a-ddl-query">
  ## Criando um dicionário com uma consulta DDL
</div>

<CloudSupportedBadge />

Os dicionários podem ser criados com consultas DDL.
Este é o método recomendado porque, com dicionários criados via DDL:

* Nenhum registro adicional é incluído nos arquivos de configuração do servidor.
* Os dicionários podem ser usados como entidades de primeira classe, como tabelas ou views.
* Os dados podem ser lidos diretamente, usando a sintaxe familiar de `SELECT` em vez de table functions de dicionário. Observe que, ao acessar um dicionário diretamente por meio de uma instrução `SELECT`, um dicionário em cache retornará apenas os dados em cache, enquanto um dicionário sem cache retornará todos os dados que armazena.
* Os dicionários podem ser renomeados facilmente.

<div id="syntax">
  ### Sintaxe
</div>

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| Cláusula                                    | Descrição                                                                                                                                                                        |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Atributos](./attributes.md)                | Os atributos do dicionário são especificados de maneira semelhante às colunas de uma tabela. A única propriedade obrigatória é o tipo; todas as demais podem ter valores padrão. |
| PRIMARY KEY                                 | Define a(s) coluna(s)-chave para consultas ao dicionário. Dependendo do layout, um ou mais atributos podem ser especificados como chaves.                                        |
| [`SOURCE`](./sources/overview.md)           | Define a fonte de dados do dicionário (por exemplo, tabela do ClickHouse, HTTP, PostgreSQL).                                                                                     |
| [`LAYOUT`](./layouts/overview.md)           | Controla como o dicionário é armazenado em memória (por exemplo, `FLAT`, `HASHED`, `CACHE`).                                                                                     |
| [`LIFETIME`](./lifetime.md)                 | Define o intervalo de atualização do dicionário.                                                                                                                                 |
| [`ON CLUSTER`](../../../distributed-ddl.md) | Cria o dicionário em um cluster. Opcional.                                                                                                                                       |
| `SETTINGS`                                  | Configurações adicionais do dicionário. Opcional.                                                                                                                                |
| `COMMENT`                                   | Adiciona um comentário em texto ao dicionário. Opcional.                                                                                                                         |

<div id="creating-a-dictionary-with-a-configuration-file">
  ## Criando um dicionário com arquivo de configuração
</div>

<CloudNotSupportedBadge />

:::note
Criar um dicionário com arquivo de configuração não está disponível no ClickHouse Cloud. Use DDL (veja acima) e crie seu dicionário como o usuário `default`.
:::

O arquivo de configuração do dicionário tem o seguinte formato:

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

É possível configurar quantos dicionários quiser no mesmo arquivo.

<div id="related-content">
  ## Conteúdo relacionado
</div>

* [Layouts](/pt-BR/sql-reference/statements/create/dictionary/layouts) — Como os dicionários são armazenados na memória
* [Sources](/pt-BR/sql-reference/statements/create/dictionary/sources) — Como se conectar a fontes de dados
* [Ciclo de vida](./lifetime.md) — Configuração de atualização automática
* [Atributos](./attributes.md) — Configuração de chave e atributo
* [Dicionários embutidos](./embedded.md) — Dicionários geobase nativos
* [system.dictionaries](../../../../operations/system-tables/dictionaries.md) — Tabela de sistema com informações sobre dicionários