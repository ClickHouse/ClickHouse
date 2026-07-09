---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: 'Origem de dicionário em arquivo local'
sidebar_position: 2
sidebar_label: 'Arquivo local'
description: 'Configure um arquivo local como origem de dicionário no ClickHouse.'
doc_type: 'referência'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

A fonte de arquivo local carrega dados de dicionário de um arquivo no sistema de arquivos local. Isso é útil para tabelas de consulta pequenas e estáticas que podem ser armazenadas como arquivos simples em formatos como TSV, CSV ou qualquer outro [formato compatível](/pt-BR/sql-reference/formats).

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuração:

| Configuração | Descrição                                                                                                |
| ------------ | -------------------------------------------------------------------------------------------------------- |
| `path`       | O caminho absoluto para o arquivo.                                                                       |
| `format`     | O formato do arquivo. Todos os formatos descritos em [Formatos](/pt-BR/sql-reference/formats) são compatíveis. |

Quando um dicionário com a fonte `FILE` é criado por meio de um comando DDL (`CREATE DICTIONARY ...`), o arquivo de origem precisa estar localizado no diretório `user_files` para impedir que usuários do banco de dados acessem arquivos arbitrários no nó do ClickHouse.

**Veja também**

* [função dictionary](/pt-BR/sql-reference/table-functions/dictionary)