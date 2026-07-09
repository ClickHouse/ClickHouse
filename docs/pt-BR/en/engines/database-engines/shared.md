---
description: 'Página sobre o motor de banco de dados `Shared`, disponível no ClickHouse Cloud'
sidebar_label: 'Shared'
sidebar_position: 10
slug: /engines/database-engines/shared
title: 'Shared'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="shared-database-engine">
  # Motor de banco de dados Shared
</div>

O motor de banco de dados `Shared` funciona em conjunto com o Shared Catalog para gerenciar bancos de dados cujas tabelas usam motores de tabela sem estado, como [`SharedMergeTree`](/pt-BR/cloud/reference/shared-merge-tree).
Esses motores de tabela não gravam estado persistente em disco e são compatíveis com ambientes dinâmicos de computação.

O motor de banco de dados `Shared` no Cloud elimina a dependência de discos locais.
É um motor puramente em memória, exigindo apenas CPU e memória.

<div id="how-it-works">
  ## Como isso funciona?
</div>

O motor de banco de dados `Shared` armazena todas as definições de banco de dados e de tabela em um Shared Catalog central baseado no Keeper. Em vez de gravar no disco local, ele mantém um único estado global versionado compartilhado entre todos os nós de computação.

Cada nó mantém apenas a última versão aplicada e, na inicialização, busca o estado mais recente sem necessidade de arquivos locais nem de configuração manual.

<div id="syntax">
  ## Sintaxe
</div>

Para os usuários finais, o uso do Shared Catalog e do motor de banco de dados Shared não requer nenhuma configuração adicional. A criação do banco de dados é a mesma de sempre:

```sql
CREATE DATABASE my_database;
```

O ClickHouse Cloud atribui automaticamente o motor de banco de dados Shared aos bancos de dados. Quaisquer tabelas criadas nesse tipo de banco de dados com motores sem estado se beneficiarão automaticamente dos recursos de replicação e coordenação do Shared Catalog.

:::tip
Para mais informações sobre o Shared Catalog e seus benefícios, consulte [&quot;Shared catalog and shared database engine&quot;](/pt-BR/cloud/reference/shared-catalog) na seção de referência do Cloud.
:::