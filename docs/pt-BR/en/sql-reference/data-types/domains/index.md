---
description: 'Visão geral dos tipos de domínio no ClickHouse, que estendem tipos
  básicos com recursos adicionais'
sidebar_label: 'Domínios'
sidebar_position: 56
slug: /sql-reference/data-types/domains/
title: 'Domínios'
doc_type: 'reference'
---

Domínios são tipos de finalidade específica que adicionam recursos extras a tipos básicos existentes, sem alterar o formato wire e em disco do tipo de dado subjacente. Atualmente, o ClickHouse não oferece suporte a domínios definidos pelo usuário.

Você pode usar domínios em qualquer lugar em que o tipo básico correspondente possa ser usado, por exemplo:

* Criar uma coluna de tipo de domínio
* Ler/gravar valores de/para uma coluna de domínio
* Usá-lo como índice se um tipo básico puder ser usado como índice
* Chamar funções com valores de uma coluna de domínio

<div id="extra-features-of-domains">
  ### Recursos extras dos Domínios
</div>

* Nome explícito do tipo da coluna em `SHOW CREATE TABLE` ou `DESCRIBE TABLE`
* Entrada em formato legível por humanos com `INSERT INTO domain_table(domain_column) VALUES(...)`
* Saída em formato legível por humanos para `SELECT domain_column FROM domain_table`
* Carregamento de dados de uma fonte externa em formato legível por humanos: `INSERT INTO domain_table FORMAT CSV ...`

<div id="limitations">
  ### Limitações
</div>

* Não é possível converter a coluna de índice de um tipo básico para um tipo de domínio via `ALTER TABLE`.
* Não é possível converter implicitamente valores de texto em valores de domínio ao inserir dados de outra coluna ou tabela.
* O domínio não impõe restrições aos valores armazenados.